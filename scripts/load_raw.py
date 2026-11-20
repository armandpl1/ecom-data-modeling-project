#!/usr/bin/env python3
"""Load the raw CSV extracts into an append-only raw layer in DuckDB.

Each extract in data/raw/ is appended to a table in the `raw` schema
(raw.customers, raw.products, raw.orders, raw.subscriptions), with three
columns that say where every row came from:

    _ingested_at   when this loader inserted the row
    _batch_id      <table>-<first 12 hex characters of the file's SHA-256>
    _file_name     the extract's file name

Every load is recorded in raw._load_log (batch, table, file, row count,
SHA-256, time). A file whose checksum is already logged for that table is
skipped, so rerunning the loader is safe. A file's insert and its log row
are written in one transaction: a failed load leaves no partial batch.

Tables are never rebuilt, so the raw layer keeps the history of every load:
a bad extract can be traced to its batch, and past loads can be replayed.
The staging models already deduplicate rows that arrive more than once.

Usage:
    python scripts/load_raw.py                       # data/raw -> ecom.duckdb
    python scripts/load_raw.py --raw-dir DIR --database FILE
"""

from __future__ import annotations

import argparse
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

TABLES = ("customers", "products", "orders", "subscriptions")
DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_DATABASE = Path("ecom.duckdb")

log = logging.getLogger("load_raw")


@dataclass(frozen=True)
class LoadResult:
    table: str
    file_name: str
    batch_id: str
    rows: int
    loaded: bool  # False when the file was already loaded and was skipped


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_load_log(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists raw")
    con.execute("""
        create table if not exists raw._load_log (
            batch_id   varchar   not null,
            table_name varchar   not null,
            file_name  varchar   not null,
            row_count  bigint    not null,
            sha256     varchar   not null,
            loaded_at  timestamp not null
        )
        """)


def load_file(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> LoadResult:
    """Append one extract to raw.<table>, unless its checksum is already logged."""
    checksum = sha256_of(path)
    batch_id = f"{table}-{checksum[:12]}"
    already = con.execute(
        "select row_count from raw._load_log where table_name = ? and sha256 = ?",
        [table, checksum],
    ).fetchone()
    if already:
        log.info("skip %s: already loaded as %s", path.name, batch_id)
        return LoadResult(table, path.name, batch_id, already[0], loaded=False)

    source = "read_csv(?, header = true, all_varchar = true)"
    con.execute("begin transaction")
    try:
        # Create the table from the file's columns on first load (no rows yet)
        con.execute(
            f"create table if not exists raw.{table} as "
            f"select *, null::timestamp as _ingested_at, null::varchar as _batch_id, "
            f"null::varchar as _file_name from {source} limit 0",
            [str(path)],
        )
        # BY NAME matches columns by name, so a reordered extract still loads
        con.execute(
            f"insert into raw.{table} by name "
            f"select *, current_timestamp::timestamp as _ingested_at, "
            f"? as _batch_id, ? as _file_name from {source}",
            [batch_id, path.name, str(path)],
        )
        rows = con.execute(
            f"select count(*) from raw.{table} where _batch_id = ?", [batch_id]
        ).fetchone()[0]
        con.execute(
            "insert into raw._load_log "
            "values (?, ?, ?, ?, ?, current_timestamp::timestamp)",
            [batch_id, table, path.name, rows, checksum],
        )
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    log.info("loaded %s: %d rows as %s", path.name, rows, batch_id)
    return LoadResult(table, path.name, batch_id, rows, loaded=True)


def load_all(raw_dir: Path, database: Path) -> list[LoadResult]:
    missing = [t for t in TABLES if not (raw_dir / f"{t}.csv").exists()]
    if missing:
        raise FileNotFoundError(
            f"missing extracts in {raw_dir}: {', '.join(missing)} "
            "(run scripts/generate_raw_data.py first)"
        )
    with duckdb.connect(str(database)) as con:
        ensure_load_log(con)
        return [load_file(con, t, raw_dir / f"{t}.csv") for t in TABLES]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)
    results = load_all(args.raw_dir, args.database)
    loaded = [r for r in results if r.loaded]
    print(
        f"{len(loaded)} of {len(results)} extracts loaded into {args.database} "
        f"({sum(r.rows for r in loaded)} new rows); "
        f"{len(results) - len(loaded)} already loaded and skipped"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
