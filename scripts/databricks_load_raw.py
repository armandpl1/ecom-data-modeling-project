#!/usr/bin/env python3
"""Load the raw CSV extracts into Unity Catalog on Databricks (raw layer).

The Databricks version of scripts/load_raw.py, with Databricks-native ingestion:

1. Upload each CSV to the volume /Volumes/<catalog>/raw/landing/<table>/ as
   <table>-<first 12 hex of its SHA-256>.csv. A file already there is not
   uploaded again.
2. COPY INTO <catalog>.raw.<table>, all columns as text (like DuckDB's raw
   layer), plus _ingested_at, _batch_id (<table>-<sha prefix>) and
   _file_name. COPY INTO remembers the files it has loaded, so reruns load
   nothing twice.
3. Record each batch that loaded rows in <catalog>.raw._load_log.

Runs every statement on the SQL warehouse through the Databricks SDK. Auth
is your Databricks CLI login (profile DATABRICKS_CONFIG_PROFILE); settings
come from the environment (see .env.databricks.example).

Usage:
    python scripts/databricks_load_raw.py [--raw-dir DIR]
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from load_raw import DEFAULT_RAW_DIR, TABLES, sha256_of

LINEAGE = (
    "current_timestamp() as _ingested_at, "
    "concat('{table}-', regexp_extract("
    "_metadata.file_name, '-([0-9a-f]{{12}})\\\\.csv$', 1)) as _batch_id, "
    "_metadata.file_name as _file_name"
)


@dataclass(frozen=True)
class Load:
    table: str
    file_name: str
    rows: int  # 0 when the file was already loaded


def settings() -> dict[str, str]:
    missing = [
        v
        for v in ("DATABRICKS_WAREHOUSE_ID", "DATABRICKS_CATALOG")
        if not os.environ.get(v)
    ]
    if missing:
        raise SystemExit(
            f"Missing {', '.join(missing)}: copy .env.databricks.example to "
            ".env.databricks and run through `make databricks-load`"
        )
    return {
        "warehouse": os.environ["DATABRICKS_WAREHOUSE_ID"],
        "catalog": os.environ["DATABRICKS_CATALOG"],
    }


def client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(profile=os.environ.get("DATABRICKS_CONFIG_PROFILE"))


def run_sql(w, warehouse: str, statement: str) -> list[list]:
    """Run one statement on the warehouse and return its rows."""
    from databricks.sdk.service.sql import StatementState

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse, statement=statement, wait_timeout="50s"
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error.message if response.status.error else ""
        raise RuntimeError(f"{response.status.state}: {error}\n{statement}")
    return (response.result.data_array or []) if response.result else []


def landing_name(table: str, checksum: str) -> str:
    return f"{table}-{checksum[:12]}.csv"


def copy_into_sql(catalog: str, table: str) -> str:
    folder = f"/Volumes/{catalog}/raw/landing/{table}/"
    return (
        f"COPY INTO {catalog}.raw.{table} FROM ("
        f"SELECT *, {LINEAGE.format(table=table)} FROM '{folder}')"
        " FILEFORMAT = CSV"
        " FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')"
        " COPY_OPTIONS ('mergeSchema' = 'true')"
    )


def setup(w, warehouse: str, catalog: str) -> None:
    for statement in (
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.raw",
        f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.landing",
        (
            f"CREATE TABLE IF NOT EXISTS {catalog}.raw._load_log ("
            "batch_id STRING, table_name STRING, file_name STRING, row_count BIGINT,"
            " sha256 STRING, loaded_at TIMESTAMP)"
        ),
        *(f"CREATE TABLE IF NOT EXISTS {catalog}.raw.{t}" for t in TABLES),
    ):
        run_sql(w, warehouse, statement)


def upload(w, catalog: str, table: str, path: Path, checksum: str) -> str:
    name = landing_name(table, checksum)
    target = f"/Volumes/{catalog}/raw/landing/{table}/{name}"
    from databricks.sdk.errors import NotFound

    try:
        w.files.get_metadata(target)
        return name  # same content already landed
    except NotFound:
        pass  # not landed yet: upload it below
    with path.open("rb") as f:
        w.files.upload(target, f, overwrite=False)
    return name


def load_all(raw_dir: Path) -> list[Load]:
    cfg = settings()
    w = client()
    setup(w, cfg["warehouse"], cfg["catalog"])
    loads = []
    for table in TABLES:
        path = raw_dir / f"{table}.csv"
        if not path.exists():
            raise FileNotFoundError(f"missing extract {path}")
        checksum = sha256_of(path)
        name = upload(w, cfg["catalog"], table, path, checksum)
        result = run_sql(w, cfg["warehouse"], copy_into_sql(cfg["catalog"], table))
        rows = int(result[0][0]) if result else 0  # num_affected_rows
        if rows:
            run_sql(
                w,
                cfg["warehouse"],
                f"INSERT INTO {cfg['catalog']}.raw._load_log VALUES ("
                f"'{table}-{checksum[:12]}', '{table}', '{name}', {rows},"
                f" '{checksum}', current_timestamp())",
            )
        loads.append(Load(table, name, rows))
    return loads


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    args = parser.parse_args(argv)
    try:
        loads = load_all(args.raw_dir)
    except FileNotFoundError as err:
        print(f"{err} (run `make data` first)", file=sys.stderr)
        return 2
    for load in loads:
        status = f"{load.rows} rows" if load.rows else "already loaded"
        print(f"{load.table:<14} {load.file_name:<30} {status}")
    new = sum(1 for load in loads if load.rows)
    print(f"{new} of {len(loads)} extracts loaded; the rest were already loaded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
