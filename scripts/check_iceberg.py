#!/usr/bin/env python3
"""Check that the Iceberg tables match Gold in ecom.duckdb.

For each table in ops/lakehouse.yml, read the current Iceberg snapshot with
DuckDB's own `iceberg` extension (a different reader from the one that wrote
it) and compare it with gold.<table> row for row, in both directions
(EXCEPT ALL, so duplicates count). Also checks:

- every table was covered by the same publish (one dbt run): each table's
  `last-publish-id` property matches, whether its data changed or not
- time travel: the latest snapshot of the previous publish is still readable

Exits 1 if any table differs or the publish ids disagree, 2 if inputs are
missing.

Usage:
    python scripts/check_iceberg.py
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
from publish_iceberg import (
    DEFAULT_CONFIG,
    DEFAULT_DATABASE,
    ROOT,
    load_config,
    open_catalog,
    snapshot_publish_ids,
)
from pyiceberg.exceptions import NoSuchTableError


@dataclass(frozen=True)
class TableCheck:
    table: str
    gold_rows: int
    iceberg_rows: int
    only_in_gold: int
    only_in_iceberg: int
    publish_id: str
    publishes_kept: int
    previous_rows: int | None  # rows in the previous publish (time travel)

    @property
    def identical(self) -> bool:
        return self.only_in_gold == 0 and self.only_in_iceberg == 0


def load_iceberg_extension(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")


def scan_sql(metadata_location: str, snapshot_id: int | None = None) -> str:
    """DuckDB's reader for one Iceberg table, optionally at an older snapshot."""
    if snapshot_id is None:
        return f"iceberg_scan('{metadata_location}')"
    return f"iceberg_scan('{metadata_location}', snapshot_from_id={snapshot_id})"


def count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(f"select count(*) from ({sql})").fetchall()[0][0])


def check(
    database: Path = DEFAULT_DATABASE, config: dict | None = None, root: Path = ROOT
) -> list[TableCheck]:
    config = config or load_config()
    catalog = open_catalog(config, root)
    con = duckdb.connect(str(database), read_only=True)
    load_iceberg_extension(con)
    results = []
    try:
        for table in config["tables"]:
            iceberg = catalog.load_table(f"{config['namespace']}.{table}")
            scan = scan_sql(iceberg.metadata_location)
            gold = f"gold.{table}"

            history = snapshot_publish_ids(iceberg)
            current_pid = iceberg.properties.get("last-publish-id", "")
            publishes = list(dict.fromkeys(pid for _, pid in history))
            previous_rows = None
            if len(publishes) >= 2:
                prev_pid = publishes[-2]
                prev_snapshot = [sid for sid, pid in history if pid == prev_pid][-1]
                previous = scan_sql(iceberg.metadata_location, prev_snapshot)
                previous_rows = count(con, f"select * from {previous}")
            results.append(
                TableCheck(
                    table=table,
                    gold_rows=count(con, f"select * from {gold}"),
                    iceberg_rows=count(con, f"select * from {scan}"),
                    only_in_gold=count(
                        con, f"select * from {gold} except all select * from {scan}"
                    ),
                    only_in_iceberg=count(
                        con, f"select * from {scan} except all select * from {gold}"
                    ),
                    publish_id=current_pid,
                    publishes_kept=len(publishes),
                    previous_rows=previous_rows,
                )
            )
    finally:
        con.close()
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args(argv)
    if not args.database.exists():
        print(f"Missing {args.database}: run `make build` first.", file=sys.stderr)
        return 2
    try:
        results = check(args.database, load_config(args.config))
    except NoSuchTableError as err:
        print(
            f"Missing Iceberg table ({err}): run `make iceberg` first.", file=sys.stderr
        )
        return 2

    print(
        f"{'table':<31} {'gold':>6} {'iceberg':>8} {'diff':>5} {'kept':>5} {'prev':>6}"
    )
    for r in results:
        prev = "-" if r.previous_rows is None else str(r.previous_rows)
        diff = r.only_in_gold + r.only_in_iceberg
        print(
            f"{r.table:<31} {r.gold_rows:>6} {r.iceberg_rows:>8} {diff:>5}"
            f" {r.publishes_kept:>5} {prev:>6}"
        )
    ids = {r.publish_id for r in results}
    same_run = len(ids) == 1
    identical = sum(r.identical for r in results)
    print(f"\n{identical} of {len(results)} tables identical to Gold")
    print(f"publish ids: {'all ' + next(iter(ids)) if same_run else sorted(ids)}")
    return 0 if identical == len(results) and same_run else 1


if __name__ == "__main__":
    sys.exit(main())
