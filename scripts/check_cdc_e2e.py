#!/usr/bin/env python3
"""End-to-end check of the CDC path against the CSV path, on the live stack.

Needs the CDC stack running and empty (`make cdc-up` on fresh volumes).

1. CSV path: load the extracts into one warehouse and dbt build it.
2. CDC path: replay the history into Postgres, consume it in capped batches
   (so batches end mid-stream and transaction trimming is exercised) into a
   second warehouse, check a rerun loads nothing, then dbt build it.
3. Every Gold table must be identical in both warehouses. restricted differs
   by design: CDC keeps a correction the extracts later undo (README, "Change
   data capture"), so it is reported, not failed.
4. Schema change: add a column in Postgres, change one row, consume. The
   column must land in raw with its value, be logged in
   raw._cdc_schema_changes, and dbt build must still pass.

Usage:
    python scripts/check_cdc_e2e.py [--work-dir DIR] [--json FILE]
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

import cdc_consumer
import cdc_source
import duckdb
from check_incremental import ROOT, compare, dbt_build, table_fingerprints
from load_raw import load_all

BATCH = 5_000  # events per table per run: several capped runs per table


def wait_for(predicate, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting for {what}")
        time.sleep(1)


def consume_until_caught_up(bootstrap: str, database: Path, expected_rows: int) -> int:
    """Run the consumer until the raw layer holds every change; return run count."""
    runs = 0

    def raw_rows() -> int:
        with duckdb.connect(str(database), read_only=True) as con:
            return sum(
                con.execute(f"select count(*) from raw.{t}").fetchone()[0]
                for t in cdc_consumer.TABLES
            )

    def step() -> bool:
        nonlocal runs
        cdc_consumer.run_once(bootstrap, database, BATCH)
        runs += 1
        return raw_rows() >= expected_rows

    wait_for(step, 300, f"{expected_rows} raw rows")
    return runs


def raw_rows_by_table(database: Path) -> dict[str, int]:
    with duckdb.connect(str(database), read_only=True) as con:
        return {
            t: con.execute(f"select count(*) from raw.{t}").fetchone()[0]
            for t in cdc_consumer.TABLES
        }


def schema_change(dsn: str, bootstrap: str, database: Path) -> dict:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("alter table customers add column if not exists loyalty_tier text")
        cur.execute(
            "update customers set loyalty_tier = 'gold', updated_at = updated_at "
            "where customer_id = (select min(customer_id) from customers) "
            "returning customer_id"
        )
        [customer_id] = cur.fetchone()

    def landed() -> bool:
        cdc_consumer.run_once(bootstrap, database)
        with duckdb.connect(str(database), read_only=True) as con:
            columns = {
                r[0]
                for r in con.execute(
                    "select column_name from information_schema.columns "
                    "where table_schema = 'raw' and table_name = 'customers'"
                ).fetchall()
            }
            if "loyalty_tier" not in columns:
                return False
            return bool(
                con.execute(
                    "select count(*) from raw.customers where loyalty_tier = 'gold'"
                ).fetchone()[0]
            )

    wait_for(landed, 120, "the new column in raw.customers")
    with duckdb.connect(str(database), read_only=True) as con:
        logged = con.execute(
            "select table_name, column_name from raw._cdc_schema_changes"
        ).fetchall()
    if ("customers", "loyalty_tier") not in logged:
        raise AssertionError(f"schema change not logged: {logged}")
    return {"customer_id": customer_id, "logged": logged}


def run(work_dir: Path, dsn: str, bootstrap: str) -> dict:
    import generate_raw_data
    import psycopg

    raw_dir = work_dir / "raw"
    generate_raw_data.main(["--out-dir", str(raw_dir)])
    csv_db, cdc_db = work_dir / "csv.duckdb", work_dir / "cdc.duckdb"

    load_all(raw_dir, csv_db)
    report: dict = {"csv_build_seconds": dbt_build(csv_db)}

    changes = cdc_source.build_changes()
    with psycopg.connect(dsn, autocommit=True) as conn:
        replayed = cdc_source.replay(conn, changes, None, None)
    if replayed["remaining"] or not replayed["applied"]:
        raise AssertionError(f"replay incomplete or already done: {replayed}")
    report["replay"] = replayed

    began = time.monotonic()
    report["consumer_runs"] = consume_until_caught_up(
        bootstrap, cdc_db, replayed["applied"]
    )
    report["consume_seconds"] = round(time.monotonic() - began, 1)
    rows = raw_rows_by_table(cdc_db)
    report["raw_rows"] = rows
    if sum(rows.values()) != replayed["applied"]:
        raise AssertionError(
            f"{sum(rows.values())} raw rows for {replayed['applied']} changes"
        )
    again = cdc_consumer.run_once(bootstrap, cdc_db)
    if any(r.rows for r in again):
        raise AssertionError(f"a rerun loaded rows again: {again}")

    report["cdc_build_seconds"] = dbt_build(cdc_db)
    csv_tables, cdc_tables = table_fingerprints(csv_db), table_fingerprints(cdc_db)
    problems = compare(cdc_tables, csv_tables)
    report["gold_differences"] = [p for p in problems if p.startswith("gold.")]
    report["restricted_differences"] = [
        p for p in problems if not p.startswith("gold.")
    ]
    report["gold_tables"] = sorted(t for t in csv_tables if t.startswith("gold."))
    if report["gold_differences"]:
        raise AssertionError("Gold differs: " + "; ".join(report["gold_differences"]))

    report["schema_change"] = schema_change(dsn, bootstrap, cdc_db)
    report["build_after_schema_change_seconds"] = dbt_build(cdc_db)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--json", type=Path, help="write the report here")
    parser.add_argument("--dsn", default=cdc_source.DEFAULT_DSN)
    parser.add_argument("--bootstrap", default=cdc_consumer.DEFAULT_BOOTSTRAP)
    args = parser.parse_args(argv)
    (ROOT / "target").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=ROOT / "target") as tmp:
        report = run(args.work_dir or Path(tmp), args.dsn, args.bootstrap)
    text = json.dumps(report, indent=2, default=str)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(text)
    print(text)
    print(
        f"OK: {len(report['gold_tables'])} Gold tables identical on both paths; "
        f"{sum(report['raw_rows'].values())} changes loaded exactly once in "
        f"{report['consumer_runs']} capped runs; new source column handled",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
