#!/usr/bin/env python3
"""Check that incremental runs give exactly the same Gold tables as a full refresh.

Simulates two nightly loads by splitting the generated extracts on
_loaded_at, between two nightly batches: rows loaded up to noon of the
day --batch2-days before the newest load go in batch 1, the rest in
batch 2. Batch 2 therefore contains new changes, late
arrivals and corrections to rows already loaded, like a real nightly file.

Two warehouses are built from the same raw data:

    incremental   load batch 1, dbt build --full-refresh,
                  load batch 2, dbt build (incremental, look-back 0 hours),
                  dbt build again with no new load (must change nothing)
    full          load batch 1 and batch 2, dbt build --full-refresh

Every Gold and restricted table must then match: same row count and the same
content hash for each column. Exits 1 on any difference or failed build.

Usage:
    python scripts/check_incremental.py [--work-dir DIR] [--batch2-days N]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from load_raw import TABLES, load_all

ROOT = Path(__file__).resolve().parent.parent
SCHEMAS = ("gold", "restricted")
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def split_extracts(
    source_dir: Path, out_dir: Path, batch2_days: int
) -> dict[str, list[int]]:
    """Write batch1/ and batch2/ CSVs; return {table: [rows in batch 1, batch 2]}."""
    rows = {}
    for table in TABLES:
        with (source_dir / f"{table}.csv").open(newline="") as f:
            reader = csv.DictReader(f)
            rows[table] = (reader.fieldnames, list(reader))
    newest = max(
        datetime.strptime(r["_loaded_at"], TS_FORMAT)
        for _, data in rows.values()
        for r in data
    )
    # Split between nightly batches (noon), never inside one night's batch
    cutoff = datetime.combine(
        (newest - timedelta(days=batch2_days)).date(), datetime.min.time()
    ) + timedelta(hours=12)
    counts = {}
    for table, (header, data) in rows.items():
        batches = [
            [
                r
                for r in data
                if datetime.strptime(r["_loaded_at"], TS_FORMAT) <= cutoff
            ],
            [r for r in data if datetime.strptime(r["_loaded_at"], TS_FORMAT) > cutoff],
        ]
        for number, batch in enumerate(batches, start=1):
            target = out_dir / f"batch{number}"
            target.mkdir(parents=True, exist_ok=True)
            with (target / f"{table}.csv").open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writeheader()
                writer.writerows(batch)
        counts[table] = [len(b) for b in batches]
        if not all(counts[table]):
            raise ValueError(
                f"{table}: a batch is empty {counts[table]}; change --batch2-days"
            )
    return counts


def table_fingerprints(database: Path) -> dict[str, dict]:
    """Row count and a content hash per column, for each Gold and restricted table."""
    fingerprints = {}
    with duckdb.connect(str(database), read_only=True) as con:
        tables = con.execute(
            "select table_schema, table_name from information_schema.tables "
            f"where table_schema in {SCHEMAS} order by 1, 2"
        ).fetchall()
        for schema, table in tables:
            columns = [
                c[0]
                for c in con.execute(
                    "select column_name from information_schema.columns "
                    "where table_schema = ? and table_name = ? "
                    "order by ordinal_position",
                    [schema, table],
                ).fetchall()
            ]
            hashes = con.execute(
                "select count(*), "
                + ", ".join(f'bit_xor(hash("{c}"))' for c in columns)
                + f' from "{schema}"."{table}"'
            ).fetchone()
            fingerprints[f"{schema}.{table}"] = {
                "rows": hashes[0],
                "columns": dict(zip(columns, hashes[1:], strict=True)),
            }
    return fingerprints


def compare(incremental: dict[str, dict], full: dict[str, dict]) -> list[str]:
    """Human-readable differences; empty when the warehouses match."""
    problems = []
    for name in sorted(set(incremental) | set(full)):
        a, b = incremental.get(name), full.get(name)
        if a is None or b is None:
            problems.append(f"{name}: only in {'full' if a is None else 'incremental'}")
            continue
        if a["rows"] != b["rows"]:
            problems.append(f"{name}: {a['rows']} rows incremental vs {b['rows']} full")
        differing = [c for c in b["columns"] if a["columns"].get(c) != b["columns"][c]]
        if differing:
            problems.append(f"{name}: columns differ: {', '.join(differing)}")
    return problems


def bronze_row_counts(database: Path) -> dict[str, int]:
    with duckdb.connect(str(database), read_only=True) as con:
        return {
            t: con.execute(f"select count(*) from bronze.stg_{t}").fetchone()[0]
            for t in TABLES
        }


def pending_raw_rows(database: Path) -> dict[str, int]:
    """Raw rows newer than Bronze's newest: what an incremental run will read."""
    with duckdb.connect(str(database), read_only=True) as con:
        return {
            t: con.execute(
                f"select count(*) from raw.{t} where _ingested_at > "
                f"(select max(ingested_at) from bronze.stg_{t})"
            ).fetchone()[0]
            for t in TABLES
        }


def dbt_build(database: Path, *args: str) -> float:
    """Run dbt build into `database`; return seconds taken. Raises on failure."""
    env = {**os.environ, "ECOM_DUCKDB_PATH": str(database)}
    start = time.monotonic()
    result = subprocess.run(
        ["dbt", "build", *args],
        check=False,  # the return code is checked below, with the log on failure
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout[-4000:], file=sys.stderr)
        raise RuntimeError(f"dbt build {' '.join(args)} failed for {database.name}")
    return round(time.monotonic() - start, 1)


def run(work_dir: Path, batch2_days: int) -> dict:
    import generate_raw_data

    generate_raw_data.main(["--out-dir", str(work_dir / "extract")])
    counts = split_extracts(work_dir / "extract", work_dir, batch2_days)
    report: dict = {"batch_rows": counts, "seconds": {}}

    inc, full = work_dir / "incremental.duckdb", work_dir / "full.duckdb"
    load_all(work_dir / "batch1", inc)
    report["seconds"]["incremental: full refresh on batch 1"] = dbt_build(
        inc, "--full-refresh"
    )
    before = bronze_row_counts(inc)
    load_all(work_dir / "batch2", inc)
    report["raw_rows_read_by_batch2_run"] = pending_raw_rows(inc)
    lookback = json.dumps({"incremental_lookback_hours": 0})
    report["seconds"]["incremental: run on batch 2"] = dbt_build(
        inc, "--vars", lookback
    )
    after = bronze_row_counts(inc)
    report["raw_rows_read_by_rerun"] = pending_raw_rows(inc)
    report["seconds"]["incremental: rerun, no new load"] = dbt_build(
        inc, "--vars", lookback
    )
    report["bronze_rows"] = {"after_batch1": before, "after_batch2": after}
    report["rerun_changed_bronze"] = bronze_row_counts(inc) != after

    load_all(work_dir / "batch1", full)
    load_all(work_dir / "batch2", full)
    report["seconds"]["full refresh on both batches"] = dbt_build(
        full, "--full-refresh"
    )

    report["differences"] = compare(table_fingerprints(inc), table_fingerprints(full))
    report["tables_compared"] = len(table_fingerprints(full))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--batch2-days", type=int, default=30)
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = args.work_dir or Path(tmp)
        work_dir.mkdir(parents=True, exist_ok=True)
        report = run(work_dir, args.batch2_days)
    print(json.dumps(report, indent=2))
    rerun_read = sum(report["raw_rows_read_by_rerun"].values())
    if report["differences"] or report["rerun_changed_bronze"] or rerun_read:
        print("FAIL: incremental and full refresh differ", file=sys.stderr)
        return 1
    print(f"OK: {report['tables_compared']} tables identical after incremental runs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
