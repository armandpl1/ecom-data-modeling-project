#!/usr/bin/env python3
"""Check early-arriving facts: orders that arrive before their customer.

1. Generate the extracts and pick a customer with orders and subscriptions.
2. Load 1: everything except that customer's rows, then dbt build
   --full-refresh. The build must pass. Every fact row of that customer must
   point to the unknown member (customer_sk '-1'), and the unknown_member_rows
   tests must warn about them.
3. Load 2: only the late customer's rows (the other extracts are the same
   files, so the loader skips them), then a normal incremental dbt build.
   No fact row may point to the unknown member any more.
4. A second warehouse gets the same two loads and one dbt build --full-refresh.
   Every Gold and restricted table must match the incremental one, so the
   re-pointing is exactly what a rebuild from scratch would give.

Usage:
    python scripts/check_late_dimensions.py [--work-dir DIR]
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path

import duckdb
from check_incremental import ROOT, compare, dbt_build, table_fingerprints
from load_raw import TABLES, load_all

UNKNOWN = "-1"
FACTS = {
    "fact_orders": ("customer_sk", "product_sk"),
    "fact_order_headers": ("customer_sk",),
    "fact_subscriptions": ("customer_sk",),
    "fact_subscription_mrr_monthly": ("customer_sk",),
}


def read_rows(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def write_rows(path: Path, header: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def pick_customer(extract: Path) -> str:
    """The customer with the most orders among those with a subscription."""
    _, orders = read_rows(extract / "orders.csv")
    _, subscriptions = read_rows(extract / "subscriptions.csv")
    subscribed = {r["customer_id"] for r in subscriptions}
    counts = Counter(r["customer_id"] for r in orders if r["customer_id"] in subscribed)
    return max(sorted(counts), key=lambda c: counts[c])


def split(extract: Path, work_dir: Path, customer_id: str) -> dict:
    """batch1/: all but the customer's rows; batch2/: the customer's rows only."""
    header, customers = read_rows(extract / "customers.csv")
    late = [r for r in customers if r["customer_id"] == customer_id]
    early = [r for r in customers if r["customer_id"] != customer_id]
    write_rows(work_dir / "batch1" / "customers.csv", header, early)
    write_rows(work_dir / "batch2" / "customers.csv", header, late)
    for table in TABLES:
        if table == "customers":
            continue
        for batch in ("batch1", "batch2"):
            (work_dir / batch).mkdir(parents=True, exist_ok=True)
            # Same bytes in both batches: the loader skips the second copy
            shutil.copyfile(extract / f"{table}.csv", work_dir / batch / f"{table}.csv")
    return {"customer_id": customer_id, "late_customer_rows": len(late)}


def unknown_rows(database: Path, customer_id: str | None = None) -> dict[str, int]:
    """Fact rows pointing to the unknown member, per fact and key."""
    out = {}
    with duckdb.connect(str(database), read_only=True) as con:
        for fact, keys in FACTS.items():
            for key in keys:
                where = f"{key} = '{UNKNOWN}'"
                if customer_id is not None:
                    where += f" and customer_id = '{customer_id}'"
                out[f"{fact}.{key}"] = con.execute(
                    f"select count(*) from gold.{fact} where {where}"
                ).fetchall()[0][0]
    return out


def customer_fact_rows(database: Path, customer_id: str) -> dict[str, int]:
    with duckdb.connect(str(database), read_only=True) as con:
        return {
            fact: con.execute(
                f"select count(*) from gold.{fact} where customer_id = ?", [customer_id]
            ).fetchall()[0][0]
            for fact in FACTS
        }


def warned_tests(run_results: Path) -> dict[str, int]:
    """unknown_member_rows tests that warned, with their failing row counts."""
    results = json.loads(run_results.read_text())["results"]
    return {
        r["unique_id"].split(".")[2]: r["failures"]
        for r in results
        if "unknown_member_rows" in r["unique_id"] and r["status"] == "warn"
    }


def run(work_dir: Path) -> dict:
    import generate_raw_data

    extract = work_dir / "extract"
    generate_raw_data.main(["--out-dir", str(extract)])
    customer_id = pick_customer(extract)
    report: dict = split(extract, work_dir, customer_id)

    inc, full = work_dir / "incremental.duckdb", work_dir / "full.duckdb"
    load_all(work_dir / "batch1", inc)
    report["build_seconds_load1"] = dbt_build(inc, "--full-refresh")
    report["warned_after_load1"] = warned_tests(ROOT / "target" / "run_results.json")
    report["customer_fact_rows"] = customer_fact_rows(inc, customer_id)
    report["unknown_after_load1"] = unknown_rows(inc)
    report["unknown_for_customer_after_load1"] = unknown_rows(inc, customer_id)

    load_all(work_dir / "batch2", inc)
    report["build_seconds_load2"] = dbt_build(inc)  # normal incremental run
    report["unknown_after_load2"] = unknown_rows(inc)

    load_all(work_dir / "batch1", full)
    load_all(work_dir / "batch2", full)
    dbt_build(full, "--full-refresh")
    report["differences"] = compare(table_fingerprints(inc), table_fingerprints(full))
    report["tables_compared"] = len(table_fingerprints(full))
    return report


def problems(report: dict) -> list[str]:
    found = []
    rows, unknown = (
        report["customer_fact_rows"],
        report["unknown_for_customer_after_load1"],
    )
    if not all(rows.values()):
        found.append(f"the late customer has no rows in some fact: {rows}")
    for fact, count in rows.items():
        if unknown[f"{fact}.customer_sk"] != count:
            found.append(
                f"{fact}: {unknown[f'{fact}.customer_sk']} of {count} rows on -1"
            )
    other = sum(report["unknown_after_load1"].values()) - sum(unknown.values())
    if other:
        found.append(f"{other} rows of other customers point to -1 after load 1")
    if len(report["warned_after_load1"]) != len(FACTS):
        found.append(
            f"expected one warning per fact, got {report['warned_after_load1']}"
        )
    if any(report["unknown_after_load2"].values()):
        found.append(f"still on -1 after load 2: {report['unknown_after_load2']}")
    found += report["differences"]
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args(argv)
    (ROOT / "target").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=ROOT / "target") as tmp:
        work_dir = args.work_dir or Path(tmp)
        work_dir.mkdir(parents=True, exist_ok=True)
        report = run(work_dir)
    print(json.dumps(report, indent=2))
    if found := problems(report):
        print("FAIL:", *found, sep="\n  - ", file=sys.stderr)
        return 1
    rows = sum(report["customer_fact_rows"].values())
    print(
        f"OK: {rows} fact rows of late customer {report['customer_id']} pointed to "
        f"the unknown member and warned, then were re-pointed by an incremental "
        f"run; {report['tables_compared']} tables match a full rebuild"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
