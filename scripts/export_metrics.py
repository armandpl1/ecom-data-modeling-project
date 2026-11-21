#!/usr/bin/env python3
"""Export semantic-layer metrics to CSV for the dashboard.

Each dataset is a MetricFlow query (`mf query`), so the dashboard shows the
metrics exactly as models/semantic/ defines them, with no SQL of its own. The
CSVs go to dashboard/sources/metrics/, which Evidence reads through its CSV
connector.

Why CSV and not the warehouse directly: Evidence's DuckDB connector bundles
DuckDB 1.4, which can't open a file written by DuckDB 2.0 (this project's
version). CSV also keeps the dashboard independent of the warehouse.

Usage:
    python scripts/export_metrics.py            # needs a built warehouse
"""

from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "dashboard" / "sources" / "metrics"

# dataset name -> (metrics, group by)
DATASETS: dict[str, tuple[list[str], list[str]]] = {
    "monthly_subscriptions": (
        ["mrr", "net_new_mrr", "paying_subscriptions", "churn_rate"],
        ["metric_time__month"],
    ),
    "monthly_revenue": (
        ["recognized_revenue", "orders", "average_order_value"],
        ["metric_time__month"],
    ),
    "quarterly_trials": (
        ["trial_conversions", "trial_endings", "trial_conversion_rate"],
        ["metric_time__quarter"],
    ),
}


def query_command(metrics: list[str], group_by: list[str], out: Path) -> list[str]:
    return [
        "mf",
        "query",
        "--quiet",
        "--metrics",
        ",".join(metrics),
        "--group-by",
        ",".join(group_by),
        "--order",
        ",".join(group_by),
        "--csv",
        str(out),
    ]


def clean(path: Path) -> int:
    """Drop rows where every metric is empty; return the rows kept.

    A derived metric with a time offset (churn_rate) adds a trailing month
    that has no data yet.
    """
    with path.open(newline="") as f:
        rows = list(csv.reader(f))
    header, body = rows[0], rows[1:]
    kept = [r for r in body if any(cell.strip() for cell in r[1:])]
    with path.open("w", newline="") as f:
        csv.writer(f).writerows([header, *kept])
    return len(kept)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, (metrics, group_by) in DATASETS.items():
        out = OUT_DIR / f"{name}.csv"
        result = subprocess.run(
            query_command(metrics, group_by, out),
            check=False,  # the return code is handled below
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or not out.exists():
            print(
                f"mf query failed for {name}:\n{result.stdout}{result.stderr}",
                file=sys.stderr,
            )
            return 1
        print(f"{name}: {clean(out)} rows -> {out.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
