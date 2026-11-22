#!/usr/bin/env python3
"""Measure CDC lag: from a commit in PostgreSQL to the row landing in raw.

Makes N email changes to existing customers at a steady rate, then reads the
matching rows in raw.customers and reports how long each took from the source
commit (_loaded_at, UTC) to its insert into the raw layer (_ingested_at, local
time, converted to UTC). Run the consumer in follow mode against the same
DuckDB file while this runs:

    python scripts/cdc_consumer.py --follow --interval 1 --database lag.duckdb &
    python scripts/cdc_lag.py --database lag.duckdb --changes 200 --rate 10
"""

from __future__ import annotations

import argparse
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb

DEFAULT_DSN = "host=localhost port=15432 dbname=source user=ecom password=ecom"
MARKER = "lagtest"


def make_changes(dsn: str, n: int, rate: float, run_id: str) -> list[str]:
    """Update n customers' emails, one commit each; return the emails written."""
    import psycopg

    emails = []
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "select customer_id from customers order by customer_id limit %s", [n]
        )
        ids = [r[0] for r in cur.fetchall()]
        for i, customer_id in enumerate(ids):
            email = f"{MARKER}-{run_id}-{i}@example.com"
            cur.execute(
                "update customers set email = %s, updated_at = now()::timestamp(0) "
                "where customer_id = %s",
                [email, customer_id],
            )
            emails.append(email)
            time.sleep(1 / rate)
    return emails


def lags_seconds(database: Path, emails: list[str], wait: float) -> list[float]:
    """Seconds from commit to raw insert for each email, waiting for stragglers."""
    utc_offset = datetime.now().astimezone().utcoffset()
    deadline = time.monotonic() + wait
    rows: list = []
    while True:
        try:
            with duckdb.connect(str(database), read_only=True) as con:
                rows = con.execute(
                    "select cast(_loaded_at as timestamp), _ingested_at "
                    "from raw.customers where email in (select unnest(?::varchar[]))",
                    [emails],
                ).fetchall()
        except duckdb.IOException:
            pass  # the consumer holds DuckDB's write lock mid-batch: try again
        if len(rows) >= len(emails) or time.monotonic() > deadline:
            break
        time.sleep(1)
    return [
        ((ingested - utc_offset) - committed).total_seconds()
        for committed, ingested in rows
    ]


def percentile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(p / 100 * (len(ordered) - 1)))]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--changes", type=int, default=200)
    parser.add_argument("--rate", type=float, default=10.0, help="changes per second")
    parser.add_argument(
        "--wait", type=float, default=60.0, help="seconds to wait at the end"
    )
    args = parser.parse_args(argv)

    run_id = datetime.now(UTC).strftime("%H%M%S")
    emails = make_changes(args.dsn, args.changes, args.rate, run_id)
    lags = lags_seconds(args.database, emails, args.wait)
    if not lags:
        print("no changes arrived: is the consumer running with --follow?")
        return 1
    print(
        f"{len(lags)} of {len(emails)} changes arrived; lag p50 "
        f"{statistics.median(lags):.2f} s, p95 {percentile(lags, 95):.2f} s, "
        f"max {max(lags):.2f} s"
    )
    return 0 if len(lags) == len(emails) else 1


if __name__ == "__main__":
    raise SystemExit(main())
