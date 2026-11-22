#!/usr/bin/env python3
"""Replay the generator's change history into PostgreSQL, the CDC source system.

The CSV path writes the history as nightly extract files. This script turns
the same history into real database changes, so Debezium can capture them:

    _op I  ->  INSERT
    _op U  ->  UPDATE of every column
    _op D  ->  UPDATE updated_at to the delete time, then DELETE, in one
               transaction (so the delete event carries the delete time; the
               consumer drops that touch-update)

An update whose row isn't there yet is applied as an INSERT of the full row.
That happens for a few orders whose first insert landed after the extract
cutoff, so the history only has their later updates; a real source database
would have the order, so the replay creates it.

Changes are applied in source time order (updated_at, then _loaded_at, so a
correction lands after the bad value it fixes), one transaction each.
Re-deliveries are dropped: a database changes once; only extracts repeat.

Progress is kept in the _replay_state table, so a replay can stop at a date
and continue later, like nights of activity:

    python scripts/cdc_source.py replay --until 2026-06-30
    python scripts/cdc_source.py replay            # the rest of the history
    python scripts/cdc_source.py status
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime
from datetime import time as dtime

import generate_raw_data
from cdc_schema import DATA_COLUMNS, KEYS, kind
from raw_data.writer import fmt

DEFAULT_DSN = "host=localhost port=15432 dbname=source user=ecom password=ecom"
TABLE_RANK = {
    t: i for i, t in enumerate(("products", "customers", "subscriptions", "orders"))
}

log = logging.getLogger("cdc_source")


@dataclass(frozen=True)
class Change:
    table: str
    op: str
    row: dict  # source columns as CSV text (None for empty)
    updated_at: datetime
    loaded_at: datetime


def build_changes(generator_args: list[str] | None = None) -> list[Change]:
    """The generator's history as ordered, de-duplicated source changes."""
    args = generate_raw_data.parse_args(generator_args or [])
    seen: set[tuple] = set()
    changes = []
    for table, rows in generate_raw_data.generate(args).items():
        for row in rows:
            values = {c: (fmt(row.get(c)) or None) for c in DATA_COLUMNS[table]}
            identity = (table, row["_op"], tuple(values.values()))
            if identity in seen:  # a re-delivered row, not a new change
                continue
            seen.add(identity)
            changes.append(
                Change(table, row["_op"], values, row["updated_at"], row["_loaded_at"])
            )
    changes.sort(
        key=lambda c: (
            c.updated_at,
            c.loaded_at,
            TABLE_RANK[c.table],
            c.row[KEYS[c.table]],
        )
    )
    return changes


def _cast(table: str, column: str) -> str:
    k = kind(table, column)
    return "%s" if k == "text" else f"%s::{k}"


def statements(change: Change) -> list[tuple[str, list]]:
    """SQL (with parameters) that applies one change to the source table."""
    t, key = change.table, KEYS[change.table]
    cols = DATA_COLUMNS[t]
    if change.op == "I":
        placeholders = ", ".join(_cast(t, c) for c in cols)
        return [
            (
                f"insert into {t} ({', '.join(cols)}) values ({placeholders})",
                [change.row[c] for c in cols],
            )
        ]
    non_key = [c for c in cols if c != key]
    if change.op == "U":
        sets = ", ".join(f"{c} = {_cast(t, c)}" for c in non_key)
        return [
            (
                f"update {t} set {sets} where {key} = %s",
                [change.row[c] for c in non_key] + [change.row[key]],
            )
        ]
    if change.op == "D":
        return [
            (
                f"update {t} set updated_at = %s::timestamp where {key} = %s",
                [change.row["updated_at"], change.row[key]],
            ),
            (f"delete from {t} where {key} = %s", [change.row[key]]),
        ]
    raise ValueError(f"unknown _op {change.op!r}")


def replay(conn, changes: list[Change], until: date | None, limit: int | None) -> dict:
    """Apply the next changes (one transaction each); return what happened."""
    with conn.cursor() as cur:
        cur.execute("select applied from _replay_state where id = 1")
        start = cur.fetchone()[0]
    end_time = datetime.combine(until, dtime.max) if until else None
    applied = start
    missing = 0
    upserted = 0
    began = time.perf_counter()
    for change in changes[start:]:
        if end_time and change.updated_at > end_time:
            break
        if limit is not None and applied - start >= limit:
            break
        with conn.transaction(), conn.cursor() as cur:
            for sql, params in statements(change):
                cur.execute(sql, params)
                if change.op == "U" and cur.rowcount == 0:
                    # The insert is missing from the history: create the row
                    [(insert, values)] = statements(
                        Change(change.table, "I", change.row, None, None)
                    )
                    cur.execute(insert, values)
                    upserted += 1
                elif change.op == "D" and cur.rowcount == 0:
                    missing += 1  # delete of a row that isn't there
            cur.execute(
                "update _replay_state set applied = %s where id = 1", [applied + 1]
            )
        applied += 1
    seconds = time.perf_counter() - began
    return {
        "applied": applied - start,
        "total_applied": applied,
        "remaining": len(changes) - applied,
        "missing_rows": missing,
        "upserted": upserted,
        "seconds": round(seconds, 1),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["replay", "status"])
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--until", type=date.fromisoformat, help="stop after this day")
    parser.add_argument("--limit", type=int, help="apply at most this many changes")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    import psycopg

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)
    changes = build_changes()
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        if args.command == "status":
            with conn.cursor() as cur:
                cur.execute("select applied from _replay_state where id = 1")
                done = cur.fetchone()[0]
            print(f"{done} of {len(changes)} changes applied")
            return 0
        result = replay(conn, changes, args.until, args.limit)
    rate = result["applied"] / result["seconds"] if result["seconds"] else 0
    print(
        f"applied {result['applied']} changes in {result['seconds']} s "
        f"({rate:.0f}/s); {result['total_applied']} of {len(changes)} done, "
        f"{result['upserted']} updates inserted a missing row, "
        f"{result['missing_rows']} deletes found no row"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
