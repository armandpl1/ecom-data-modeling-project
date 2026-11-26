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
    # An order header change replays before its lines (same updated_at)
    t: i
    for i, t in enumerate(
        ("products", "customers", "subscriptions", "orders", "order_lines")
    )
}

log = logging.getLogger("cdc_source")


@dataclass(frozen=True)
class Change:
    """One change to one source row, as the generator recorded it.

    Attributes:
        table: source table, e.g. ``"orders"`` (a key of ``cdc_schema.KEYS``).
        op: ``"I"`` (insert), ``"U"`` (update) or ``"D"`` (delete), the
            generator's ``_op``.
        row: every source column (``cdc_schema.DATA_COLUMNS[table]``) as CSV
            text, ``None`` for empty. Statements cast the text to the
            column's type.
        updated_at: when the change happened at the source; the replay order.
        loaded_at: when the extract carrying it was loaded; breaks ties, so
            a correction lands after the value it fixes.

    Example:
        >>> from datetime import datetime
        >>> change = Change(
        ...     table="customers",
        ...     op="D",
        ...     row={"customer_id": "C00449", "updated_at": "2024-12-31 08:18:03"},
        ...     updated_at=datetime(2024, 12, 31, 8, 18, 3),
        ...     loaded_at=datetime(2025, 1, 1, 2, 0),
        ... )
        >>> change.op, change.row["customer_id"]
        ('D', 'C00449')
    """

    table: str
    op: str
    row: dict  # source columns as CSV text (None for empty)
    updated_at: datetime
    loaded_at: datetime


def build_changes(generator_args: list[str] | None = None) -> list[Change]:
    """The generator's history as ordered, de-duplicated source changes.

    Runs the raw data generator (no files written) and turns every row it
    produced into a ``Change``:

    - **De-duplicated:** a re-delivered row (same table, op and values) is
      kept once. Extracts repeat; a database changes once.
    - **Ordered** the way the source database would have applied them: by
      ``updated_at``, then ``loaded_at`` (a correction after the bad value
      it fixes), then table (an order header before its lines at the same
      time), then key, so the order is the same on every run.

    Args:
        generator_args: command-line arguments for ``generate_raw_data``,
            e.g. ``["--seed", "7"]``. Default: the generator's defaults, the
            same history the CSV path writes.

    Returns:
        Every change, ready for ``replay``. With the defaults: 32,473
        changes (17,973 inserts, 14,484 updates, 16 deletes), in about 2 s.

    Example:
        >>> changes = build_changes()
        >>> len(changes)
        32473
        >>> first = changes[0]
        >>> first.table, first.op, first.row["customer_id"]
        ('customers', 'I', 'C00310')
        >>> all(a.updated_at <= b.updated_at for a, b in zip(changes, changes[1:]))
        True
    """
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
    """The SQL placeholder for one column: ``%s``, cast if it isn't text.

    Values travel as CSV text, so typed columns (``cdc_schema.KINDS``) are
    cast in the statement instead of parsed in Python.

    Example:
        >>> _cast("products", "unit_price")
        '%s::numeric'
        >>> _cast("products", "product_name")
        '%s'
    """
    k = kind(table, column)
    return "%s" if k == "text" else f"%s::{k}"


def statements(change: Change) -> list[tuple[str, list]]:
    """SQL (with parameters) that applies one change to the source table.

    - ``I``: one INSERT of every column.
    - ``U``: one UPDATE of every non-key column, by key.
    - ``D``: an UPDATE of ``updated_at`` to the delete time, then the DELETE.
      ``replay`` runs both in one transaction, so Debezium's delete event
      carries the delete time; the consumer drops the touch-update.

    Placeholders are psycopg's ``%s``; values stay text and are cast in SQL.

    Args:
        change: the change to apply.

    Returns:
        ``(sql, params)`` pairs, to run in order.

    Raises:
        ValueError: ``change.op`` isn't ``I``, ``U`` or ``D``.

    Example (a real product update from the history):
        >>> row = {
        ...     "product_id": "P0073", "sku": "OUT-28776",
        ...     "product_name": "Deluxe Tent - MediumOrchid", "category": "Outdoors",
        ...     "unit_price": "277.99", "currency": "USD", "is_active": "true",
        ...     "created_at": "2024-01-10 09:18:20",
        ...     "updated_at": "2024-01-11 03:59:12",
        ... }
        >>> [(sql, params)] = statements(Change("products", "U", row, None, None))
        >>> print(sql)  # doctest: +NORMALIZE_WHITESPACE
        update products set sku = %s, product_name = %s, category = %s,
        unit_price = %s::numeric, currency = %s, is_active = %s::boolean,
        created_at = %s::timestamp, updated_at = %s::timestamp
        where product_id = %s
        >>> params[-1]  # the key comes last, for the where clause
        'P0073'

        A delete is two statements:

        >>> for sql, params in statements(Change("products", "D", row, None, None)):
        ...     print(sql)
        ...     print("   ", params)
        update products set updated_at = %s::timestamp where product_id = %s
            ['2024-01-11 03:59:12', 'P0073']
        delete from products where product_id = %s
            ['P0073']
    """
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
    """Apply the next changes (one transaction each); return what happened.

    Resumes where the last replay stopped: ``_replay_state.applied`` counts
    the changes already applied, and each transaction applies one change and
    advances the count, so a crash never applies a change twice or skips one.

    Two gaps in the history are handled the way a real source would:

    - An UPDATE that finds no row (the insert happened before the extract
      cutoff) inserts the full row instead; counted in ``upserted``.
    - A DELETE that finds no row is skipped; counted in ``missing_rows``.

    Args:
        conn: an autocommit psycopg connection to the source database
            (``cdc/postgres/init.sql`` applied, so ``_replay_state`` exists).
        changes: ``build_changes()``; always the full list, the state picks
            the starting point.
        until: stop after the last change on this day (inclusive), e.g.
            ``date(2026, 6, 30)``. ``None``: no date limit.
        limit: apply at most this many changes. ``None``: no limit.

    Returns:
        A summary: ``applied`` (this call), ``total_applied`` (all calls),
        ``remaining``, ``upserted``, ``missing_rows`` and ``seconds``.

    Example (needs the source database: ``make cdc-up``):
        >>> import psycopg  # doctest: +SKIP
        >>> conn = psycopg.connect(DEFAULT_DSN, autocommit=True)  # doctest: +SKIP
        >>> replay(conn, build_changes(), date(2026, 6, 30), None)  # doctest: +SKIP
        {'applied': ..., 'total_applied': ..., 'remaining': ..., 'missing_rows': ...,
         'upserted': ..., 'seconds': ...}

        The full history replays in about 23 s (README, CDC section).
    """
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
    """Parse the command line: ``replay`` or ``status``, with options.

    Example:
        >>> args = parse_args(["replay", "--until", "2026-06-30"])
        >>> args.command, args.until, args.limit
        ('replay', datetime.date(2026, 6, 30), None)
        >>> parse_args(["status"]).dsn == DEFAULT_DSN
        True
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["replay", "status"])
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--until", type=date.fromisoformat, help="stop after this day")
    parser.add_argument("--limit", type=int, help="apply at most this many changes")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the command and print a one-line summary; return the exit code.

    Example (needs the source database: ``make cdc-up``)::

        $ python scripts/cdc_source.py replay --until 2024-06-30
        applied <n> changes in <s> s (<rate>/s); <done> of 32473 done,
        <u> updates inserted a missing row, <m> deletes found no row
        $ python scripts/cdc_source.py status
        <done> of 32473 changes applied

    (One line each; wrapped here. 32473 is the default history's size.)
    """
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
