#!/usr/bin/env python3
"""Load Debezium change events from Redpanda (Kafka) into the raw layer.

Reads the topics ecom.public.<table> and appends one row per change to
raw.<table>, in the same columns and text format as the CSV extracts:

    Debezium op c / r (snapshot)  ->  _op I, row = after
    Debezium op u                 ->  _op U, row = after
    Debezium op d                 ->  _op D, row = before (REPLICA IDENTITY FULL)
    tombstone (null value)        ->  skipped

_loaded_at is the source commit time (microseconds), so when two changes share
an updated_at, the later commit wins in Bronze, as the later load does in the
CSV path. The source simulator sets updated_at just before a DELETE in the same
transaction; that touch-update is dropped here.

Exactly once: each run appends at most one batch per table and stores the
next offset of every partition in raw._cdc_offsets, in the same DuckDB
transaction as the rows. A crash before the commit leaves neither rows nor
offsets, so the next run re-reads the same events; nothing is lost or doubled.
Each batch is also recorded in raw._load_log, like an extract file.

A batch never ends inside a source transaction: when --max-events cuts a
partition short, the trailing events of the unfinished transaction are left
for the next run, so a delete and the update just before it stay together.

Schema changes: a column added in Postgres shows up as an extra field in the
events. It is added to raw.<table> as varchar (raw is all text) in the same
transaction as the batch and recorded in raw._cdc_schema_changes, so nothing
is dropped. Staging selects its columns by name, so dbt keeps working; the new
column reaches Silver once a staging model selects it. A dropped column just
arrives as null.

Types: every event carries its schema (value.schemas.enable=true), and values
are decoded by the type in that schema, so a new timestamp column lands as
timestamp text, not microseconds. When a known column's type changes at the
source (say `timestamp` to `timestamptz`), that table stops loading: its
offsets don't move, nothing is lost, the change is recorded in
raw._cdc_type_changes, the other tables still load, and the run exits 3.
Once the change is reviewed and added to cdc_schema.ALLOWED_SOURCE_KINDS,
the next run loads everything (docs/runbooks/cdc-type-change.md).

Usage:
    python scripts/cdc_consumer.py                       # one catch-up run
    python scripts/cdc_consumer.py --follow --interval 1 # keep consuming
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
from cdc_schema import (
    DATA_COLUMNS,
    KEYS,
    METADATA,
    TABLES,
    accepted_kinds,
    kind,
    source_kind,
    topic,
)
from load_raw import ensure_load_log
from raw_data.config import COLUMNS, TS_FMT

DEFAULT_BOOTSTRAP = "localhost:19092"
DEFAULT_DATABASE = Path("ecom.duckdb")
LOADED_AT_FMT = "%Y-%m-%d %H:%M:%S.%f"
EPOCH = datetime(1970, 1, 1)
OPS = {"c": "I", "r": "I", "u": "U", "d": "D"}

log = logging.getLogger("cdc_consumer")


@dataclass(frozen=True)
class Event:
    offset: int
    row: dict | None  # raw-layer columns as text; None for a tombstone
    tx_id: int | None
    kinds: dict | None = None  # column -> source kind, from the event's schema


@dataclass(frozen=True)
class TypeChange:
    table: str
    column: str
    expected: str  # the kinds that load without review
    observed: str
    offset: int


class SourceTypeChanged(Exception):
    """A known column arrived with a different type than cdc_schema expects."""

    def __init__(self, changes: list[TypeChange]) -> None:
        self.changes = changes
        super().__init__(
            "; ".join(
                f"{c.table}.{c.column}: expected {c.expected}, got {c.observed} "
                f"(offset {c.offset})"
                for c in changes
            )
        )


class TablesBlocked(Exception):
    """Some tables were not loaded because of source type changes."""

    def __init__(self, results: list, changes: list[TypeChange]) -> None:
        self.results, self.changes = results, changes
        tables = sorted({c.table for c in changes})
        super().__init__(f"blocked by source type changes: {', '.join(tables)}")


@dataclass(frozen=True)
class BatchResult:
    table: str
    batch_id: str | None
    events: int
    rows: int


def format_value(table: str, column: str, value, observed: str | None = None):
    """Debezium JSON value -> the CSV text the generator writes.

    `observed` is the source kind from the event's schema; without one (an
    event written without schemas), the column's kind in cdc_schema is used.
    """
    if value is None:
        return None
    if observed is not None:
        return format_observed(value, observed)
    k = kind(table, column)
    if k == "timestamp":  # io.debezium.time.MicroTimestamp
        return (EPOCH + timedelta(microseconds=int(value))).strftime(TS_FMT)
    if k == "date":  # io.debezium.time.Date: days since epoch
        return (date(1970, 1, 1) + timedelta(days=int(value))).isoformat()
    if k == "boolean":
        return "true" if value else "false"
    if isinstance(value, (dict, list)):  # e.g. json columns added later
        return json.dumps(value, sort_keys=True)
    return str(value)  # numeric arrives as text (decimal.handling.mode=string)


def format_observed(value, observed: str):
    """Decode a value by its source kind into the CSV text format."""
    if observed == "timestamp":  # microseconds since the epoch
        return (EPOCH + timedelta(microseconds=int(value))).strftime(TS_FMT)
    if observed == "timestamp_ms":
        return (EPOCH + timedelta(milliseconds=int(value))).strftime(TS_FMT)
    if observed == "timestamp_ns":
        return (EPOCH + timedelta(microseconds=int(value) // 1000)).strftime(TS_FMT)
    if observed == "timestamptz":  # ISO 8601 in UTC, e.g. 2024-01-15T05:12:27Z
        moment = datetime.fromisoformat(str(value))
        return moment.astimezone(UTC).replace(tzinfo=None).strftime(TS_FMT)
    if observed == "date":  # days since the epoch
        return (date(1970, 1, 1) + timedelta(days=int(value))).isoformat()
    if observed == "boolean":
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def schema_kinds(schema: dict) -> dict[str, str]:
    """column -> source kind, from a Debezium envelope schema."""
    for part in schema.get("fields", []):
        if part.get("field") in ("after", "before") and part.get("fields"):
            return {f["field"]: source_kind(f) for f in part["fields"]}
    return {}


def type_changes(table: str, offset: int, kinds: dict[str, str]) -> list[TypeChange]:
    changes = []
    for column in DATA_COLUMNS[table]:
        observed = kinds.get(column)
        if observed is None:  # dropped at the source: arrives as null
            continue
        accepted = accepted_kinds(table, column)
        if observed not in accepted:
            expected = " or ".join(sorted(accepted))
            changes.append(TypeChange(table, column, expected, observed, offset))
    return changes


def map_event(table: str, offset: int, value: bytes | str | None) -> Event:
    """One Kafka message -> an Event with a raw-layer row (or a tombstone).

    Raises SourceTypeChanged if a known column's type changed at the source.
    """
    if value is None:
        return Event(offset, None, None)
    message = json.loads(value)
    kinds: dict[str, str] | None = None
    if "schema" in message and "payload" in message:
        kinds = schema_kinds(message["schema"])
        if changes := type_changes(table, offset, kinds):
            raise SourceTypeChanged(changes)
        envelope = message["payload"]
    else:
        envelope = message
    op = OPS[envelope["op"]]
    image = envelope["before"] if op == "D" else envelope["after"]
    source = envelope.get("source") or {}
    commit_us = source.get("ts_us") or source.get("ts_ms", 0) * 1000

    def observed(column: str) -> str | None:
        return kinds.get(column) if kinds else None

    row = {
        c: format_value(table, c, image.get(c), observed(c))
        for c in DATA_COLUMNS[table]
    }
    # Columns added at the source after this code was written: keep them too,
    # decoded by their own type when the event has a schema
    row |= {
        c: format_value(table, c, v, observed(c) or "string")
        for c, v in image.items()
        if c not in row and c not in METADATA
    }
    row["_op"] = op
    row["_loaded_at"] = (EPOCH + timedelta(microseconds=commit_us)).strftime(
        LOADED_AT_FMT
    )
    return Event(offset, row, source.get("txId"), kinds)


def drop_delete_touches(table: str, events: list[Event]) -> list[Event]:
    """Drop the update the source makes just before a delete (same transaction)."""
    key = KEYS[table]
    deletes = {(e.row[key], e.tx_id) for e in events if e.row and e.row["_op"] == "D"}
    return [
        e
        for e in events
        if not (e.row and e.row["_op"] == "U" and (e.row[key], e.tx_id) in deletes)
    ]


def trim_partial_transaction(events: list[Event]) -> list[Event]:
    """Drop the trailing events of a transaction the batch cut in the middle.

    Only for a capped read (more events wait after the last one). If one
    transaction is larger than the whole batch it is kept, so the run still
    makes progress.
    """
    last_tx = events[-1].tx_id if events else None
    if last_tx is None:
        return events
    cut = len(events)
    while cut > 0 and events[cut - 1].tx_id == last_tx:
        cut -= 1
    if cut == 0:
        log.warning("transaction %s is larger than --max-events; kept whole", last_tx)
        return events
    return events[:cut]


def ensure_tables(con: duckdb.DuckDBPyConnection, table: str) -> None:
    ensure_load_log(con)
    con.execute("""
        create table if not exists raw._cdc_offsets (
            topic       varchar not null,
            partition   integer not null,
            next_offset bigint  not null,
            primary key (topic, partition)
        )
        """)
    con.execute("""
        create table if not exists raw._cdc_schema_changes (
            table_name  varchar   not null,
            column_name varchar   not null,
            batch_id    varchar   not null,
            detected_at timestamp not null,
            primary key (table_name, column_name)
        )
        """)
    con.execute(
        "alter table raw._cdc_schema_changes "
        "add column if not exists source_kind varchar"
    )
    con.execute("""
        create table if not exists raw._cdc_type_changes (
            table_name  varchar   not null,
            column_name varchar   not null,
            expected    varchar   not null,
            observed    varchar   not null,
            first_offset bigint   not null,
            detected_at timestamp not null,
            primary key (table_name, column_name, observed)
        )
        """)
    columns = ", ".join(f"{c} varchar" for c in COLUMNS[table])
    con.execute(
        f"create table if not exists raw.{table} ({columns}, _ingested_at timestamp, "
        "_batch_id varchar, _file_name varchar)"
    )


LINEAGE_COLUMNS = ("_ingested_at", "_batch_id", "_file_name")


def raw_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    rows = con.execute(
        "select column_name from information_schema.columns "
        "where table_schema = 'raw' and table_name = ? order by ordinal_position",
        [table],
    ).fetchall()
    return [r[0] for r in rows if r[0] not in LINEAGE_COLUMNS]


def add_new_columns(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rows: list[dict],
    batch_id: str,
    kinds: dict[str, str] | None = None,
) -> list[str]:
    """Add columns the source gained since the raw table was created."""
    existing = raw_columns(con, table)
    new = [c for c in dict.fromkeys(k for r in rows for k in r) if c not in existing]
    for column in new:
        if not column.isidentifier():
            raise ValueError(f"unsafe column name from the source: {column!r}")
        con.execute(f'alter table raw.{table} add column "{column}" varchar')
        con.execute(
            "insert or ignore into raw._cdc_schema_changes "
            "(table_name, column_name, batch_id, detected_at, source_kind) "
            "values (?, ?, ?, current_timestamp::timestamp, ?)",
            [table, column, batch_id, (kinds or {}).get(column)],
        )
        log.warning(
            "raw.%s: new source column %s added (varchar); "
            "select it in the staging model to use it",
            table,
            column,
        )
    return existing + new


def stored_offsets(con: duckdb.DuckDBPyConnection, name: str) -> dict[int, int]:
    rows = con.execute(
        "select partition, next_offset from raw._cdc_offsets where topic = ?", [name]
    ).fetchall()
    return dict(rows)


def write_batch(
    con: duckdb.DuckDBPyConnection,
    table: str,
    events: list[Event],
    ranges: dict[int, tuple[int, int]],
    fail_before_commit: bool = False,
) -> BatchResult:
    """Append one batch and advance the offsets, in a single transaction.

    ranges: partition -> (first offset read, next offset to read).
    """
    name = topic(table)
    span = "|".join(f"{p}:{a}-{b}" for p, (a, b) in sorted(ranges.items()))
    checksum = hashlib.sha256(f"{name}|{span}".encode()).hexdigest()
    batch_id = f"{table}-cdc-{checksum[:12]}"
    file_name = f"cdc://{name}/{span}"
    rows = [e.row for e in drop_delete_touches(table, events) if e.row is not None]

    con.execute("begin transaction")
    try:
        if rows:
            kinds: dict[str, str] = {}
            for e in events:
                kinds |= e.kinds or {}
            columns = add_new_columns(con, table, rows, batch_id, kinds)
            with tempfile.NamedTemporaryFile("w", suffix=".csv", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
                f.flush()
                con.execute(
                    f"insert into raw.{table} by name "
                    "select *, current_timestamp::timestamp as _ingested_at, "
                    "? as _batch_id, ? as _file_name "
                    "from read_csv(?, header = true, all_varchar = true)",
                    [batch_id, file_name, f.name],
                )
            con.execute(
                "insert into raw._load_log "
                "values (?, ?, ?, ?, ?, current_timestamp::timestamp)",
                [batch_id, table, file_name, len(rows), checksum],
            )
        for partition, (_, next_offset) in ranges.items():
            con.execute(
                "insert or replace into raw._cdc_offsets values (?, ?, ?)",
                [name, partition, next_offset],
            )
        if fail_before_commit:
            raise RuntimeError("simulated crash before commit")
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    return BatchResult(table, batch_id if rows else None, len(events), len(rows))


def read_new_events(
    bootstrap: str, table: str, offsets: dict[int, int], max_events: int | None
) -> tuple[list[Event], dict[int, tuple[int, int]]]:
    """Every event after the stored offsets, up to the partitions' current end."""
    from confluent_kafka import Consumer, TopicPartition

    name = topic(table)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ecom-raw-loader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        metadata = consumer.list_topics(name, timeout=10).topics.get(name)
        if metadata is None or metadata.error is not None:
            return [], {}
        events: list[Event] = []
        ranges: dict[int, tuple[int, int]] = {}
        for partition in sorted(metadata.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(name, partition), timeout=10
            )
            start = max(offsets.get(partition, low), low)
            end = high
            if max_events is not None:
                high = min(high, start + max(max_events - len(events), 0))
            if start >= high:
                continue
            consumer.assign([TopicPartition(name, partition, start)])
            position = start
            read: list[Event] = []
            while position < high:
                for message in consumer.consume(min(500, high - position), timeout=10):
                    if message.error():
                        raise RuntimeError(message.error())
                    read.append(map_event(table, message.offset(), message.value()))
                    position = message.offset() + 1
            if position < end:  # capped: don't stop inside a transaction
                read = trim_partial_transaction(read)
                position = read[-1].offset + 1
            events += read
            ranges[partition] = (start, position)
        return events, ranges
    finally:
        consumer.close()


def run_once(
    bootstrap: str,
    database: Path,
    max_events: int | None = None,
    fail_before_commit: bool = False,
) -> list[BatchResult]:
    """Load every table once. A table whose source type changed is skipped
    (and recorded); the others still load, then TablesBlocked is raised."""
    results, blocked = [], []
    with duckdb.connect(str(database)) as con:
        for table in TABLES:
            ensure_tables(con, table)
            try:
                events, ranges = read_new_events(
                    bootstrap, table, stored_offsets(con, topic(table)), max_events
                )
            except SourceTypeChanged as err:
                record_type_changes(con, err.changes)
                blocked += err.changes
                results.append(BatchResult(table, None, 0, 0))
                continue
            if not ranges:
                results.append(BatchResult(table, None, 0, 0))
                continue
            results.append(write_batch(con, table, events, ranges, fail_before_commit))
    if blocked:
        raise TablesBlocked(results, blocked)
    return results


def record_type_changes(
    con: duckdb.DuckDBPyConnection, changes: list[TypeChange]
) -> None:
    for c in changes:
        con.execute(
            "insert or ignore into raw._cdc_type_changes "
            "values (?, ?, ?, ?, ?, current_timestamp::timestamp)",
            [c.table, c.column, c.expected, c.observed, c.offset],
        )
        log.error(
            "raw.%s not loaded: source column %s changed type (expected %s, got %s,"
            " offset %s). See docs/runbooks/cdc-type-change.md",
            c.table,
            c.column,
            c.expected,
            c.observed,
            c.offset,
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--max-events", type=int, help="per table, per run")
    parser.add_argument("--follow", action="store_true", help="keep consuming")
    parser.add_argument("--interval", type=float, default=2.0, help="seconds, --follow")
    parser.add_argument("--duration", type=float, help="stop --follow after seconds")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)
    began = time.monotonic()
    while True:
        try:
            results = run_once(args.bootstrap, args.database, args.max_events)
        except TablesBlocked as err:
            print(
                "; ".join(
                    f"{r.table}: {r.rows} rows from {r.events} events"
                    for r in err.results
                )
            )
            for c in err.changes:
                print(
                    f"BLOCKED {c.table}.{c.column}: expected {c.expected}, "
                    f"got {c.observed} (see docs/runbooks/cdc-type-change.md)"
                )
            return 3
        loaded = [r for r in results if r.rows]
        if loaded or not args.follow:
            print(
                "; ".join(
                    f"{r.table}: {r.rows} rows from {r.events} events" for r in results
                )
            )
        if not args.follow:
            return 0
        if args.duration and time.monotonic() - began >= args.duration:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
