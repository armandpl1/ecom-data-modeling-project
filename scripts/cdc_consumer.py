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
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
from cdc_schema import DATA_COLUMNS, KEYS, TABLES, kind, topic
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


@dataclass(frozen=True)
class BatchResult:
    table: str
    batch_id: str | None
    events: int
    rows: int


def format_value(table: str, column: str, value):
    """Debezium JSON value -> the CSV text the generator writes."""
    if value is None:
        return None
    k = kind(table, column)
    if k == "timestamp":  # io.debezium.time.MicroTimestamp
        return (EPOCH + timedelta(microseconds=int(value))).strftime(TS_FMT)
    if k == "date":  # io.debezium.time.Date: days since epoch
        return (date(1970, 1, 1) + timedelta(days=int(value))).isoformat()
    if k == "boolean":
        return "true" if value else "false"
    return str(value)  # numeric arrives as text (decimal.handling.mode=string)


def map_event(table: str, offset: int, value: bytes | str | None) -> Event:
    """One Kafka message -> an Event with a raw-layer row (or a tombstone)."""
    if value is None:
        return Event(offset, None, None)
    envelope = json.loads(value)
    op = OPS[envelope["op"]]
    image = envelope["before"] if op == "D" else envelope["after"]
    source = envelope.get("source") or {}
    commit_us = source.get("ts_us") or source.get("ts_ms", 0) * 1000
    row = {c: format_value(table, c, image.get(c)) for c in DATA_COLUMNS[table]}
    row["_op"] = op
    row["_loaded_at"] = (EPOCH + timedelta(microseconds=commit_us)).strftime(LOADED_AT_FMT)
    return Event(offset, row, source.get("txId"))


def drop_delete_touches(table: str, events: list[Event]) -> list[Event]:
    """Drop the update the source makes just before a delete (same transaction)."""
    key = KEYS[table]
    deletes = {
        (e.row[key], e.tx_id) for e in events if e.row and e.row["_op"] == "D"
    }
    return [
        e
        for e in events
        if not (e.row and e.row["_op"] == "U" and (e.row[key], e.tx_id) in deletes)
    ]


def ensure_tables(con: duckdb.DuckDBPyConnection, table: str) -> None:
    ensure_load_log(con)
    con.execute(
        """
        create table if not exists raw._cdc_offsets (
            topic       varchar not null,
            partition   integer not null,
            next_offset bigint  not null,
            primary key (topic, partition)
        )
        """
    )
    columns = ", ".join(f"{c} varchar" for c in COLUMNS[table])
    con.execute(
        f"create table if not exists raw.{table} ({columns}, _ingested_at timestamp, "
        "_batch_id varchar, _file_name varchar)"
    )


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
            with tempfile.NamedTemporaryFile("w", suffix=".csv", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS[table])
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
            if max_events is not None:
                high = min(high, start + max(max_events - len(events), 0))
            if start >= high:
                continue
            consumer.assign([TopicPartition(name, partition, start)])
            position = start
            while position < high:
                for message in consumer.consume(min(500, high - position), timeout=10):
                    if message.error():
                        raise RuntimeError(message.error())
                    events.append(map_event(table, message.offset(), message.value()))
                    position = message.offset() + 1
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
    results = []
    with duckdb.connect(str(database)) as con:
        for table in TABLES:
            ensure_tables(con, table)
            events, ranges = read_new_events(
                bootstrap, table, stored_offsets(con, topic(table)), max_events
            )
            if not ranges:
                results.append(BatchResult(table, None, 0, 0))
                continue
            results.append(write_batch(con, table, events, ranges, fail_before_commit))
    return results


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
        results = run_once(args.bootstrap, args.database, args.max_events)
        loaded = [r for r in results if r.rows]
        if loaded or not args.follow:
            print(
                "; ".join(f"{r.table}: {r.rows} rows from {r.events} events" for r in results)
            )
        if not args.follow:
            return 0
        if args.duration and time.monotonic() - began >= args.duration:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
