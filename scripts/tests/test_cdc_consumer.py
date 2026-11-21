"""Tests for scripts/cdc_consumer.py: event mapping and exactly-once batches.

No Kafka or Docker needed: events are built by hand, as Debezium writes them.
"""

import json
from pathlib import Path

import duckdb
import pytest
from cdc_consumer import (
    Event,
    drop_delete_touches,
    ensure_tables,
    format_value,
    map_event,
    write_batch,
)

TS_2024 = 1_704_067_200_000_000  # 2024-01-01 00:00:00 in microseconds


def customer(email: str = "a@example.com", **overrides) -> dict:
    row = {
        "customer_id": "C00001",
        "first_name": "Ann",
        "last_name": "Lee",
        "email": email,
        "phone": None,
        "street_address": "1 Main St",
        "city": "Austin",
        "state": "TX",
        "postal_code": "73301",
        "country": "US",
        "customer_segment": "consumer",
        "marketing_opt_in": True,
        "created_at": TS_2024,
        "updated_at": TS_2024 + 3_600_000_000,
    }
    return row | overrides


def envelope(op: str, before=None, after=None, ts_us=TS_2024, tx=7) -> str:
    return json.dumps(
        {
            "op": op,
            "before": before,
            "after": after,
            "source": {"ts_us": ts_us, "ts_ms": ts_us // 1000, "txId": tx},
        }
    )


def test_format_value_matches_the_csv_text() -> None:
    assert format_value("orders", "updated_at", TS_2024) == "2024-01-01 00:00:00"
    assert format_value("subscriptions", "started_at", 19723) == "2024-01-01"
    assert format_value("products", "is_active", False) == "false"
    assert format_value("orders", "unit_price", "22.99") == "22.99"
    assert format_value("orders", "quantity", 3) == "3"
    assert format_value("customers", "phone", None) is None


def test_insert_update_delete_and_snapshot_map_to_raw_rows() -> None:
    inserted = map_event("customers", 0, envelope("c", after=customer()))
    assert inserted.row["_op"] == "I"
    assert inserted.row["updated_at"] == "2024-01-01 01:00:00"
    assert inserted.row["marketing_opt_in"] == "true"
    assert inserted.row["_loaded_at"] == "2024-01-01 00:00:00.000000"
    assert map_event("customers", 1, envelope("r", after=customer())).row["_op"] == "I"
    updated = map_event("customers", 2, envelope("u", after=customer("b@example.com")))
    assert (updated.row["_op"], updated.row["email"]) == ("U", "b@example.com")
    deleted = map_event("customers", 3, envelope("d", before=customer("z@example.com")))
    assert (deleted.row["_op"], deleted.row["email"]) == ("D", "z@example.com")


def test_tombstones_are_kept_as_empty_events() -> None:
    event = map_event("customers", 9, None)
    assert event.offset == 9
    assert event.row is None


def test_the_touch_before_a_delete_is_dropped() -> None:
    touch = map_event("customers", 0, envelope("u", after=customer(), tx=5))
    delete = map_event("customers", 1, envelope("d", before=customer(), tx=5))
    other = map_event("customers", 2, envelope("u", after=customer(), tx=6))
    kept = drop_delete_touches("customers", [touch, delete, other])
    assert [e.offset for e in kept] == [1, 2]


def events(n: int, start: int = 0) -> list[Event]:
    return [
        map_event(
            "customers",
            start + i,
            envelope("u", after=customer(f"{i}@x.com"), ts_us=TS_2024 + i, tx=i),
        )
        for i in range(n)
    ]


def test_a_batch_lands_rows_log_and_offsets_together(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        ensure_tables(con, "customers")
        result = write_batch(con, "customers", events(3), {0: (0, 3)})
        assert result.rows == 3
        assert con.execute("select count(*) from raw.customers").fetchone()[0] == 3
        log = con.execute("select table_name, row_count from raw._load_log").fetchall()
        assert log == [("customers", 3)]
        offsets = con.execute("select next_offset from raw._cdc_offsets").fetchall()
        assert offsets == [(3,)]
        lineage = con.execute(
            "select distinct _batch_id, _file_name from raw.customers"
        ).fetchall()
        assert lineage == [(result.batch_id, "cdc://ecom.public.customers/0:0-3")]


def test_a_crash_before_commit_leaves_nothing(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        ensure_tables(con, "customers")
        with pytest.raises(RuntimeError, match="simulated crash"):
            write_batch(con, "customers", events(3), {0: (0, 3)}, fail_before_commit=True)
        assert con.execute("select count(*) from raw.customers").fetchone()[0] == 0
        assert con.execute("select count(*) from raw._load_log").fetchone()[0] == 0
        assert con.execute("select count(*) from raw._cdc_offsets").fetchone()[0] == 0
        # The retry reads the same events again and lands them once
        write_batch(con, "customers", events(3), {0: (0, 3)})
        assert con.execute("select count(*) from raw.customers").fetchone()[0] == 3


def test_tombstone_only_batch_advances_offsets_without_a_log_row(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        ensure_tables(con, "customers")
        tombstones = [Event(5, None, None), Event(6, None, None)]
        result = write_batch(con, "customers", tombstones, {0: (5, 7)})
        assert (result.rows, result.batch_id) == (0, None)
        assert con.execute("select count(*) from raw._load_log").fetchone()[0] == 0
        assert con.execute("select next_offset from raw._cdc_offsets").fetchone()[0] == 7
