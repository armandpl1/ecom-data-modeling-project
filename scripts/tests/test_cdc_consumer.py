"""Tests for scripts/cdc_consumer.py: event mapping and exactly-once batches.

No Kafka or Docker needed: events are built by hand, as Debezium writes them.
"""

import json
from pathlib import Path

import cdc_consumer
import cdc_schema
import duckdb
import pytest
from cdc_consumer import (
    Event,
    SourceTypeChanged,
    TablesBlocked,
    drop_delete_touches,
    ensure_tables,
    format_value,
    map_event,
    trim_partial_transaction,
    write_batch,
)
from cdc_schema import source_kind

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
            write_batch(
                con, "customers", events(3), {0: (0, 3)}, fail_before_commit=True
            )
        assert con.execute("select count(*) from raw.customers").fetchone()[0] == 0
        assert con.execute("select count(*) from raw._load_log").fetchone()[0] == 0
        assert con.execute("select count(*) from raw._cdc_offsets").fetchone()[0] == 0
        # The retry reads the same events again and lands them once
        write_batch(con, "customers", events(3), {0: (0, 3)})
        assert con.execute("select count(*) from raw.customers").fetchone()[0] == 3


def test_tombstone_only_batch_advances_offsets_without_a_log_row(
    tmp_path: Path,
) -> None:
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        ensure_tables(con, "customers")
        tombstones = [Event(5, None, None), Event(6, None, None)]
        result = write_batch(con, "customers", tombstones, {0: (5, 7)})
        assert (result.rows, result.batch_id) == (0, None)
        assert con.execute("select count(*) from raw._load_log").fetchone()[0] == 0
        assert (
            con.execute("select next_offset from raw._cdc_offsets").fetchone()[0] == 7
        )


def tx_events(txs: list[int]) -> list[Event]:
    return [
        map_event("customers", i, envelope("u", after=customer(), tx=tx))
        for i, tx in enumerate(txs)
    ]


def test_a_capped_batch_stops_at_the_last_whole_transaction() -> None:
    # tx 3 is a touch-update + delete; the cap fell between them
    kept = trim_partial_transaction(tx_events([1, 2, 3]))
    assert [e.offset for e in kept] == [0, 1]


def test_a_transaction_larger_than_the_batch_is_kept_whole() -> None:
    kept = trim_partial_transaction(tx_events([4, 4, 4]))
    assert [e.offset for e in kept] == [0, 1, 2]


def test_tombstones_at_the_end_are_not_trimmed() -> None:
    batch = [*tx_events([1, 2]), Event(2, None, None)]
    assert trim_partial_transaction(batch) == batch


def test_a_column_added_at_the_source_is_kept(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        ensure_tables(con, "customers")
        write_batch(con, "customers", events(1), {0: (0, 1)})
        added = map_event(
            "customers",
            1,
            envelope(
                "u", after=customer(loyalty_tier="gold", prefs={"sms": True}), tx=9
            ),
        )
        result = write_batch(con, "customers", [added], {0: (1, 2)})
        rows = con.execute(
            "select loyalty_tier, prefs from raw.customers order by _loaded_at"
        ).fetchall()
        assert rows == [(None, None), ("gold", '{"sms": true}')]
        changes = con.execute(
            "select table_name, column_name, batch_id from raw._cdc_schema_changes "
            "order by column_name"
        ).fetchall()
        assert changes == [
            ("customers", "loyalty_tier", result.batch_id),
            ("customers", "prefs", result.batch_id),
        ]
        # A later batch without the column still loads (dropped column -> null)
        write_batch(con, "customers", events(1, start=2), {0: (2, 3)})
        assert con.execute("select count(*) from raw.customers").fetchone()[0] == 3


def test_an_unsafe_column_name_is_refused(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        ensure_tables(con, "customers")
        bad = map_event("customers", 0, envelope("c", after=customer(**{"x; drop": 1})))
        with pytest.raises(ValueError, match="unsafe column"):
            write_batch(con, "customers", [bad], {0: (0, 1)})
        assert con.execute("select count(*) from raw._cdc_offsets").fetchone()[0] == 0


# --- Event schemas and source type changes ------------------------------------

DEBEZIUM_TYPES = {
    "marketing_opt_in": {"type": "boolean"},
    "created_at": {"type": "int64", "name": "io.debezium.time.MicroTimestamp"},
    "updated_at": {"type": "int64", "name": "io.debezium.time.MicroTimestamp"},
}


def with_schema(raw: str, overrides: dict | None = None) -> str:
    """Wrap an envelope the way Debezium does with value.schemas.enable=true."""
    payload = json.loads(raw)
    image = payload["after"] or payload["before"]
    types = DEBEZIUM_TYPES | (overrides or {})
    fields = [{"field": c, **types.get(c, {"type": "string"})} for c in image]
    schema = {
        "type": "struct",
        "fields": [
            {"type": "struct", "field": "before", "fields": fields},
            {"type": "struct", "field": "after", "fields": fields},
        ],
    }
    return json.dumps({"schema": schema, "payload": payload})


def test_source_kinds_from_debezium_field_schemas() -> None:
    assert source_kind(
        {"type": "int64", "name": "io.debezium.time.MicroTimestamp"}
    ) == ("timestamp")
    assert source_kind(
        {"type": "string", "name": "io.debezium.time.ZonedTimestamp"}
    ) == ("timestamptz")
    assert source_kind({"type": "int32", "name": "io.debezium.time.Date"}) == "date"
    assert source_kind({"type": "int32"}) == "integer"
    assert source_kind({"type": "string"}) == "string"
    assert source_kind({"type": "bytes", "name": "x.Custom"}) == "bytes:x.Custom"


def test_an_event_with_a_schema_maps_like_one_without() -> None:
    plain = map_event("customers", 0, envelope("c", after=customer()))
    typed = map_event("customers", 0, with_schema(envelope("c", after=customer())))
    assert typed.row == plain.row
    assert typed.kinds["created_at"] == "timestamp"


def test_a_new_column_is_decoded_by_its_own_type() -> None:
    event = map_event(
        "customers",
        0,
        with_schema(
            envelope("u", after=customer(last_login_at=TS_2024)),
            {
                "last_login_at": {
                    "type": "int64",
                    "name": "io.debezium.time.MicroTimestamp",
                }
            },
        ),
    )
    assert event.row["last_login_at"] == "2024-01-01 00:00:00"  # not microseconds


def test_a_type_change_on_a_known_column_is_refused() -> None:
    changed = with_schema(
        envelope("u", after=customer(created_at="2024-01-01T05:00:00.000000Z")),
        {"created_at": {"type": "string", "name": "io.debezium.time.ZonedTimestamp"}},
    )
    with pytest.raises(SourceTypeChanged) as err:
        map_event("customers", 42, changed)
    [change] = err.value.changes
    assert (change.column, change.expected, change.observed, change.offset) == (
        "created_at",
        "timestamp",
        "timestamptz",
        42,
    )


def test_a_reviewed_type_change_loads_in_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        cdc_schema.ALLOWED_SOURCE_KINDS, "customers", {"created_at": {"timestamptz"}}
    )
    changed = with_schema(
        envelope("u", after=customer(created_at="2024-01-01T05:00:00-05:00")),
        {"created_at": {"type": "string", "name": "io.debezium.time.ZonedTimestamp"}},
    )
    assert map_event("customers", 0, changed).row["created_at"] == "2024-01-01 10:00:00"


def test_a_blocked_table_keeps_its_offsets_and_the_others_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_read(bootstrap, table, offsets, max_events):
        if table == "customers":
            map_event(
                "customers",
                5,
                with_schema(
                    envelope("u", after=customer(marketing_opt_in="yes")),
                    {"marketing_opt_in": {"type": "string"}},
                ),
            )
        if table == "products":
            return [], {}
        return events(0), {0: (0, 0)}

    monkeypatch.setattr(cdc_consumer, "read_new_events", fake_read)
    db = tmp_path / "w.duckdb"
    with pytest.raises(TablesBlocked) as err:
        cdc_consumer.run_once("unused:9092", db)
    assert [c.table for c in err.value.changes] == ["customers"]
    assert {r.table for r in err.value.results} == set(cdc_consumer.TABLES)
    with duckdb.connect(str(db)) as con:
        logged = con.execute(
            "select table_name, column_name, expected, observed, first_offset "
            "from raw._cdc_type_changes"
        ).fetchall()
        assert logged == [("customers", "marketing_opt_in", "boolean", "string", 5)]
        topics = {
            r[0] for r in con.execute("select topic from raw._cdc_offsets").fetchall()
        }
        assert "ecom.public.customers" not in topics  # nothing consumed, nothing lost
        assert "ecom.public.orders" in topics  # the other tables still advanced
