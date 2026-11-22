"""Tests for scripts/cdc_source.py: replay order, de-duplication and SQL."""

import pytest
from cdc_schema import DATA_COLUMNS, KEYS, KINDS, TABLES
from cdc_source import Change, build_changes, statements

SMALL = ["--customers", "40", "--products", "10", "--orders", "120"]


@pytest.fixture(scope="module")
def changes() -> list[Change]:
    return build_changes(SMALL)


def test_changes_are_in_source_time_order(changes: list[Change]) -> None:
    times = [(c.updated_at, c.loaded_at) for c in changes]
    assert times == sorted(times)


def test_redeliveries_are_dropped(changes: list[Change]) -> None:
    identities = [(c.table, c.op, tuple(c.row.values())) for c in changes]
    assert len(identities) == len(set(identities))


def test_deletes_only_follow_an_insert(changes: list[Change]) -> None:
    """Updates may come first (an insert after the extract cutoff); deletes can't."""
    inserted: set[tuple[str, str]] = set()
    for c in changes:
        key = (c.table, c.row[KEYS[c.table]])
        if c.op == "I":
            inserted.add(key)
        elif c.op == "D":
            assert key in inserted, f"delete before insert for {key}"


def test_a_correction_follows_the_value_it_fixes(changes: list[Change]) -> None:
    by_key_time: dict[tuple, list[Change]] = {}
    for c in changes:
        by_key_time.setdefault(
            (c.table, c.row[KEYS[c.table]], c.updated_at), []
        ).append(c)
    pairs = [group for group in by_key_time.values() if len(group) > 1]
    for group in pairs:
        loaded = [c.loaded_at for c in group]
        assert loaded == sorted(loaded)


def test_statements_cast_typed_columns() -> None:
    row = {c: "1" for c in DATA_COLUMNS["order_lines"]}
    [(sql, params)] = statements(Change("order_lines", "I", row, None, None))
    assert sql.startswith("insert into order_lines (order_line_id")
    assert "%s::integer" in sql and "%s::numeric" in sql and "%s::timestamp" in sql
    assert len(params) == len(DATA_COLUMNS["order_lines"])


def test_update_sets_every_non_key_column() -> None:
    row = {c: "x" for c in DATA_COLUMNS["customers"]}
    [(sql, params)] = statements(Change("customers", "U", row, None, None))
    assert sql.endswith("where customer_id = %s")
    assert len(params) == len(DATA_COLUMNS["customers"])


def test_delete_touches_updated_at_first() -> None:
    row = {c: "x" for c in DATA_COLUMNS["products"]}
    (touch, _), (delete, params) = statements(Change("products", "D", row, None, None))
    assert touch.startswith("update products set updated_at")
    assert delete == "delete from products where product_id = %s"
    assert params == ["x"]


def test_schema_kinds_name_real_columns() -> None:
    for table in TABLES:
        assert set(KINDS[table]) <= set(DATA_COLUMNS[table])
