"""Tests for raw_data/config.py."""

from raw_data.config import (
    COLUMNS,
    CUSTOMER_SEGMENTS,
    ID_COLUMNS,
    PLAN_ORDER,
    PLAN_PRICES_NEW,
    PLAN_PRICES_OLD,
)


def test_every_table_has_a_key_and_cdc_metadata_columns():
    for table, columns in COLUMNS.items():
        assert columns[0] == ID_COLUMNS[table]
        assert columns[-3:] == ["updated_at", "_op", "_loaded_at"]
        assert len(columns) == len(set(columns)), f"duplicate column in {table}"


def test_plans_are_ordered_by_price_and_prices_only_go_up():
    for prices in (PLAN_PRICES_OLD, PLAN_PRICES_NEW):
        assert [prices[p] for p in PLAN_ORDER] == sorted(prices.values())
    assert all(PLAN_PRICES_NEW[p] > PLAN_PRICES_OLD[p] for p in PLAN_ORDER)


def test_segments_are_unique():
    assert len(CUSTOMER_SEGMENTS) == len(set(CUSTOMER_SEGMENTS))
