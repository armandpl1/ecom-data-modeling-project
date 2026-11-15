"""Tests for raw_data/subscriptions.py."""

from datetime import datetime

from raw_data.config import PLAN_PRICES_NEW, PLAN_PRICES_OLD
from raw_data.subscriptions import mrr

REPRICE_AT = datetime(2024, 8, 1)


def test_mrr_uses_old_then_new_prices():
    assert mrr("plus", "monthly", datetime(2024, 1, 1), REPRICE_AT) == 19.99
    assert mrr("plus", "monthly", datetime(2024, 9, 1), REPRICE_AT) == 24.99


def test_annual_plans_cost_ten_months_per_year():
    assert mrr("basic", "annual", datetime(2024, 1, 1), REPRICE_AT) == round(
        9.99 * 10 / 12, 2
    )


def test_generated_subscriptions(small_dataset):
    rows = small_dataset["subscriptions"]
    _, customers = small_dataset["customers"]
    statuses = {"trialing", "active", "past_due", "paused", "cancelled"}
    possible_mrr = {
        mrr(plan, period, ts, REPRICE_AT)
        for prices in (PLAN_PRICES_OLD, PLAN_PRICES_NEW)
        for plan in prices
        for period in ("monthly", "annual")
        for ts in (datetime(2024, 1, 1), datetime(2024, 9, 1))
    }

    owner = {}
    for row in rows:
        assert row["status"] in statuses
        assert row["mrr"] in possible_mrr
        assert (row["cancelled_at"] is not None) == (row["status"] == "cancelled")
        # At most one subscription per customer
        assert owner.setdefault(row["customer_id"], row["subscription_id"]) == (
            row["subscription_id"]
        )
    assert set(owner) <= {c.id for c in customers}
