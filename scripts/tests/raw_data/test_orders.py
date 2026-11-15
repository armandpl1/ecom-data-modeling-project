"""Tests for raw_data/orders.py."""

from datetime import timedelta
from itertools import pairwise

from raw_data.orders import pick_order_times

# Order lifecycle stages. A status can stay the same (quantity fix, goodwill
# discount) or move forward, and may skip a stage: a row that loads after the
# extract cutoff is missing, just like late data in a real extract.
STAGE = {"placed": 0, "cancelled": 1, "shipped": 1, "delivered": 2, "returned": 3}


def test_order_times_are_sorted_and_within_customer_and_product_life(small_dataset):
    h = small_dataset["history"]
    _, customers = small_dataset["customers"]
    _, products = small_dataset["products"]
    placed = pick_order_times(h, 200, customers, products)
    assert [ts for ts, _, _ in placed] == sorted(ts for ts, _, _ in placed)
    for ts, customer, product in placed:
        assert customer.created <= ts <= (customer.deleted or h.end)
        assert product.is_available(ts)


def test_generated_orders(small_dataset):
    h = small_dataset["history"]
    rows = small_dataset["orders"]
    by_order = {}
    for row in sorted(rows, key=lambda r: (r["updated_at"], r["_loaded_at"])):
        by_order.setdefault(row["order_id"], []).append(row)

    for row in rows:
        expected = round(
            row["quantity"] * row["unit_price"] * (1 - row["discount_pct"]), 2
        )
        assert row["order_total"] == expected

    for history in by_order.values():
        inserts = [r for r in history if r["_op"] == "I"]
        if inserts:
            assert history[0] is inserts[0] and history[0]["status"] == "placed"
        else:
            # The insert loaded after the extract cutoff. Rows load at most
            # 15 days late, so this only happens to the newest orders.
            assert history[0]["order_placed_at"] > h.end - timedelta(days=15)
        for before, after in pairwise(history):
            assert STAGE[after["status"]] >= STAGE[before["status"]]
            assert before["status"] != "cancelled"  # nothing follows a cancel
            if after["status"] == "cancelled":
                assert before["status"] == "placed"
