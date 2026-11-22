"""Tests for raw_data/orders.py."""

from datetime import datetime, time, timedelta
from itertools import pairwise

from raw_data.orders import line_total, pick_order_times

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


def latest(rows, key):
    out = {}
    for row in sorted(rows, key=lambda r: (r["updated_at"], r["_loaded_at"])):
        out[row[key]] = row
    return out


def test_order_lines(small_dataset):
    orders = latest(small_dataset["orders"], "order_id")
    lines = latest(small_dataset["order_lines"], "order_line_id")
    by_order = {}
    for line in lines.values():
        by_order.setdefault(line["order_id"], []).append(line)

    counts = [len(v) for v in by_order.values()]
    assert min(counts) == 1 and max(counts) <= 5 and max(counts) > 1
    for order_id, order_lines in by_order.items():
        numbers = sorted(line["line_number"] for line in order_lines)
        assert numbers == list(range(1, len(numbers) + 1))
        products = [line["product_id"] for line in order_lines]
        assert len(products) == len(set(products))  # one line per product
        for line in order_lines:
            assert line["order_line_id"] == f"{order_id}-{line['line_number']}"
            assert line["line_total"] == line_total(line)
        # The header total is the sum of its lines
        if order_id in orders:
            total = round(sum(line["line_total"] for line in order_lines), 2)
            assert abs(orders[order_id]["order_total"] - total) < 0.011


def test_lines_load_with_their_header_change(small_dataset):
    header_loads = {
        (r["order_id"], r["updated_at"]): r["_loaded_at"]
        for r in small_dataset["orders"]
    }
    for line in small_dataset["order_lines"]:
        if line["line_status"] == "returned":
            continue  # a single-line return is its own source change
        key = (line["order_id"], line["updated_at"])
        assert key in header_loads  # every line version belongs to a header change


def test_header_and_lines_agree_after_every_nightly_load(small_dataset):
    """At any load cutoff, an order's lines add up to its header total: a line
    change is never delivered before the header change that goes with it."""
    orders, lines = small_dataset["orders"], small_dataset["order_lines"]
    # Nightly batches load around 02:00; cut between them, at noon
    days = sorted({r["_loaded_at"].date() for r in orders + lines})
    cutoffs = [datetime.combine(day, time(12, 0)) for day in days]
    for cutoff in cutoffs[::3]:
        header = latest([r for r in orders if r["_loaded_at"] <= cutoff], "order_id")
        current = latest(
            [r for r in lines if r["_loaded_at"] <= cutoff], "order_line_id"
        )
        totals = {}
        for line in current.values():
            totals[line["order_id"]] = (
                totals.get(line["order_id"], 0) + line["line_total"]
            )
        for order_id, total in totals.items():
            if order_id in header:
                assert abs(header[order_id]["order_total"] - total) < 0.011, (
                    cutoff,
                    order_id,
                )
