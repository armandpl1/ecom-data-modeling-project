"""Orders and their status lifecycle.

Each order has one product. Orders are only placed while the customer exists
and the product is active, at the product's price at that moment. Possible
changes: a wrong quantity fixed after placing, then placed -> shipped ->
delivered (or cancelled), a goodwill discount, and returns.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from .customers import Customer
from .history import History
from .products import Product


def pick_order_times(
    h: History, n: int, customers: list[Customer], products: list[Product]
) -> list[tuple[datetime, Customer, Product]]:
    """Choose who orders what and when, sorted by time so order_ids increase
    with time. Busy customers (high weight) are picked more often."""
    weights = [c.weight for c in customers]
    placed = []
    while len(placed) < n:
        customer = h.rng.choices(customers, weights)[0]
        ts = h.between(customer.created, customer.deleted or h.end)
        available = [p for p in products if p.is_available(ts)]
        if available:
            placed.append((ts, customer, h.rng.choice(available)))
    placed.sort(key=lambda x: x[0])
    return placed


def generate_orders(
    h: History, n: int, customers: list[Customer], products: list[Product]
) -> list[dict]:
    """Generate n orders and their status history. Returns CSV rows."""
    rng, rows = h.rng, []
    for i, (ts, customer, product) in enumerate(
        pick_order_times(h, n, customers, products), start=1
    ):
        qty = rng.choices([1, 2, 3, 4, 5], [60, 20, 10, 6, 4])[0]
        discount = rng.choices([0.0, 0.05, 0.10, 0.20], [75, 10, 10, 5])[0]
        unit_price = product.price_at(ts)
        state = {
            "order_id": f"O{i:06d}",
            "customer_id": customer.id,
            "product_id": product.id,
            "quantity": qty,
            "unit_price": unit_price,
            "discount_pct": discount,
            "order_total": round(qty * unit_price * (1 - discount), 2),
            "currency": "USD",
            "payment_method": rng.choices(
                ["card", "paypal", "apple_pay", "gift_card"], [65, 20, 12, 3]
            )[0],
            "status": "placed",
            "order_placed_at": ts,
        }

        # Wrong quantity keyed in, then fixed.
        if rng.random() < 0.04:
            bad_qty = qty + rng.randint(1, 9)
            bad = {
                **state,
                "quantity": bad_qty,
                "order_total": round(bad_qty * unit_price * (1 - discount), 2),
            }
            t = h.emit_corrected(rows, bad, state, ts)
        else:
            h.emit(rows, state, ts, "I")
            t = ts

        # Status lifecycle. Changes after the end date don't exist yet.
        if rng.random() < 0.06:
            state["status"] = "cancelled"
            h.emit(rows, state, t + timedelta(hours=rng.uniform(1, 48)))
            continue
        t += timedelta(days=rng.uniform(0.5, 4))
        state["status"] = "shipped"
        h.emit(rows, state, t)
        t += timedelta(days=rng.uniform(2, 7))
        state["status"] = "delivered"
        h.emit(rows, state, t)

        if rng.random() < 0.02:  # goodwill discount applied after delivery
            t += timedelta(days=rng.uniform(1, 10))
            state["discount_pct"] = min(discount + 0.15, 0.5)
            state["order_total"] = round(
                qty * unit_price * (1 - state["discount_pct"]), 2
            )
            h.emit(rows, state, t)
        if rng.random() < 0.05:
            t += timedelta(days=rng.uniform(3, 30))
            state["status"] = "returned"
            h.emit(rows, state, t)
    return rows
