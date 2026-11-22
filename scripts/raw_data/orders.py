"""Orders, their lines, and the order status lifecycle.

An order (orders.csv) has a customer, a payment method, a status and a total.
Its lines (order_lines.csv) each have a product, a quantity, the product's
price at that moment and a line discount. order_total is always the sum of
the line totals. Orders are only placed while the customer exists, and lines
only for products that are active.

Possible changes: a wrong quantity on the first line fixed after placing,
then placed -> shipped -> delivered (or cancelled), a goodwill discount on
every line, returns of the whole order, and returns of single lines.

Determinism: the first line and everything about the order header draw from
the main History in the same order as when every order had one product, so
products, customers and subscriptions stay byte-identical. Extra lines and
single-line returns draw from a separate substream.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from .customers import Customer
from .history import History
from .products import Product

LINE_SUBSTREAM = 1_000_003  # seed offset of the order-lines random source
LINE_COUNTS = ([1, 2, 3, 4, 5], [55, 25, 12, 5, 3])
QUANTITIES = ([1, 2, 3, 4, 5], [60, 20, 10, 6, 4])
DISCOUNTS = ([0.0, 0.05, 0.10, 0.20], [75, 10, 10, 5])
LINE_RETURN_RATE = 0.04  # each line of a delivered multi-line order


def pick_order_times(
    h: History, n: int, customers: list[Customer], products: list[Product]
) -> list[tuple[datetime, Customer, Product]]:
    """Choose who orders what and when, sorted by time so order_ids increase
    with time. Busy customers (high weight) are picked more often. The
    product is the order's first line."""
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


def line_total(line: dict) -> float:
    return round(line["quantity"] * line["unit_price"] * (1 - line["discount_pct"]), 2)


def order_total(lines: list[dict]) -> float:
    return round(sum(line_total(line) for line in lines), 2)


def make_line(order_id: str, number: int, product: Product, ts, qty, discount) -> dict:
    line = {
        "order_line_id": f"{order_id}-{number}",
        "order_id": order_id,
        "line_number": number,
        "product_id": product.id,
        "quantity": qty,
        "unit_price": product.price_at(ts),
        "discount_pct": discount,
        "line_status": "active",
    }
    line["line_total"] = line_total(line)
    return line


def extra_lines(
    lines_h: History,
    order_id: str,
    ts: datetime,
    first: Product,
    products: list[Product],
) -> list[dict]:
    """Lines 2..n of an order: other products active at ts, from the substream."""
    rng = lines_h.rng
    count = rng.choices(*LINE_COUNTS)[0]
    available = [p for p in products if p.is_available(ts) and p.id != first.id]
    chosen = rng.sample(available, min(count - 1, len(available)))
    return [
        make_line(
            order_id,
            number,
            product,
            ts,
            rng.choices(*QUANTITIES)[0],
            rng.choices(*DISCOUNTS)[0],
        )
        for number, product in enumerate(chosen, start=2)
    ]


def emit_lines(lines_out: list, lines: list[dict], header_row: dict | None, op: str):
    """Write line versions in the same load as the header change they belong to
    (one source transaction). Nothing is written if that header row wasn't."""
    if header_row is None:
        return
    for line in lines:
        lines_out.append(
            {
                **line,
                "updated_at": header_row["updated_at"],
                "_op": op,
                "_loaded_at": header_row["_loaded_at"],
            }
        )


def emitted(rows: list, before: int, total: float | None = None) -> dict | None:
    """The header row appended since len(rows) was `before` (matching total)."""
    for row in rows[before:]:
        if total is None or row["order_total"] == total:
            return row
    return None


def deliver_line_fix_with_header(
    rows: list, header_from: int, line_rows: list, fix_from: int, fix_to: int
) -> None:
    """A line correction is delivered no later than the first later header
    change that already shows the corrected total (one source transaction).
    Only _loaded_at moves, so no random draw changes."""
    later = [r["_loaded_at"] for r in rows[header_from:]]
    if not later:
        return

    # A fix that reuses the bad row's updated_at only wins because it loads
    # later, so it must stay after the bad version it corrects
    def floor_for(fix: dict):
        same_time = [
            r["_loaded_at"]
            for r in line_rows[:fix_from]
            if r["order_line_id"] == fix["order_line_id"]
            and r["updated_at"] == fix["updated_at"]
        ]
        return max(same_time) + timedelta(seconds=1) if same_time else None

    for line in line_rows[fix_from:fix_to]:
        moved = min(line["_loaded_at"], min(later))
        floor = floor_for(line)
        line["_loaded_at"] = max(moved, floor) if floor else moved


def generate_orders(
    h: History, n: int, customers: list[Customer], products: list[Product]
) -> tuple[list[dict], list[dict]]:
    """Generate n orders with their lines and history. Returns (order rows,
    order line rows), both as CSV rows."""
    rng, rows, line_rows = h.rng, [], []
    lines_h = h.substream(LINE_SUBSTREAM)
    for i, (ts, customer, product) in enumerate(
        pick_order_times(h, n, customers, products), start=1
    ):
        order_id = f"O{i:06d}"
        qty = rng.choices(*QUANTITIES)[0]
        discount = rng.choices(*DISCOUNTS)[0]
        lines = [make_line(order_id, 1, product, ts, qty, discount)]
        lines += extra_lines(lines_h, order_id, ts, product, products)
        state = {
            "order_id": order_id,
            "customer_id": customer.id,
            "order_total": order_total(lines),
            "currency": "USD",
            "payment_method": rng.choices(
                ["card", "paypal", "apple_pay", "gift_card"], [65, 20, 12, 3]
            )[0],
            "status": "placed",
            "order_placed_at": ts,
        }

        # Wrong quantity keyed in on the first line, then fixed.
        if rng.random() < 0.04:
            bad_qty = qty + rng.randint(1, 9)
            bad_first = {**lines[0], "quantity": bad_qty}
            bad_first["line_total"] = line_total(bad_first)
            bad_lines = [bad_first, *lines[1:]]
            bad = {**state, "order_total": order_total(bad_lines)}
            before = len(rows)
            t = h.emit_corrected(rows, bad, state, ts)
            emit_lines(
                line_rows, bad_lines, emitted(rows, before, bad["order_total"]), "I"
            )
            fixed = emitted(rows, before, state["order_total"])
            bad_written = emitted(rows, before, bad["order_total"]) is not None
            fix_from = len(line_rows)
            emit_lines(line_rows, lines[:1] if bad_written else lines, fixed, "U")
            fix_span = (len(rows), fix_from, len(line_rows))
        else:
            before = len(rows)
            h.emit(rows, state, ts, "I")
            emit_lines(line_rows, lines, emitted(rows, before), "I")
            t = ts
            fix_span = None

        # Status lifecycle. Changes after the end date don't exist yet.
        if rng.random() < 0.06:
            state["status"] = "cancelled"
            h.emit(rows, state, t + timedelta(hours=rng.uniform(1, 48)))
            if fix_span:
                deliver_line_fix_with_header(
                    rows, fix_span[0], line_rows, *fix_span[1:]
                )
            continue
        t += timedelta(days=rng.uniform(0.5, 4))
        state["status"] = "shipped"
        h.emit(rows, state, t)
        t += timedelta(days=rng.uniform(2, 7))
        state["status"] = "delivered"
        delivered = h.emit(rows, state, t) is not None
        delivered_at = t

        if rng.random() < 0.02:  # goodwill discount on every line after delivery
            t += timedelta(days=rng.uniform(1, 10))
            for line in lines:
                line["discount_pct"] = min(line["discount_pct"] + 0.15, 0.5)
                line["line_total"] = line_total(line)
            state["order_total"] = order_total(lines)
            before = len(rows)
            h.emit(rows, state, t)
            emit_lines(line_rows, lines, emitted(rows, before), "U")
        if rng.random() < 0.05:  # the whole order comes back
            t += timedelta(days=rng.uniform(3, 30))
            state["status"] = "returned"
            h.emit(rows, state, t)
        elif delivered and len(lines) > 1:  # sometimes a single line comes back
            for line in lines:
                if lines_h.rng.random() < LINE_RETURN_RATE:
                    returned_at = delivered_at + timedelta(
                        days=lines_h.rng.uniform(3, 30)
                    )
                    line["line_status"] = "returned"
                    if returned_at > lines_h.end:
                        continue
                    # A line's changes are delivered in order: never before
                    # the line's earlier versions (e.g. a late goodwill fix)
                    earlier = [
                        r["_loaded_at"]
                        for r in line_rows
                        if r["order_line_id"] == line["order_line_id"]
                    ]
                    loaded = lines_h.load_time(returned_at)
                    if earlier:
                        loaded = max(loaded, max(earlier) + timedelta(seconds=1))
                    lines_h.emit(line_rows, dict(line), returned_at, loaded=loaded)
        if fix_span:
            deliver_line_fix_with_header(rows, fix_span[0], line_rows, *fix_span[1:])
    # Lines are re-delivered from the substream too, so the main History's
    # duplicate draws (for every other table) don't move
    return rows, lines_h.add_duplicates(line_rows)
