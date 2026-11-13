"""Product catalog and price history.

Possible changes per product: a price or name typo fixed after creation, 0-3
price changes, and discontinuation (sometimes followed by a hard delete).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .config import ADJECTIVES, CATEGORIES
from .history import History


@dataclass
class Product:
    """What orders need to know about a product."""

    id: str
    created: datetime
    inactive_at: datetime | None = None
    # (from when, price) pairs, so orders are charged the price of their time
    prices: list[tuple[datetime, float]] = field(default_factory=list)

    def is_available(self, ts: datetime) -> bool:
        """Exists and is still active at ts."""
        return self.created <= ts and (
            self.inactive_at is None or self.inactive_at > ts
        )

    def price_at(self, ts: datetime) -> float:
        """The unit price as of ts."""
        price = self.prices[0][1]
        for changed_at, p in sorted(self.prices):
            if changed_at <= ts:
                price = p
        return price


def price_point(x: float) -> float:
    """Round to a retail-style price ending in .99, e.g. 41.37 -> 41.99."""
    return max(1, int(x)) + 0.99


def generate_products(h: History, n: int) -> tuple[list[dict], list[Product]]:
    """Generate n products. Returns (CSV rows, product lookups for orders)."""
    rng, rows, products = h.rng, [], []
    span = h.end - h.start
    for i in range(1, n + 1):
        category = rng.choice(list(CATEGORIES))
        nouns, lo, hi = CATEGORIES[category]
        price = price_point(rng.uniform(lo, hi))
        if rng.random() < 0.8:  # most of the catalog exists from the start
            created = h.start + span * rng.uniform(0, 0.05)
        else:
            created = h.between(h.start, h.start + span * 0.8)

        state = {
            "product_id": f"P{i:04d}",
            "sku": f"{category[:3].upper()}-{rng.randint(10000, 99999)}",
            "product_name": (
                f"{rng.choice(ADJECTIVES)} {rng.choice(nouns)}"
                f" - {h.fake.color_name()}"
            ),
            "category": category,
            "unit_price": price,
            "currency": "USD",
            "is_active": True,
            "created_at": created,
        }
        product = Product(
            id=state["product_id"], created=created, prices=[(created, price)]
        )

        r = rng.random()
        if r < 0.10:  # price typo: shifted decimal point
            bad_price = (
                round(price * 10, 2) if rng.random() < 0.5 else round(price / 10, 2)
            )
            fixed_at = h.emit_corrected(
                rows, {**state, "unit_price": bad_price}, state, created
            )
            # Orders placed before the fix really were charged the bad price.
            product.prices = [(created, bad_price), (fixed_at, price)]
        elif r < 0.18:  # name typo
            bad = {**state, "product_name": h.typo(state["product_name"])}
            h.emit_corrected(rows, bad, state, created)
        else:
            h.emit(rows, state, created, "I")

        # Price changes over time.
        changes = sorted(
            h.between(created, h.end) for _ in range(rng.choice([0, 0, 1, 1, 2, 3]))
        )
        for ts in changes:
            state["unit_price"] = price_point(
                state["unit_price"] * rng.uniform(0.85, 1.25)
            )
            h.emit(rows, state, ts)
            product.prices.append((ts, state["unit_price"]))

        # Discontinued, and sometimes hard-deleted afterwards.
        if rng.random() < 0.10:
            last = changes[-1] if changes else created
            ts = h.between(last, h.end)
            state["is_active"] = False
            h.emit(rows, state, ts)
            product.inactive_at = ts
            if rng.random() < 0.3:
                h.emit(rows, state, h.between(ts, h.end), "D")

        products.append(product)
    return rows, products
