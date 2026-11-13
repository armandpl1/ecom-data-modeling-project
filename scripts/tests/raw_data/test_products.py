"""Tests for raw_data/products.py."""

from datetime import datetime

from raw_data.config import CATEGORIES
from raw_data.products import Product, price_point


def test_price_point_ends_in_99():
    assert price_point(41.37) == 41.99
    assert price_point(0.2) == 1.99


def test_price_at_uses_the_price_in_effect():
    p = Product(
        id="P1",
        created=datetime(2024, 1, 1),
        prices=[(datetime(2024, 1, 1), 10.0), (datetime(2024, 6, 1), 12.0)],
    )
    assert p.price_at(datetime(2024, 3, 1)) == 10.0
    assert p.price_at(datetime(2024, 6, 1)) == 12.0


def test_is_available_between_creation_and_discontinuation():
    p = Product(id="P1", created=datetime(2024, 2, 1), inactive_at=datetime(2024, 9, 1))
    assert not p.is_available(datetime(2024, 1, 15))
    assert p.is_available(datetime(2024, 5, 1))
    assert not p.is_available(datetime(2024, 9, 2))


def test_generated_products(small_dataset):
    rows, products = small_dataset["products"]
    assert [p.id for p in products] == [f"P{i:04d}" for i in range(1, 41)]
    assert {r["category"] for r in rows} <= set(CATEGORIES)
    for product in products:
        history = [r for r in rows if r["product_id"] == product.id]
        assert history[0]["_op"] == "I"
        # A deleted product was discontinued first
        if any(r["_op"] == "D" for r in history):
            assert product.inactive_at is not None
        # Prices in the lookup match prices written to the CSV rows
        assert {r["unit_price"] for r in history} <= {p for _, p in product.prices}
