"""Shared fixtures: a small, fully generated dataset reused by many tests."""

from datetime import date

import pytest
from raw_data.customers import generate_customers
from raw_data.history import History
from raw_data.orders import generate_orders
from raw_data.products import generate_products
from raw_data.subscriptions import generate_subscriptions

START, END = date(2024, 1, 1), date(2024, 12, 31)


def make_history(seed=1, late_rate=0.0, dup_rate=0.0):
    return History(seed, START, END, late_rate, dup_rate)


@pytest.fixture
def history():
    """A fresh History with no late arrivals or duplicates."""
    return make_history()


@pytest.fixture(scope="session")
def small_dataset():
    """Products, customers, orders and subscriptions generated together, in the
    same order as the real script (each step depends on the random draws of
    the previous ones)."""
    h = make_history(seed=3, late_rate=0.05)
    product_rows, products = generate_products(h, 40)
    customer_rows, customers = generate_customers(h, 300)
    order_rows = generate_orders(h, 1500, customers, products)
    subscription_rows = generate_subscriptions(h, 0.4, customers)
    return {
        "history": h,
        "products": (product_rows, products),
        "customers": (customer_rows, customers),
        "orders": order_rows,
        "subscriptions": subscription_rows,
    }
