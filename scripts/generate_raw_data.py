#!/usr/bin/env python3
"""Generate raw e-commerce source data with change history, for dbt practice.

Writes four CSVs that look like append-only extracts from a source system
(CDC-style): every change to a record is a new row, never an in-place edit.

    customers.csv      one row per customer version
    products.csv       one row per product version
    orders.csv         one row per order status change
    subscriptions.csv  one row per subscription version

Metadata columns on every file:
    updated_at   when the change happened in the source system
    _op          I = insert, U = update, D = delete
    _loaded_at   when the row landed in the warehouse (nightly batch)

What the history contains:
    Updates       customers move, change email/phone/name/segment, opt out;
                  product prices change and products get discontinued;
                  orders move placed -> shipped -> delivered (or cancelled,
                  returned); subscriptions trial, upgrade, pause, churn, and
                  get repriced.
    Corrections   a bad value is written, then fixed a few hours later
                  (price typos, name typos, messy emails, wrong quantities).
                  Some fixes keep the same updated_at as the bad row and only
                  arrive in a later load, so _loaded_at is needed to break ties.
    Late arrivals some rows load days after they happened, out of order.
    Duplicates    some rows are re-delivered, identically or in a later batch.
    Deletes       a few customers (GDPR) and products are hard-deleted.

The logic lives in the raw_data package next to this script; see
raw_data/__init__.py for a map of its modules.

Usage:
    python scripts/generate_raw_data.py
    python scripts/generate_raw_data.py --customers 2000 --orders 15000 --seed 7
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from raw_data.config import COLUMNS, ID_COLUMNS
from raw_data.customers import generate_customers
from raw_data.history import History
from raw_data.orders import generate_orders
from raw_data.products import generate_products
from raw_data.subscriptions import generate_subscriptions
from raw_data.writer import write_csv

DEFAULT_OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--products", type=int, default=80)
    parser.add_argument("--orders", type=int, default=6000)
    parser.add_argument(
        "--subscription-share",
        type=float,
        default=0.35,
        help="share of customers who start a subscription",
    )
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 10, 31))
    parser.add_argument(
        "--late-rate",
        type=float,
        default=0.03,
        help="share of rows that load days late",
    )
    parser.add_argument(
        "--dup-rate", type=float, default=0.01, help="share of rows re-delivered"
    )
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args(argv)


def generate(args: argparse.Namespace) -> dict[str, list[dict]]:
    """Build every table's rows, including duplicate deliveries.

    Order matters: orders and subscriptions refer to customers and products,
    and every step draws from the same seeded random sources.
    """
    h = History(args.seed, args.start, args.end, args.late_rate, args.dup_rate)
    product_rows, products = generate_products(h, args.products)
    customer_rows, customers = generate_customers(h, args.customers)
    tables = {
        "products": product_rows,
        "customers": customer_rows,
        "orders": generate_orders(h, args.orders, customers, products),
        "subscriptions": generate_subscriptions(h, args.subscription_share, customers),
    }
    return {name: h.add_duplicates(rows) for name, rows in tables.items()}


def main(argv: list[str] | None = None) -> None:
    """Parse arguments, generate all tables, and write the CSVs."""
    args = parse_args(argv)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    for name, rows in generate(args).items():
        path = args.out_dir / f"{name}.csv"
        write_csv(path, COLUMNS[name], rows)
        ids = len({r[ID_COLUMNS[name]] for r in rows})
        print(f"{path}: {len(rows):>6} rows, {ids:>5} distinct {ID_COLUMNS[name]}s")


if __name__ == "__main__":
    main()
