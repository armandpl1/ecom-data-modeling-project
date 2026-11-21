"""Shared description of the CDC source tables (cdc/postgres/init.sql).

The generator's CSV columns (raw_data.config.COLUMNS) minus the CDC metadata
(_op, _loaded_at) are the source tables' columns. KINDS gives each non-text
column's type, so the source simulator can cast values on the way in and the
consumer can turn Debezium's typed values back into the CSV text format.
"""

from __future__ import annotations

from raw_data.config import COLUMNS, ID_COLUMNS

TABLES = ("customers", "products", "orders", "subscriptions")
METADATA = ("_op", "_loaded_at")
DATA_COLUMNS = {t: [c for c in COLUMNS[t] if c not in METADATA] for t in TABLES}
KEYS = {t: ID_COLUMNS[t] for t in TABLES}
TOPIC_PREFIX = "ecom.public"

# Column kinds other than text. Must match cdc/postgres/init.sql.
KINDS: dict[str, dict[str, str]] = {
    "customers": {
        "marketing_opt_in": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
    "products": {
        "unit_price": "numeric",
        "is_active": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
    "orders": {
        "quantity": "integer",
        "unit_price": "numeric",
        "discount_pct": "numeric",
        "order_total": "numeric",
        "order_placed_at": "timestamp",
        "updated_at": "timestamp",
    },
    "subscriptions": {
        "mrr": "numeric",
        "started_at": "date",
        "trial_ends_at": "date",
        "cancelled_at": "timestamp",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
}


def kind(table: str, column: str) -> str:
    return KINDS[table].get(column, "text")


def topic(table: str) -> str:
    return f"{TOPIC_PREFIX}.{table}"
