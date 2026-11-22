"""Shared description of the CDC source tables (cdc/postgres/init.sql).

The generator's CSV columns (raw_data.config.COLUMNS) minus the CDC metadata
(_op, _loaded_at) are the source tables' columns. KINDS gives each non-text
column's type, so the source simulator can cast values on the way in and the
consumer can turn Debezium's typed values back into the CSV text format.

Type changes: every Debezium event carries its schema, and `source_kind`
reduces each field's schema to one kind. A column whose kind is not the one
its KINDS entry implies (or one listed in ALLOWED_SOURCE_KINDS) has changed
type at the source; the consumer stops loading that table until someone
reviews the change (docs/runbooks/cdc-type-change.md).
"""

from __future__ import annotations

from raw_data.config import COLUMNS, ID_COLUMNS

TABLES = ("customers", "products", "orders", "order_lines", "subscriptions")
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
        "order_total": "numeric",
        "order_placed_at": "timestamp",
        "updated_at": "timestamp",
    },
    "order_lines": {
        "line_number": "integer",
        "quantity": "integer",
        "unit_price": "numeric",
        "discount_pct": "numeric",
        "line_total": "numeric",
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


# Debezium (decimal.handling.mode=string) sends numeric as a plain string, so
# numeric and text look the same and both count as "string".
EXPECTED_SOURCE_KIND = {
    "text": "string",
    "numeric": "string",
    "integer": "integer",
    "boolean": "boolean",
    "timestamp": "timestamp",
    "date": "date",
}

# Reviewed type changes: extra source kinds accepted for a column, e.g.
#   "orders": {"order_placed_at": {"timestamptz"}}
# after `timestamp` became `timestamptz` at the source. Each event is decoded
# by its own schema, so events from before and after the change both load.
ALLOWED_SOURCE_KINDS: dict[str, dict[str, set[str]]] = {t: {} for t in TABLES}

_NAMED_KINDS = {
    "io.debezium.time.MicroTimestamp": "timestamp",
    "io.debezium.time.Timestamp": "timestamp_ms",
    "io.debezium.time.NanoTimestamp": "timestamp_ns",
    "io.debezium.time.ZonedTimestamp": "timestamptz",
    "io.debezium.time.Date": "date",
    "io.debezium.data.Json": "json",
}
_PLAIN_KINDS = {
    "string": "string",
    "boolean": "boolean",
    "int8": "integer",
    "int16": "integer",
    "int32": "integer",
    "int64": "integer",
    "float32": "float",
    "float64": "float",
}


def source_kind(field_schema: dict) -> str:
    """One Debezium field schema -> a kind; unknown types keep their own name."""
    name, type_ = field_schema.get("name"), field_schema.get("type")
    if name in _NAMED_KINDS:
        return _NAMED_KINDS[name]
    if name is None and type_ in _PLAIN_KINDS:
        return _PLAIN_KINDS[type_]
    return f"{type_}:{name}" if name else str(type_)


def accepted_kinds(table: str, column: str) -> set[str]:
    """Source kinds that load for a known column without review."""
    expected = EXPECTED_SOURCE_KIND[kind(table, column)]
    return {expected} | ALLOWED_SOURCE_KINDS[table].get(column, set())
