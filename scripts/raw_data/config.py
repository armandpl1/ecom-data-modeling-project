"""Reference data: the fixed values the generators draw from, and the CSV layout."""

# Timestamp format used in every CSV (UTC, no timezone suffix).
TS_FMT = "%Y-%m-%d %H:%M:%S"

# Product catalog building blocks. Names come out like "Eco Lamp - Navy".
CATEGORIES = {
    # category: (product nouns, min price, max price)
    "Electronics": (
        ["Headphones", "Speaker", "Charger", "Keyboard", "Mouse", "Monitor"],
        15,
        400,
    ),
    "Home": (["Lamp", "Blanket", "Mug", "Vase", "Cushion", "Candle"], 8, 150),
    "Outdoors": (["Tent", "Backpack", "Water Bottle", "Hammock", "Headlamp"], 10, 300),
    "Apparel": (["Hoodie", "T-Shirt", "Jacket", "Cap", "Socks"], 5, 180),
    "Books": (["Notebook", "Planner", "Cookbook", "Novel"], 5, 60),
}
ADJECTIVES = [
    "Classic",
    "Pro",
    "Ultra",
    "Eco",
    "Smart",
    "Compact",
    "Deluxe",
    "Everyday",
    "Travel",
]

# Monthly subscription prices. Prices rise once, 60% of the way through the
# date range, and every live subscription is repriced on that day.
PLAN_PRICES_OLD = {"basic": 9.99, "plus": 19.99, "premium": 49.99}
PLAN_PRICES_NEW = {"basic": 11.99, "plus": 24.99, "premium": 59.99}
PLAN_ORDER = ["basic", "plus", "premium"]  # cheapest to most expensive
REPRICE_POINT = 0.6  # share of the date range at which prices rise

CUSTOMER_SEGMENTS = [
    "consumer",
    "small_business",
    "enterprise",
]  # lowest to highest tier

# Column order for each output file.
COLUMNS = {
    "customers": [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "street_address",
        "city",
        "state",
        "postal_code",
        "country",
        "customer_segment",
        "marketing_opt_in",
        "created_at",
        "updated_at",
        "_op",
        "_loaded_at",
    ],
    "products": [
        "product_id",
        "sku",
        "product_name",
        "category",
        "unit_price",
        "currency",
        "is_active",
        "created_at",
        "updated_at",
        "_op",
        "_loaded_at",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "order_total",
        "currency",
        "payment_method",
        "status",
        "order_placed_at",
        "updated_at",
        "_op",
        "_loaded_at",
    ],
    "order_lines": [
        "order_line_id",
        "order_id",
        "line_number",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "line_total",
        "line_status",
        "updated_at",
        "_op",
        "_loaded_at",
    ],
    "subscriptions": [
        "subscription_id",
        "customer_id",
        "plan_name",
        "billing_period",
        "mrr",
        "status",
        "started_at",
        "trial_ends_at",
        "cancelled_at",
        "created_at",
        "updated_at",
        "_op",
        "_loaded_at",
    ],
}

# The key column of each table, used for the summary printed after a run.
ID_COLUMNS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_lines": "order_line_id",
    "subscriptions": "subscription_id",
}
