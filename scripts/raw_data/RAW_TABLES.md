# Raw tables: how they connect

The generator writes five CSVs to `data/raw/`, which load into `raw.*`. Each
file is a **change log**: one row per version of a record, never an in-place
edit. The diagram shows the business keys. Every table also has the three
metadata columns described below.

```mermaid
erDiagram
    customers ||--o{ orders : "places"
    customers ||--o| subscriptions : "has"
    orders ||--|{ order_lines : "contains"
    products ||--o{ order_lines : "appears in"

    customers {
        string customer_id PK "C00001"
        string first_name
        string last_name
        string email
        string phone
        string street_address
        string city
        string state
        string postal_code
        string country
        string customer_segment "consumer, small_business, enterprise"
        boolean marketing_opt_in
        timestamp created_at
    }

    products {
        string product_id PK
        string sku "one SKU per product"
        string product_name
        string category
        decimal unit_price "changes over time"
        string currency
        boolean is_active
        timestamp created_at
    }

    orders {
        string order_id PK "O000001"
        string customer_id FK
        decimal order_total "= sum of line_total"
        string currency
        string payment_method
        string status "placed, shipped, delivered, cancelled, returned"
        timestamp order_placed_at
    }

    order_lines {
        string order_line_id PK "order_id-line_number"
        string order_id FK
        int line_number "1 to 5"
        string product_id FK
        int quantity
        decimal unit_price "product price on the order date"
        decimal discount_pct
        decimal line_total
        string line_status "active, returned"
    }

    subscriptions {
        string subscription_id PK
        string customer_id FK
        string plan_name "basic, plus, premium"
        string billing_period
        decimal mrr
        string status
        timestamp started_at
        timestamp trial_ends_at
        timestamp cancelled_at
        timestamp created_at
    }
```

The raw layer stores every column as text. The types above are what Bronze
casts them to.

## Relationships

| From        | To              | Cardinality        | Join on       | Notes                                    |
| ----------- | --------------- | ------------------ | ------------- | ---------------------------------------- |
| `customers` | `orders`        | one to many        | `customer_id` | some customers never order               |
| `customers` | `subscriptions` | one to zero or one | `customer_id` | about a third of customers subscribe     |
| `orders`    | `order_lines`   | one to many        | `order_id`    | 1 to 5 lines per order                   |
| `products`  | `order_lines`   | one to many        | `product_id`  | a product appears once per order at most |

## Keys in a change log

The `PK` columns above identify a **record**, not a row. One customer has
many rows, one per version, so `customer_id` alone is not unique in raw.

| Key                            | Unique in raw? | What it identifies                          |
| ------------------------------ | -------------- | ------------------------------------------- |
| `<id>`                         | no             | the record (customer, order, ...)           |
| `<id>, updated_at`             | no             | one version, but a correction can share it  |
| `<id>, updated_at, _loaded_at` | no             | one delivery, but a duplicate can repeat it |

To get one row per version, Bronze (`stg_*`) keeps the row with the latest
`_loaded_at` for each `(<id>, updated_at)`. That removes duplicate deliveries
and lets a correction that reuses the bad row's `updated_at` win.

## Metadata columns (on every table)

| Column       | Meaning                                                      |
| ------------ | ------------------------------------------------------------ |
| `updated_at` | when the change happened in the source (event time)          |
| `_op`        | `I` insert, `U` update (full record after it), `D` delete    |
| `_loaded_at` | when the row landed in the warehouse (load time)             |

The loader adds three lineage columns in `raw.*`: `_ingested_at`,
`_batch_id` and `_file_name`.

## Known exceptions

- **Order headers ahead of their lines.** A few orders (4 with the default
  seed) have a header row in `orders` but no rows yet in `order_lines`. The
  order's insert (header and lines together) arrived late, after the
  extract, while a later status change had already loaded. The lines show
  up in a later load. The dbt test `assert_order_headers_have_lines` warns
  on these.
- **Foreign keys always resolve** in the generated data: every
  `orders.customer_id`, `subscriptions.customer_id` and
  `order_lines.product_id` exists in its parent table.
