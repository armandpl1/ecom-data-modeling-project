# ecom-data-modeling-project

A data modeling project for a fictional e-commerce business, built with dbt and DuckDB.

The raw data is synthetic. A Python script generates it with a realistic change history, including updates, corrections, late-arriving rows and duplicates, so the dbt models have to handle what real source extracts look like.

## Setup

Requires Python 3.9+.

```bash
conda create -n dbt python=3.12   # or any virtualenv
conda activate dbt
pip install -r requirements.txt
```

## Generating the raw data

```bash
python scripts/generate_raw_data.py
```

This writes four CSVs to `data/raw/`. The output is reproducible: the same `--seed` and options always produce the same files, so `data/` is not committed.

| Option | Default | Meaning |
|---|---|---|
| `--customers` | 1000 | Number of customers |
| `--products` | 80 | Number of products |
| `--orders` | 6000 | Number of orders |
| `--subscription-share` | 0.35 | Share of customers who start a subscription |
| `--start` / `--end` | 2024-01-01 / 2026-10-31 | Date range of the history |
| `--late-rate` | 0.03 | Share of rows that load 1–14 days late |
| `--dup-rate` | 0.01 | Share of rows delivered twice |
| `--seed` | 42 | Random seed |
| `--out-dir` | `data/raw` | Output folder |

Example for a larger dataset:

```bash
python scripts/generate_raw_data.py --customers 5000 --orders 30000 --seed 7
```

## The raw data

Each file is an append-only extract, as if copied nightly from the source system. Every change to a record is a **new row**; nothing is edited in place. To get the current state of a record you have to pick its latest version.

Every file has three metadata columns:

| Column | Meaning |
|---|---|
| `updated_at` | When the change happened in the source system |
| `_op` | `I` insert, `U` update, `D` delete |
| `_loaded_at` | When the row landed in the warehouse (a nightly batch around 02:00) |

### Files

| File | Key | One row per | Main columns |
|---|---|---|---|
| `customers.csv` | `customer_id` | customer version | name, email, phone, address, segment, marketing opt-in |
| `products.csv` | `product_id` | product version | SKU, name, category, unit price, active flag |
| `orders.csv` | `order_id` | order status change | customer, product, quantity, price, discount, total, status |
| `subscriptions.csv` | `subscription_id` | subscription version | customer, plan, billing period, MRR, status, start/cancel dates |

Orders contain one product each. Orders and subscriptions reference customers and products by ID.

### What's in the history

- **Updates:**
  - Customers move, change email, phone or last name, toggle marketing opt-in, and move up a segment.
  - Product prices change, and some products are discontinued.
  - Orders go `placed` → `shipped` → `delivered`, or are `cancelled`. Some are later `returned` or get a goodwill discount.
  - Subscriptions go `trialing` → `active`, upgrade or downgrade, become `past_due` or `paused`, and churn to `cancelled`. All live subscriptions are repriced once when plan prices go up.
- **Corrections:** a wrong value is written, then fixed. Examples are a product price with a shifted decimal point, typos in names, an uppercase email with spaces, a wrong state, an order with the wrong quantity, and a backdated subscription start date. About 30% of fixes have the **same `updated_at`** as the bad row, so only `_loaded_at` tells you which one is newer.
- **Late arrivals:** about 3% of rows load 1–14 days after they happened, so rows can arrive out of order.
- **Duplicates:** about 1% of rows are delivered twice, either identically or in a later batch.
- **Deletes:** about 2% of customers are hard-deleted (`_op = 'D'`), and their live subscriptions are cancelled first. A few discontinued products are deleted too.
- **Unfixed messy values:** some emails keep odd capitalization, phone formats vary, and about 5% of phones are missing.
- **Cutoff:** the data is an extract taken on the morning after `--end`. Anything not yet loaded by then is missing, so recent orders can still be open.

### Picking the latest version

To deduplicate, order each key's rows by `updated_at` and then `_loaded_at`, both descending. For example:

```sql
select *
from raw_customers
qualify row_number() over (
    partition by customer_id
    order by updated_at desc, _loaded_at desc
) = 1
```

Drop the result if that latest row has `_op = 'D'`.

## Project layout

```
scripts/generate_raw_data.py   raw data generator
data/raw/                      generated CSVs (not committed)
requirements.txt               Python dependencies
```
