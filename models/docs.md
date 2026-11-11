{% docs __overview__ %}
# E-commerce data model

A dbt + DuckDB project for a fictional e-commerce business that sells
products one-off and through subscriptions.

## Layers

| Layer | Folder / schema | Purpose |
|---|---|---|
| **Bronze** | `staging` / `bronze` | Typed, cleaned, deduplicated copies of the raw extracts. One row per record change. |
| **Silver** | `intermediate` / `silver` | Business logic: SCD Type 2 history, order lifecycle, point-in-time joins. |
| **Gold** | `marts` / `gold` | Kimball star schema for analysis. |

## Gold layer

| Model | Type | Grain |
|---|---|---|
| `dim_customer` | dimension, SCD2 | customer version |
| `dim_product` | dimension, SCD2 | product version |
| `dim_date` | dimension | day |
| `fact_orders` | transaction fact | order line |
| `fact_subscriptions` | transaction fact | MRR movement or churn event |
| `fact_subscription_mrr_monthly` | periodic snapshot | subscription × month |

Open the lineage graph with the button at the bottom right to see how the
four raw CSVs flow through each layer into these models.

## Testing

Every model is tested with built-in tests (unique, not_null,
accepted_values, relationships) and the project's generic tests:

- `not_negative`: amounts and quantities never drop below zero
- `scd2_valid_ranges`: SCD2 validity ranges never overlap, have no gaps,
  and have one current version per entity
- `expression_is_true`: row-level business rules across columns
- `unique_combination_of_columns`: composite keys
{% enddocs %}


{% docs scd2_sk %}
Surrogate key of this version: a hash of the natural key and
`valid_from_timestamp`. Facts join on this key to get the version that was
valid at the time of the event.
{% enddocs %}

{% docs valid_from_timestamp %}
When this version became true in the source system. Validity ranges are
half-open: `[valid_from_timestamp, valid_to_timestamp)`.
{% enddocs %}

{% docs valid_to_timestamp %}
When the next version replaced this one. Null for the latest version.
For a deleted record, the last version ends at the delete.
{% enddocs %}

{% docs is_current %}
True for the latest version of a record that still exists. False on every
version of a deleted record.
{% enddocs %}

{% docs version_number %}
1 for the first version of a record, then 2, 3 and so on.
{% enddocs %}

{% docs deleted_at %}
When the record was hard-deleted in the source (`_op = 'D'`), repeated on
every version. Null if it still exists.
{% enddocs %}

{% docs is_deleted_tombstone %}
True for the tombstone row that records a hard delete. Gold models filter
these rows out.
{% enddocs %}

{% docs loaded_at %}
When the row landed in the warehouse (nightly batch). Breaks ties between
rows with the same `updated_at`: the row loaded later wins.
{% enddocs %}

{% docs paying_mrr %}
Monthly recurring revenue that counts as paying: the subscription's `mrr`
while its status is `active` or `past_due`, and 0 for `trialing`, `paused`
and `cancelled`. Annual plans are spread evenly over 12 months.
{% enddocs %}

{% docs date_key %}
Date as an integer in `yyyymmdd` form, the join key to `dim_date`.
{% enddocs %}
