# Runbook: CDC source type change

**Alert:** the CDC consumer (`make cdc-load`, or the `cdc_changes` Dagster
asset) stops loading a table and exits 3 with a line like:

```
BLOCKED orders.order_placed_at: expected timestamp, got timestamptz
```

A column in the source Postgres changed type. Every Debezium event carries its
schema, and the consumer compares each known column's type with
`scripts/cdc_schema.py`. On a mismatch it loads nothing for that table, does
not move its offsets, and records the change in `raw._cdc_type_changes`. The
other tables keep loading. Nothing is lost: the held events load once the
change is accepted.

The consumer stops instead of guessing because a type change can change what
the values mean. `timestamp` to `timestamptz` moves values to UTC; `integer`
to `numeric` can add decimals; `boolean` to `text` can bring new values that
Bronze's casts reject.

## Diagnose

1. See what changed and when:

   ```sql
   select * from raw._cdc_type_changes order by detected_at;
   ```
2. Ask the source team why, and whether old values were converted. In
   Postgres: `\d orders` shows the new type; `select ... from orders` shows
   what the values look like now.
3. Decide what the new values mean for Bronze. Raw stores text, so the
   question is whether the text after the change still parses the same way
   in `models/staging/stg_<table>.sql`.

## Fix

1. Accept the new type for that column in `scripts/cdc_schema.py`:

   ```python
   ALLOWED_SOURCE_KINDS = {
       ...,
       "orders": {"order_placed_at": {"timestamptz"}},
   }
   ```

   Each event is decoded by its own schema, so events from before and after
   the change both load (`timestamptz` is written as UTC text in the same
   format as `timestamp`).
2. Once the source has fully switched, also update `KINDS` and
   `cdc/postgres/init.sql` to the new type, and remove the allowance.
3. If Bronze needs a different cast, change the staging model in the same
   pull request.
4. Rerun: `make cdc-load`, then `dbt build`. The consumer loads the held
   events exactly once.

## Check

- `make cdc-load` reports rows for the table again and exits 0.
- `select count(*) from raw.orders where _batch_id like 'orders-cdc-%'`
  grows.
- `dbt build` passes.

`make cdc-e2e` runs this whole scenario on a fresh stack: it changes
`orders.order_placed_at` to `timestamptz`, checks that the table is blocked
with its offsets unchanged, then allows the change and checks that the held
event loads with the same timestamp text.
