# Runbook: warehouse_caught_up

**Alert:** Bronze (`bronze.stg_<table>`) hasn't processed the newest raw load
of a table: its newest `ingested_at` is older than the raw table's newest
`_ingested_at`, or Bronze doesn't exist.

## Likely causes

- The load ran but `dbt build` didn't (it failed, or was never started).
- `dbt build` ran with a selection that skipped the table.
- An incremental model's watermark is ahead of the data (for example after a
  manual edit of the raw layer).

## Diagnose

```sql
select 'raw' as layer, max(_ingested_at) from raw.orders
union all
select 'bronze', max(ingested_at) from bronze.stg_orders;
```

Then check the last dbt run: `target/run_results.json`, or the Dagster run log.

## Fix

- Run `dbt build` (or `make pipeline`).
- If Bronze still doesn't catch up, rebuild the table and its downstream:
  `dbt build --full-refresh -s stg_orders+`.

## Verify

`make check-slos` shows `warehouse_caught_up` met (0.0 h behind raw).
