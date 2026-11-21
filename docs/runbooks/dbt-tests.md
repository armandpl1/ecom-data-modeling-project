# Runbook: dbt_tests

**Alert:** the last `dbt build` had a failing or erroring test or model, or a
warning that isn't on the `allowed_warnings` list in `ops/slos.yml`. The only
allowed warning is `delivered_orders_have_ship_event`, a documented,
self-healing data gap (see README, "Errors vs. warnings").

## Diagnose

1. Which nodes failed?

   ```bash
   python -c "import json; r=json.load(open('target/run_results.json'))['results']; \
   [print(n['status'], n['unique_id']) for n in r if n['status'] in ('fail','error','warn')]"
   ```
2. A failed test stores its failing rows: `select * from test_failures.<test_name>`.
3. An enforced contract error ("This model has an enforced contract that
   failed") means a Gold column's name or type changed: compare the model's
   SQL with `models/marts/_marts.yml`.

## Fix

- Bad data: find the batch (`_batch_id` on Bronze rows) and follow
  [batch-volume.md](batch-volume.md) to quarantine it, or fix the source.
- A code change broke a test or contract: revert it, or update the model and
  its YAML together in one reviewed change.
- A new, expected warning: fix it, or document it and add it to
  `allowed_warnings` with the reason.

## Verify

`dbt build` passes and `make check-slos` shows `dbt_tests` met.
