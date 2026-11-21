# Runbook: a failed daily_pipeline run

**Alert:** `alert_on_failure` posts "Pipeline daily_pipeline failed (run …)"
with the error. If the error lists breached service-level objectives, follow
the runbook named for each one. Otherwise:

## Diagnose

1. Open `make orchestrate` → Runs → the failed run, and find the failed step:
   `source_extracts`, `raw_layer`, `dbt_models` or `service_levels`.
2. `raw_layer` failed: usually a missing or malformed extract. The loader
   writes nothing for a failed file (one transaction per file).
3. `dbt_models` failed: see [dbt-tests.md](dbt-tests.md).

## Fix and verify

Fix the cause, then rerun from the failed step in the Dagster UI (or run
`make pipeline`). The loader skips files already loaded, so a rerun is safe.
The run succeeds and `make check-slos` reports 0 breached.
