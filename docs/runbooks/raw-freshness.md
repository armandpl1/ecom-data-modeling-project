# Runbook: raw_freshness

**Alert:** a raw table's newest load is older than `max_age_hours` (36 h)
compared with the reference time.

## Likely causes

- The nightly extract for that table didn't arrive (the source system or its
  export failed).
- The extract arrived but was identical to one already loaded, so the loader
  skipped it (same SHA-256). A source that stops changing looks like this.
- The pipeline didn't run (schedule off, Dagster down).

## Diagnose

1. When did each table last load?

   ```sql
   select table_name, max(loaded_at) as last_load, count(*) as batches
   from raw._load_log group by 1 order by 2;
   ```
2. Did the pipeline run? Open `make orchestrate` → Runs, or check the
   `daily_pipeline_schedule` is on.
3. Look for the file: `ls -l data/raw/`. Was it delivered, and when?

## Fix

- Missing file: get the extract re-sent, then `make load` and `dbt build`.
- Pipeline not running: turn the schedule on, or run `make pipeline`.
- Skipped as a duplicate: confirm with the source that nothing changed. If so,
  the data is current and the alert is expected; note it in the incident log.

## Verify

`make check-slos` shows `raw_freshness` met for every table.
