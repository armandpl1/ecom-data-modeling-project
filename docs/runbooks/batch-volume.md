# Runbook: batch_volume

**Alert:** a table's latest batch has fewer than `min_ratio` (50%) of the
average rows of its previous batches, or is empty. dbt tests usually still
pass: a short file has valid rows, there are just too few of them. See the
[postmortem of 2026-11-21](../postmortems/2026-11-21-truncated-orders-extract.md).

## Likely causes

- A truncated or partial extract (export cut off, disk full on the source).
- The source filtered the export differently (a wrong date range).
- A real drop in activity (a holiday): rare, and visible in the business.

## Diagnose

1. Compare the latest batch with the previous ones:

   ```sql
   select batch_id, file_name, row_count, loaded_at
   from raw._load_log where table_name = 'orders' order by loaded_at;
   ```
2. Look at what the batch contains (dates covered, last row):

   ```sql
   select min(updated_at), max(updated_at), count(*)
   from raw.orders where _batch_id = '<batch_id>';
   ```
3. Check the damage: did Gold change? Compare row counts with yesterday, or
   with a fingerprint taken before the load (`table_fingerprints` in
   `scripts/check_incremental.py`).

## Fix

If the batch is bad, quarantine it: keep its rows for audit, remove them from
the raw layer, then rebuild the table and everything downstream. The raw
layer is append-only; quarantine is the one documented exception.

```sql
begin;
create table if not exists raw._quarantine as
  select *, current_timestamp::timestamp as quarantined_at, ''::varchar as reason
  from raw.orders limit 0;
insert into raw._quarantine
  select *, current_timestamp::timestamp, 'truncated extract'
  from raw.orders where _batch_id = '<batch_id>';
delete from raw.orders where _batch_id = '<batch_id>';
delete from raw._load_log where batch_id = '<batch_id>';
commit;
```

```bash
dbt build --full-refresh -s stg_orders+    # use the affected table's stg_ model
```

`raw._quarantine` has the columns of the first table quarantined into it; use
one quarantine table per raw table (for example `raw._quarantine_orders`) if
you quarantine more than one.

Then ask the source to re-send the full extract and load it normally.

**Warning:** once the batch's log row is deleted, the loader would load the
same truncated file again if it were re-delivered. Check the file before
`make load`.

## Verify

`make check-slos` shows `batch_volume` met, and Gold matches the pre-incident
fingerprint (or yesterday's counts plus the expected new rows).
