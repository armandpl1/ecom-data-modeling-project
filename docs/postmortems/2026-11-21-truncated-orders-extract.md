# Postmortem: truncated orders extract (2026-11-21)

**Blameless.** This incident was reproduced on purpose to test the
monitoring, on a local copy of the pipeline built from the default sample
data. Times are the real times of that run (EST).

## Summary

An orders extract arrived with 10% of its usual rows (1,819 instead of
18,199). The loader accepted it, because it was a new file with a new
checksum, and `dbt build` passed all 176 tests. Only the `batch_volume`
service-level check flagged it. No Gold table was damaged. The batch was
quarantined and the affected models rebuilt, about 3 minutes after delivery.

## Timeline

| Time     | Event                                                                                      |
| -------- | ------------------------------------------------------------------------------------------ |
| 09:51:54 | Baseline: full extracts generated and loaded (21,442 rows in 4 batches)                    |
| 09:53:12 | Baseline `dbt build`: PASS=176 WARN=1 (known) ERROR=0; all 13 SLO results met              |
| 09:53:36 | Truncated `orders.csv` delivered and loaded as batch `orders-244dde45dfe8` (1,819 rows)    |
| 09:55:17 | Nightly `dbt build` finished: PASS=176 WARN=1 ERROR=0. The tests did not catch it          |
| 09:55:17 | `check_slos`: `batch_volume` / orders BREACHED, "1819 rows, 10% of previous average"       |
| 09:55:27 | Diagnosed from `raw._load_log`; Gold compared with the pre-incident fingerprint: no change |
| 09:55:38 | Batch quarantined: 1,819 rows moved to `raw._quarantine`, its log row removed              |
| 09:56:36 | `dbt build --full-refresh -s stg_orders+`: PASS=76 WARN=1 ERROR=0                          |
| 09:56:37 | `check_slos`: 0 breached; all 7 Gold and restricted tables identical to before             |

Delivery to detection: 1 min 41 s (the length of the nightly build). Detection
to verified fix: 1 min 20 s.

## Impact

None on Gold. The truncated file repeated rows that had already been loaded,
and Bronze keeps one row per key and `updated_at`, so the duplicates were
absorbed. 1,799 Bronze rows were attributed to the bad batch until the
rebuild. A truncated file with *changed* rows, or one arriving first, would
have removed orders from the facts' recent window or left them stale.

## Root cause

A partial extract (simulated by cutting `orders.csv` to its first 10% of
rows). Nothing in the load or the tests checked volume: the loader only
rejects exact duplicates, and dbt tests check that rows are valid, not that
there are enough of them.

## What went well

- The `batch_volume` objective caught what 176 dbt tests did not.
- The batch log made the bad batch obvious in one query.
- The raw layer kept everything, so the fix was a quarantine and a rebuild,
  not a re-extraction.

## What went less well

- After quarantine, the loader would accept the same truncated file again,
  because its checksum is no longer in `raw._load_log`.
- The quarantine is a manual SQL procedure (written down in the runbook).

## Action items

| Action                                                                 | Status                                               |
| ---------------------------------------------------------------------- | ---------------------------------------------------- |
| Check batch volume after every load (`service_levels` asset, CI)       | Done in this change                                  |
| Runbook for the volume alert, including the quarantine steps           | Done: [batch-volume.md](../runbooks/batch-volume.md) |
| Make the loader refuse checksums that are in quarantine                | Open                                                 |
| Check volume before Bronze, so a bad batch never reaches the warehouse | Open (today the check runs after `dbt build`)        |
