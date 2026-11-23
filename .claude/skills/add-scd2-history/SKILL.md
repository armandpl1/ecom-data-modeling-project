---
name: add-scd2-history
description: How to build or change SCD Type 2 history in this repo with the scd2_from_history macro (tracked columns, surrogate keys, tombstones, incremental per-entity rebuild, half-open validity ranges and the scd2_valid_ranges test). Use when adding a *_history model or an SCD2 dimension, or changing which columns are tracked.
---

# SCD Type 2 history

All history comes from the Bronze change log through
`macros/scd2_from_history.sql`. Never hand-roll `lag`/`lead` versioning.

## Input contract

The relation passed in (a Bronze `stg_*` model) must have:
- one row per (`unique_key`, `updated_at`), already deduplicated;
- `loaded_at` and `is_deleted` columns (`is_deleted` true on the row for a
  hard delete, `_op = 'D'`).

## Silver model template

Copy `models/intermediate/int_products_history.sql`. The parts that matter:

```sql
{{ config(
    materialized='incremental',
    unique_key='<entity>_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

{% set changed %}
    <entity>_id in (
        select <entity>_id from {{ ref('stg_<entity>s') }}
        where ingested_at > (select max(_max_ingested_at) from {{ this }})
    )
{% endset %}

with versions as (
    {{ scd2_from_history(
        relation=ref('stg_<entity>s'),
        unique_key='<entity>_id',
        surrogate_key='<entity>_sk',
        tracked_columns=['col_a', 'col_b'],
        where=changed if is_incremental() else none
    ) }}
), ...
```

- **tracked_columns** decide when a new version starts: a version begins only
  when one of them (or `is_deleted`) changes. Columns not listed ride along
  with the version they were seen in. Adding a column here changes version
  counts; say so in the docs.
- **where** filters on the entity key only, so a changed entity's whole
  history is rebuilt (a late row lands in the right place).

## Semantics to preserve

- Validity is half-open: `[valid_from_timestamp, valid_to_timestamp)`.
  The current version has `valid_to_timestamp = null`, `is_current = true`.
- A delete is a final tombstone version (`is_deleted = true`) that closes the
  previous one. Gold dimensions expose it as `deleted_at` /
  `is_deleted_<entity>` rather than dropping history.
- Facts join a version with
  `event_ts >= valid_from_timestamp and (valid_to_timestamp is null or event_ts < valid_to_timestamp)`
  and fall back to `unknown_member_key()` when no version matches.

## Tests (required)

On the Silver model and the Gold dimension:

```yaml
    data_tests:
      - scd2_valid_ranges:
          arguments:
            entity_key: <entity>_id
            current_flag: is_current
            deleted_at: deleted_at        # Gold dimension only
    columns:
      - name: <entity>_sk
        data_tests: [unique, not_null]
```

Gold dimensions also include the unknown member row (`<entity>_sk = '-1'`).

## Verify

```bash
uv run dbt build -s int_<entity>s_history+
make check-incremental        # incremental == full refresh
make check-late-dimensions    # early-arriving facts land on the unknown member, then get re-pointed
```

`analyses/customer_profile_at_order_vs_today.sql` shows how to measure the
business impact of history (the README's "28%" figure); rerun it if a
change could move that number.
