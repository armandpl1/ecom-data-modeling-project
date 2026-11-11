{#-
  Build an SCD Type 2 table from a change-history model.

  The input must have one row per (unique_key, updated_at), plus
  `loaded_at` and `is_deleted` columns (the Bronze models provide these).

  Steps:
    1. Hash the tracked columns of each row.
    2. Drop rows whose hash equals the previous row's, so a version only
       starts when a tracked value actually changes.
    3. Each remaining row is a version, valid from its updated_at until
       the next version's updated_at. The last version has valid_to_timestamp = null.

  A delete (_op = 'D') becomes a final tombstone version with
  is_deleted = true, which closes the version before it.
-#}
{% macro scd2_from_history(relation, unique_key, tracked_columns, surrogate_key) %}

with history as (

    select
        *,
        md5(concat_ws('||',
            {%- for col in tracked_columns %}
            coalesce(cast({{ col }} as varchar), '<null>'),
            {%- endfor %}
            cast(is_deleted as varchar)
        )) as _change_hash
    from {{ relation }}

),

compared as (

    select
        *,
        lag(_change_hash) over (
            partition by {{ unique_key }}
            order by updated_at, loaded_at
        ) as _previous_hash
    from history

),

changes_only as (

    select * exclude (_change_hash, _previous_hash)
    from compared
    where _previous_hash is null
       or _change_hash <> _previous_hash

),

versioned as (

    select
        md5({{ unique_key }} || '|' || cast(updated_at as varchar)) as {{ surrogate_key }},
        *,
        updated_at as valid_from_timestamp,
        lead(updated_at) over (
            partition by {{ unique_key }} order by updated_at
        ) as valid_to_timestamp,
        row_number() over (
            partition by {{ unique_key }} order by updated_at
        ) as version_number,
        max(case when is_deleted then updated_at end) over (
            partition by {{ unique_key }}
        ) as deleted_at
    from changes_only

)

select
    *,
    valid_to_timestamp is null as is_current
from versioned

{% endmacro %}
