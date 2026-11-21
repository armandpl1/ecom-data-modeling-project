{#- Start of the window that incremental Gold facts recompute.

    The newest load round starts at the earliest of each raw table's newest
    batch (tables in one nightly load are ingested seconds apart). The window
    opens var('incremental_lookback_hours') before that. Rows whose inputs
    were ingested inside the window are recomputed.

    A table with no new batch in the latest round makes the window start
    earlier: more rows are recomputed, never fewer. -#}
{% macro incremental_window_start() -%}
    (
        select min(newest_load) - interval ({{ var('incremental_lookback_hours') }}) hour
        from (
            select table_name, max(loaded_at) as newest_load
            from {{ source('raw', '_load_log') }}
            group by table_name
        )
    )
{%- endmacro %}
