{{ config(
    materialized='incremental',
    unique_key='order_line_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Silver: one row per order line with its latest state.
--
-- Incremental: per-line rebuild of lines with Bronze rows ingested after this
-- table's watermark (_max_ingested_at, newest ingestion of the line).

with line_changes as (

    select * from {{ ref('stg_order_lines') }}
    where
        not is_deleted
    {% if is_incremental() %}
            and order_line_id in (
                select changed.order_line_id from {{ ref('stg_order_lines') }} as changed
                where
                    changed.ingested_at
                    > (select max(existing._max_ingested_at) from {{ this }} as existing)
            )
    {% endif %}

),

latest_state as (

    select *
    from line_changes
    qualify row_number() over (
        partition by order_line_id
        order by updated_at desc, loaded_at desc
    ) = 1

),

lifecycle as (

    select
        order_line_id,
        min(updated_at) filter (where line_status = 'returned') as line_returned_at,
        count(*)                                                as change_count,
        max(loaded_at)                                          as last_loaded_at,
        max(ingested_at)                                        as _max_ingested_at
    from line_changes
    group by order_line_id

)

select
    latest_state.order_line_id,
    latest_state.order_id,
    latest_state.line_number,
    latest_state.product_id,

    latest_state.quantity,
    latest_state.unit_price,
    latest_state.discount_pct,
    cast(latest_state.quantity * latest_state.unit_price as decimal(12, 2))
        as gross_amount,
    cast(
        latest_state.quantity * latest_state.unit_price - latest_state.line_total
        as decimal(12, 2)
    )                                     as discount_amount,
    latest_state.line_total               as net_amount,

    latest_state.line_status,
    latest_state.line_status = 'returned' as is_line_returned,
    lifecycle.line_returned_at,
    latest_state.updated_at               as line_last_updated_at,
    lifecycle.last_loaded_at              as line_last_loaded_at,
    lifecycle.change_count                as line_change_count,
    lifecycle._max_ingested_at

from latest_state
inner join lifecycle using (order_line_id)
