{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Silver: one row per order header with its latest state and lifecycle
-- timestamps. The lines (products, quantities, prices) are in int_order_lines.
--
-- Incremental: per-order rebuild of orders with Bronze rows ingested after
-- this table's watermark (_max_ingested_at, newest ingestion of the order).

with order_changes as (

    select * from {{ ref('stg_orders') }}
    where
        not is_deleted
    {% if is_incremental() %}
            and order_id in (
                select changed.order_id from {{ ref('stg_orders') }} as changed
                where
                    changed.ingested_at
                    > (select max(existing._max_ingested_at) from {{ this }} as existing)
            )
    {% endif %}

),

latest_state as (

    select *
    from order_changes
    qualify row_number() over (
        partition by order_id
        order by updated_at desc, loaded_at desc
    ) = 1

),

lifecycle as (

    -- First time each status was reached
    select
        order_id,
        min(updated_at) filter (where order_status = 'shipped')   as shipped_at,
        min(updated_at) filter (where order_status = 'delivered') as delivered_at,
        min(updated_at) filter (where order_status = 'cancelled') as cancelled_at,
        min(updated_at) filter (where order_status = 'returned')  as returned_at,
        count(*)                                                  as change_count,
        max(loaded_at)                                            as last_loaded_at,
        max(ingested_at)                                          as _max_ingested_at
    from order_changes
    group by order_id

)

select
    latest_state.order_id,
    latest_state.customer_id,
    latest_state.order_status,
    latest_state.payment_method,
    latest_state.currency,

    latest_state.order_total,

    latest_state.order_placed_at,
    lifecycle.shipped_at,
    lifecycle.delivered_at,
    lifecycle.cancelled_at,
    lifecycle.returned_at,
    latest_state.updated_at                            as last_updated_at,
    lifecycle.last_loaded_at,
    lifecycle.change_count,
    lifecycle._max_ingested_at,

    latest_state.order_status in ('placed', 'shipped') as is_open,
    latest_state.order_status = 'cancelled'            as is_cancelled,
    latest_state.order_status = 'returned'             as is_returned

from latest_state
inner join lifecycle using (order_id)
