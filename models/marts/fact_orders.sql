-- Gold: order line fact.
-- Grain: one row per order line (an order and one product on it). The
-- source sells one product per order, so every order has a single line
-- (order_line_number = 1); the model is keyed by line so multi-product
-- orders would fit without a redesign.
-- Each row reflects the order's latest known state. Dimension keys point to
-- the customer and product versions valid when the order was placed.
--
-- Incremental: recomputes orders whose order, customer or product history
-- was ingested in the latest load round or var('incremental_lookback_hours')
-- before it (macros/incremental_window_start.sql; delete+insert on order_id).
-- A run that misses more than the look-back needs `dbt build --full-refresh`
-- (run one weekly as a safety net).

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail',
    post_hook="delete from {{ this }} where order_id not in (select order_id from {{ ref('int_orders') }})"
) }}

with orders as (

    select * from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
    where _max_ingested_at >= {{ incremental_window_start() }}
    {% endif %}

)

select
    -- Keys
    order_id || '-1'                                              as order_line_id,
    order_id,                                       -- degenerate dimension
    1                                                             as order_line_number,
    customer_sk,
    product_sk,
    cast(strftime(order_placed_at, '%Y%m%d') as integer)          as order_date_key,
    cast(strftime(shipped_at,      '%Y%m%d') as integer)          as ship_date_key,
    cast(strftime(delivered_at,    '%Y%m%d') as integer)          as delivery_date_key,
    customer_id,
    product_id,

    -- Low-cardinality order attributes
    order_status,
    payment_method,
    currency,

    -- Additive measures
    quantity,
    gross_amount                                                  as gross_revenue,
    discount_amount,
    net_amount                                                    as net_revenue,
    case when is_cancelled then net_amount else 0 end             as cancelled_revenue,
    case when is_returned  then net_amount else 0 end             as returned_revenue,
    case when is_cancelled or is_returned then 0 else net_amount end
                                                                  as recognized_revenue,

    -- Non-additive measures (average them, don't sum them)
    unit_price,
    discount_pct,
    date_diff('day', order_placed_at, shipped_at)                 as days_to_ship,
    date_diff('day', order_placed_at, delivered_at)               as days_to_deliver,

    -- Timestamps and flags
    order_placed_at,
    shipped_at,
    delivered_at,
    cancelled_at,
    returned_at,
    last_updated_at,
    is_open,
    is_cancelled,
    is_returned,
    delivered_at is not null and shipped_at is null              as is_missing_ship_event

from orders
