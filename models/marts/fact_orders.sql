-- Gold: order line fact.
-- Grain: one row per order line (an order and one product on it); an order
-- has 1-5 lines. Each row reflects the line's and the order's latest known
-- state. Dimension keys point to the customer and product versions valid
-- when the order was placed. Order-level totals: fact_order_headers.
--
-- Incremental: recomputes orders whose order, customer or product history
-- was ingested in the latest load round or var('incremental_lookback_hours')
-- before it (macros/incremental_window_start.sql; delete+insert on order_id).
-- Whole orders are recomputed, every line of them, so a change to one line
-- can't drop its siblings.
-- A run that misses more than the look-back needs `dbt build --full-refresh`
-- (run one weekly as a safety net).

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail',
    post_hook="delete from {{ this }} where order_line_id not in (select order_line_id from {{ ref('int_orders_enriched') }})"
) }}

with orders as (

    select * from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
        where order_id in (
            select changed.order_id from {{ ref('int_orders_enriched') }} as changed
            where changed._max_ingested_at >= {{ incremental_window_start() }}
        )
    {% endif %}

)

select
    -- Keys
    order_line_id,
    order_id,                                       -- degenerate dimension
    line_number                                           as order_line_number,
    -- An order whose customer or product version isn't known yet points to
    -- the unknown member instead of failing the build (unknown_member_rows warns)
    coalesce(customer_sk, {{ unknown_member_key() }})     as customer_sk,
    coalesce(product_sk, {{ unknown_member_key() }})      as product_sk,
    {{ date_key('order_placed_at') }}                     as order_date_key,
    {{ date_key('shipped_at') }}                          as ship_date_key,
    {{ date_key('delivered_at') }}                        as delivery_date_key,
    customer_id,
    product_id,

    -- Low-cardinality order attributes
    order_status,
    payment_method,
    currency,

    -- Additive measures
    quantity,
    gross_amount                                          as gross_revenue,
    discount_amount,
    net_amount                                            as net_revenue,
    case when is_cancelled then net_amount else 0 end     as cancelled_revenue,
    -- Returned: the whole order came back, or just this line
    case
        when not is_cancelled and (is_returned or is_line_returned) then net_amount
        else 0
    end                                                   as returned_revenue,
    case when is_cancelled or is_returned or is_line_returned then 0 else net_amount end
        as recognized_revenue,

    -- Non-additive measures (average them, don't sum them)
    unit_price,
    discount_pct,
    {{ days_between('order_placed_at', 'shipped_at') }}   as days_to_ship,
    {{ days_between('order_placed_at', 'delivered_at') }} as days_to_deliver,

    -- Timestamps and flags
    order_placed_at,
    shipped_at,
    delivered_at,
    cancelled_at,
    coalesce(returned_at, line_returned_at)               as returned_at,
    last_updated_at,
    is_open,
    is_cancelled,
    is_returned or is_line_returned                       as is_returned,
    is_line_returned,
    delivered_at is not null and shipped_at is null       as is_missing_ship_event

from orders
