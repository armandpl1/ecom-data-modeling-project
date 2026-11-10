-- Gold: order fact. One row per order, at its latest known state.
-- customer_sk / product_sk point to the dimension versions valid when the
-- order was placed.

select
    order_id,
    customer_sk,
    product_sk,
    cast(strftime(order_placed_at, '%Y%m%d') as integer)  as order_date_key,
    customer_id,
    product_id,

    order_status,
    payment_method,
    currency,
    customer_segment_at_order,

    quantity,
    unit_price,
    list_price_at_order,
    discount_pct,
    gross_amount,
    discount_amount,
    net_amount,
    case when is_cancelled or is_returned then 0 else net_amount end
                                                           as recognized_revenue,

    order_placed_at,
    shipped_at,
    delivered_at,
    cancelled_at,
    returned_at,
    date_diff('day', order_placed_at, shipped_at)          as days_to_ship,
    date_diff('day', order_placed_at, delivered_at)        as days_to_deliver,

    is_open,
    is_cancelled,
    is_returned,
    last_updated_at

from {{ ref('int_orders_enriched') }}
