-- Gold: order fact at the order grain.
-- Grain: one row per order, including orders whose lines haven't loaded yet
-- (line_count = 0). Line measures are summed from fact_orders; order_total is
-- the header's own total, which must equal the sum of the lines
-- (assert_order_lines_reconcile_to_header). The customer key points to the
-- customer version valid when the order was placed.

with orders as (

    select * from {{ ref('int_orders') }}

),

customers as (

    select * from {{ ref('int_customers_history') }}
    where not is_deleted

),

lines as (

    select
        order_id,
        count(*)                                 as line_count,
        count(*) filter (where is_line_returned) as returned_lines,
        sum(quantity)                            as units,
        sum(gross_revenue)                       as gross_revenue,
        sum(discount_amount)                     as discount_amount,
        sum(net_revenue)                         as net_revenue,
        sum(cancelled_revenue)                   as cancelled_revenue,
        sum(returned_revenue)                    as returned_revenue,
        sum(recognized_revenue)                  as recognized_revenue
    from {{ ref('fact_orders') }}
    group by order_id

)

select
    -- Keys
    orders.order_id,
    coalesce(customers.customer_sk, {{ unknown_member_key() }})   as customer_sk,
    cast(strftime(orders.order_placed_at, '%Y%m%d') as integer)   as order_date_key,
    orders.customer_id,

    -- Attributes
    orders.order_status,
    orders.payment_method,
    orders.currency,

    -- Additive measures (sums of the lines; 0 while no line has loaded)
    coalesce(lines.line_count, 0)                                 as line_count,
    coalesce(lines.returned_lines, 0)                             as returned_lines,
    cast(coalesce(lines.units, 0) as bigint)                      as units,
    cast(coalesce(lines.gross_revenue, 0) as decimal(12, 2))      as gross_revenue,
    cast(coalesce(lines.discount_amount, 0) as decimal(12, 2))    as discount_amount,
    cast(coalesce(lines.net_revenue, 0) as decimal(12, 2))        as net_revenue,
    cast(coalesce(lines.cancelled_revenue, 0) as decimal(12, 2))  as cancelled_revenue,
    cast(coalesce(lines.returned_revenue, 0) as decimal(12, 2))   as returned_revenue,
    cast(coalesce(lines.recognized_revenue, 0) as decimal(12, 2)) as recognized_revenue,
    orders.order_total,

    -- Timestamps and flags
    orders.order_placed_at,
    orders.shipped_at,
    orders.delivered_at,
    orders.cancelled_at,
    orders.returned_at,
    orders.is_open,
    orders.is_cancelled,
    orders.is_returned,
    lines.order_id is not null                                    as has_lines

from orders
left join lines on orders.order_id = lines.order_id
left join customers
    on
        orders.customer_id = customers.customer_id
        and orders.order_placed_at >= customers.valid_from_timestamp
        and (orders.order_placed_at < customers.valid_to_timestamp or customers.valid_to_timestamp is null)
