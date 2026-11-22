-- Silver: order lines joined to their order header and to the customer and
-- product versions that were valid when the order was placed (point-in-time
-- join on SCD2 history). One row per order line.
-- A full table: a customer or product change can move an old order to
-- another version, so every line is re-joined on each run (it's cheap).
-- An order header whose lines haven't loaded yet has no row here
-- (assert_order_headers_have_lines warns about it).

with lines as (

    select * from {{ ref('int_order_lines') }}

),

orders as (

    select * from {{ ref('int_orders') }}

),

customers as (

    select * from {{ ref('int_customers_history') }}
    where not is_deleted

),

products as (

    select * from {{ ref('int_products_history') }}
    where not is_deleted

)

select
    lines.* exclude (_max_ingested_at),

    orders.customer_id,
    orders.order_status,
    orders.payment_method,
    orders.currency,
    orders.order_total,
    orders.order_placed_at,
    orders.shipped_at,
    orders.delivered_at,
    orders.cancelled_at,
    orders.returned_at,
    greatest(orders.last_updated_at, lines.line_last_updated_at)
        as last_updated_at,
    orders.is_open,
    orders.is_cancelled,
    orders.is_returned,

    customers.customer_sk,
    customers.customer_segment  as customer_segment_at_order,
    customers.state             as customer_state_at_order,
    customers.subscription_tier as subscription_tier_at_order,

    products.product_sk,
    products.category           as product_category_at_order,
    products.unit_price         as list_price_at_order,

    -- Newest ingestion behind this row (line, order, customer or product
    -- history); the incremental Gold facts use it to find the orders to
    -- recompute
    greatest(
        lines._max_ingested_at,
        orders._max_ingested_at,
        customers._max_ingested_at,
        products._max_ingested_at
    )                           as _max_ingested_at

from lines
inner join orders on lines.order_id = orders.order_id
left join customers
    on
        orders.customer_id = customers.customer_id
        and orders.order_placed_at >= customers.valid_from_timestamp
        and (orders.order_placed_at < customers.valid_to_timestamp or customers.valid_to_timestamp is null)
left join products
    on
        lines.product_id = products.product_id
        and orders.order_placed_at >= products.valid_from_timestamp
        and (orders.order_placed_at < products.valid_to_timestamp or products.valid_to_timestamp is null)
