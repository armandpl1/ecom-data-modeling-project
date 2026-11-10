-- Silver: orders joined to the customer and product versions that were
-- valid when the order was placed (point-in-time join on SCD2 history).

with orders as (

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
    orders.*,

    customers.customer_sk,
    customers.customer_segment          as customer_segment_at_order,
    customers.state                     as customer_state_at_order,

    products.product_sk,
    products.category                   as product_category_at_order,
    products.unit_price                 as list_price_at_order

from orders
left join customers
    on  orders.customer_id = customers.customer_id
    and orders.order_placed_at >= customers.valid_from
    and (orders.order_placed_at < customers.valid_to or customers.valid_to is null)
left join products
    on  orders.product_id = products.product_id
    and orders.order_placed_at >= products.valid_from
    and (orders.order_placed_at < products.valid_to or products.valid_to is null)
