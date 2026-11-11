-- Point-in-time check: each order must point to the customer version that
-- was valid at the exact moment the order was placed.

select
    o.order_id,
    o.order_placed_at,
    c.valid_from_timestamp,
    c.valid_to_timestamp
from {{ ref('fact_orders') }} o
inner join {{ ref('dim_customer') }} c
    on o.customer_sk = c.customer_sk
where o.customer_id <> c.customer_id
   or o.order_placed_at < c.valid_from_timestamp
   or o.order_placed_at >= c.valid_to_timestamp
