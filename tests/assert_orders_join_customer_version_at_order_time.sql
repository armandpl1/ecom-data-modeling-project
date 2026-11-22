-- Point-in-time check: each order must point to the customer version that
-- was valid at the exact moment the order was placed. Orders on the unknown
-- member (customer not known yet) are reported by unknown_member_rows instead.

select
    o.order_id,
    o.order_placed_at,
    c.valid_from_timestamp,
    c.valid_to_timestamp
from {{ ref('fact_orders') }} as o
inner join {{ ref('dim_customer') }} as c
    on o.customer_sk = c.customer_sk
where
    o.customer_sk <> {{ unknown_member_key() }}
    and (
        o.customer_id <> c.customer_id
        or o.order_placed_at < c.valid_from_timestamp
        or o.order_placed_at >= c.valid_to_timestamp
    )
