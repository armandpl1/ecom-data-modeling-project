-- Why SCD Type 2 matters: compare each order's customer profile at the time
-- of the order with the customer's profile today.
-- Run with: dbt compile -s customer_profile_at_order_vs_today
-- then execute the SQL in target/compiled/ against ecom.duckdb.

with orders as (

    -- One row per order (fact_orders is one row per line)
    select * from {{ ref('fact_order_headers') }}

),

profile_at_order as (

    select * from {{ ref('dim_customer') }}

),

profile_today as (

    -- Latest version per customer (also covers deleted customers)
    select *
    from {{ ref('dim_customer') }}
    qualify row_number() over (
        partition by customer_id order by valid_from_timestamp desc
    ) = 1

)

select
    count(*)                                                                      as orders,
    count(*) filter (where at_order.customer_segment <> today.customer_segment)   as segment_changed,
    count(*) filter (where at_order.subscription_tier <> today.subscription_tier) as tier_changed,
    count(*) filter (where at_order.state <> today.state)                         as state_changed,
    count(*) filter (where at_order.email_hash <> today.email_hash)               as email_changed,
    count(
        *) filter (
        where at_order.customer_segment <> today.customer_segment
        or at_order.subscription_tier <> today.subscription_tier
        or at_order.state <> today.state
        or at_order.email_hash <> today.email_hash
    )                                                                             as any_changed
from orders
inner join profile_at_order as at_order on orders.customer_sk = at_order.customer_sk
inner join profile_today as today on orders.customer_id = today.customer_id
