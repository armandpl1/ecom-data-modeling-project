-- Reporting: customer lifetime value.
-- Grain: one row per customer (customer_id), for every customer in the
-- dimension or in any fact (an early-arriving customer still counts).
-- Orders exclude cancelled ones; revenue is recognized revenue (delivered or
-- in flight, net of discounts; returns count as zero). Subscription revenue
-- is the sum of MRR over paying months.

with customers as (

    select customer_id from {{ ref('dim_customer') }}
    where customer_sk <> {{ unknown_member_key() }}
    union
    select customer_id from {{ ref('fact_orders') }}
    union
    select customer_id from {{ ref('fact_subscription_mrr_monthly') }}

),

latest_version as (

    select
        customer_id,
        customer_segment,
        state,
        is_deleted_customer
    from {{ ref('dim_customer') }}
    where customer_sk <> {{ unknown_member_key() }}
    qualify row_number() over (
        partition by customer_id order by valid_from_timestamp desc
    ) = 1

),

orders as (

    select
        customer_id,
        count(distinct order_id) filter (where not is_cancelled) as orders,
        sum(recognized_revenue)                                  as recognized_revenue,
        min(order_placed_at) filter (where not is_cancelled)     as first_order_at,
        max(order_placed_at) filter (where not is_cancelled)     as last_order_at
    from {{ ref('fact_orders') }}
    group by customer_id

),

subscriptions as (

    select
        customer_id,
        count(*) filter (where is_paying) as paying_subscription_months,
        sum(mrr)                          as subscription_revenue
    from {{ ref('fact_subscription_mrr_monthly') }}
    group by customer_id

),

current_mrr as (

    select
        mrr.customer_id,
        sum(mrr.mrr) as current_mrr
    from {{ ref('fact_subscription_mrr_monthly') }} as mrr
    inner join {{ ref('dim_date') }} as dates
        on mrr.month_start = dates.date_day
    where dates.is_current_month
    group by mrr.customer_id

)

select
    customers.customer_id,
    latest_version.customer_segment,
    latest_version.state,
    coalesce(latest_version.is_deleted_customer, false)          as is_deleted_customer,
    orders.first_order_at,
    orders.last_order_at,
    coalesce(orders.orders, 0)                                   as orders,
    cast(coalesce(orders.recognized_revenue, 0) as decimal(14, 2))
        as recognized_revenue,
    -- Exact half-up rounding in integer cents: dividing decimals goes through
    -- floating point on some engines (75.99 / 2 would round to 37.99)
    cast(
        {{ int_div(
            '2 * cast(round(coalesce(orders.recognized_revenue, 0) * 100) as bigint) + orders.orders',
            '2 * nullif(orders.orders, 0)'
        ) }} / 100.0
        as decimal(12, 2)
    )                                                            as average_order_value,
    coalesce(subscriptions.paying_subscription_months, 0)        as paying_subscription_months,
    cast(coalesce(subscriptions.subscription_revenue, 0) as decimal(14, 2))
        as subscription_revenue,
    cast(coalesce(current_mrr.current_mrr, 0) as decimal(12, 2)) as current_mrr,
    cast(
        coalesce(orders.recognized_revenue, 0)
        + coalesce(subscriptions.subscription_revenue, 0)
        as decimal(14, 2)
    )                                                            as lifetime_value
from customers
left join latest_version on customers.customer_id = latest_version.customer_id
left join orders on customers.customer_id = orders.customer_id
left join subscriptions on customers.customer_id = subscriptions.customer_id
left join current_mrr on customers.customer_id = current_mrr.customer_id
