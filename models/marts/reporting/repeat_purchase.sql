-- Reporting: repeat purchase behavior by first-order month.
-- Grain: one row per cohort month (month of a customer's first non-cancelled
-- order). A repeat customer placed two or more non-cancelled orders; the gap
-- statistics use every pair of consecutive orders of the cohort's customers.

with orders as (

    select
        customer_id,
        order_id,
        min(order_placed_at) as order_placed_at
    from {{ ref('fact_orders') }}
    where not is_cancelled
    group by customer_id, order_id

),

sequenced as (

    select
        customer_id,
        order_placed_at,
        min(order_placed_at) over (partition by customer_id) as first_order_at,
        count(*) over (partition by customer_id)             as orders,
        date_diff(
            'day',
            lag(order_placed_at) over (partition by customer_id order by order_placed_at, order_id),
            order_placed_at
        )                                                    as days_since_previous_order
    from orders

),

by_cohort as (

    select
        date_trunc('month', first_order_at)::date              as cohort_month,
        count(distinct customer_id)                            as customers,
        count(distinct customer_id) filter (where orders >= 2) as repeat_customers,
        avg(days_since_previous_order)                         as avg_days,
        median(days_since_previous_order)                      as median_days
    from sequenced
    group by cohort_month

)

select
    cohort_month,
    customers,
    repeat_customers,
    (repeat_customers / customers)::decimal(6, 4) as repeat_rate,
    avg_days::decimal(8, 1)                       as avg_days_between_orders,
    median_days::decimal(8, 1)                    as median_days_between_orders
from by_cohort
