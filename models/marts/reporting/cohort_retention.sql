-- Reporting: order cohort retention.
-- Grain: one row per cohort month (month of a customer's first non-cancelled
-- order) and months since that first order (0, 1, 2, ... up to the as-of
-- month). retention_rate is the share of the cohort that placed at least
-- one non-cancelled order in that month; month 0 is always 1.

with orders as (

    select
        customer_id,
        date_trunc('month', order_placed_at)::date as order_month
    from {{ ref('fact_orders') }}
    where not is_cancelled
    group by customer_id, order_month

),

cohorts as (

    select
        customer_id,
        min(order_month) as cohort_month
    from orders
    group by customer_id

),

cohort_sizes as (

    select
        cohort_month,
        count(*) as cohort_customers
    from cohorts
    group by cohort_month

),

as_of as (

    select date_trunc('month', max(as_of_date))::date as as_of_month
    from {{ ref('dim_date') }}

),

grid as (

    select
        cohort_sizes.cohort_month,
        cohort_sizes.cohort_customers,
        offsets.months_since_first_order
    from cohort_sizes
    cross join as_of
    cross join
        range(0, 1000) as offsets (months_since_first_order)
    where
        cohort_sizes.cohort_month + to_months(offsets.months_since_first_order::integer)
        <= as_of.as_of_month

),

active as (

    select
        cohorts.cohort_month,
        date_diff('month', cohorts.cohort_month, orders.order_month) as months_since_first_order,
        count(distinct orders.customer_id)                           as active_customers
    from orders
    inner join cohorts on orders.customer_id = cohorts.customer_id
    group by cohorts.cohort_month, months_since_first_order

)

select
    grid.cohort_month,
    grid.months_since_first_order,
    grid.cohort_customers,
    coalesce(active.active_customers, 0) as active_customers,
    (coalesce(active.active_customers, 0) / grid.cohort_customers)::decimal(6, 4)
        as retention_rate
from grid
left join active
    on
        grid.cohort_month = active.cohort_month
        and grid.months_since_first_order = active.months_since_first_order
