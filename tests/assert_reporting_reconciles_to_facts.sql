-- The reporting marts must add up to the facts they summarize: revenue,
-- orders and subscription revenue in customer_lifetime_value, and the
-- customers and subscriptions counted in the cohort marts. Returns one row
-- per mismatch.

with orders as (

    select
        sum(recognized_revenue)                                  as revenue,
        count(distinct order_id) filter (where not is_cancelled) as orders,
        count(distinct customer_id) filter (where not is_cancelled)
            as ordering_customers
    from {{ ref('fact_orders') }}

),

mrr as (

    select
        sum(mrr)                                                 as mrr_total,
        count(distinct subscription_id) filter (where is_paying) as paying_subscriptions
    from {{ ref('fact_subscription_mrr_monthly') }}

),

clv as (

    select
        sum(recognized_revenue)            as revenue,
        sum(orders)                        as orders,
        count(*) filter (where orders > 0) as ordering_customers,
        sum(subscription_revenue)          as mrr_total
    from {{ ref('customer_lifetime_value') }}

),

order_cohorts as (

    select sum(cohort_customers) as customers
    from {{ ref('cohort_retention') }}
    where months_since_first_order = 0

),

subscription_cohorts as (

    select sum(cohort_subscriptions) as subscriptions
    from {{ ref('subscription_cohort_retention') }}
    where months_since_start = 0

),

repeat_base as (

    select sum(customers) as customers
    from {{ ref('repeat_purchase') }}

),

comparisons as (

    select
        'revenue'      as measure,
        orders.revenue as from_facts,
        clv.revenue    as from_marts
    from orders cross join clv
    union all
    select
        'orders'      as measure,
        orders.orders as from_facts,
        clv.orders    as from_marts
    from orders cross join clv
    union all
    select
        'ordering customers (lifetime value)' as measure,
        orders.ordering_customers             as from_facts,
        clv.ordering_customers                as from_marts
    from orders cross join clv
    union all
    select
        'ordering customers (cohorts)' as measure,
        orders.ordering_customers      as from_facts,
        order_cohorts.customers        as from_marts
    from orders cross join order_cohorts
    union all
    select
        'ordering customers (repeat purchase)' as measure,
        orders.ordering_customers              as from_facts,
        repeat_base.customers                  as from_marts
    from orders cross join repeat_base
    union all
    select
        'subscription revenue' as measure,
        mrr.mrr_total          as from_facts,
        clv.mrr_total          as from_marts
    from mrr cross join clv
    union all
    select
        'paying subscriptions'             as measure,
        mrr.paying_subscriptions           as from_facts,
        subscription_cohorts.subscriptions as from_marts
    from mrr cross join subscription_cohorts

)

select * from comparisons
where from_facts is distinct from from_marts
