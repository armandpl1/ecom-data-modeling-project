-- Reporting: RFM (recency, frequency, monetary) scores and segments.
-- Grain: one row per customer with at least one revenue order.
-- Each score is a quintile from 1 (worst) to 5 (best), ties broken by
-- customer_id so the scores are deterministic. Recency is measured from the
-- as-of date in dim_date (the last order date, or var('as_of_date')).

with as_of as (

    select max(as_of_date) as as_of_date from {{ ref('dim_date') }}

),

customers as (

    select
        customer_id,
        max(cast(order_placed_at as date)) as last_order_date,
        count(distinct order_id)           as orders,
        sum(recognized_revenue)            as recognized_revenue
    from {{ ref('fact_orders') }}
    where recognized_revenue > 0
    group by customer_id

),

scored as (

    select
        customers.customer_id,
        customers.last_order_date,
        as_of.as_of_date - customers.last_order_date                     as recency_days,
        customers.orders                                                 as frequency,
        cast(customers.recognized_revenue as decimal(14, 2))             as monetary,
        ntile(5) over (
            order by as_of.as_of_date - customers.last_order_date desc, customers.customer_id
        )                                                                as recency_score,
        ntile(5) over (order by customers.orders, customers.customer_id) as frequency_score,
        ntile(5) over (
            order by customers.recognized_revenue, customers.customer_id
        )                                                                as monetary_score
    from customers
    cross join as_of

)

select
    *,
    recency_score * 100 + frequency_score * 10 + monetary_score as rfm_code,
    case
        when recency_score >= 4 and frequency_score >= 4 then 'Champions'
        when frequency_score >= 4 then 'Loyal'
        when recency_score >= 4 and frequency_score >= 2 then 'Potential loyalist'
        when recency_score >= 4 then 'New'
        when recency_score <= 2 and frequency_score >= 3 then 'At risk'
        when recency_score <= 2 then 'Hibernating'
        else 'Needs attention'
    end                                                         as segment
from scored
