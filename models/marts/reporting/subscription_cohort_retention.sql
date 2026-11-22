-- Reporting: subscription cohort retention.
-- Grain: one row per cohort month (a subscription's first paying month) and
-- months since then (0, 1, 2, ... up to the last month with MRR data).
-- retention_rate is the share of the cohort's subscriptions still paying in
-- that month; mrr_retention is their MRR relative to the cohort's month-0 MRR.

with paying as (

    select
        subscription_id,
        month_start,
        mrr
    from {{ ref('fact_subscription_mrr_monthly') }}
    where is_paying

),

cohorts as (

    select
        subscription_id,
        min(month_start) as cohort_month
    from paying
    group by subscription_id

),

cohort_sizes as (

    select
        cohorts.cohort_month,
        count(*)        as cohort_subscriptions,
        sum(paying.mrr) as starting_mrr
    from cohorts
    inner join paying
        on
            cohorts.subscription_id = paying.subscription_id
            and cohorts.cohort_month = paying.month_start
    group by cohorts.cohort_month

),

last_month as (

    select max(month_start) as last_month from {{ ref('fact_subscription_mrr_monthly') }}

),

offsets as (

    {{ integer_range(0, 1000, 'months_since_start') }}

),

grid as (

    select
        cohort_sizes.cohort_month,
        cohort_sizes.cohort_subscriptions,
        cohort_sizes.starting_mrr,
        offsets.months_since_start
    from cohort_sizes
    cross join last_month
    cross join offsets
    where
        {{ add_months_to('cohort_sizes.cohort_month', 'cast(offsets.months_since_start as integer)') }}
        <= last_month.last_month

),

active as (

    select
        cohorts.cohort_month,
        {{ months_between('cohorts.cohort_month', 'paying.month_start') }} as months_since_start,
        count(*)                                                           as paying_subscriptions,
        sum(paying.mrr)                                                    as mrr
    from paying
    inner join cohorts on paying.subscription_id = cohorts.subscription_id
    group by cohorts.cohort_month, months_since_start

)

select
    grid.cohort_month,
    grid.months_since_start,
    grid.cohort_subscriptions,
    coalesce(active.paying_subscriptions, 0)                                              as paying_subscriptions,
    (coalesce(active.mrr, 0))::decimal(12, 2)                                             as mrr,
    (coalesce(active.paying_subscriptions, 0) / grid.cohort_subscriptions)::decimal(6, 4) as retention_rate,
    (coalesce(active.mrr, 0) / grid.starting_mrr)::decimal(8, 4)                          as mrr_retention
from grid
left join active
    on
        grid.cohort_month = active.cohort_month
        and grid.months_since_start = active.months_since_start
