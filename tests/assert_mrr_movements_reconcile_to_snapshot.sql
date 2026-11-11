-- The two subscription facts must agree: for every month, the sum of all
-- MRR movements up to month end equals the MRR in the monthly snapshot.

with snapshot as (

    select month_start, sum(mrr) as snapshot_mrr
    from {{ ref('fact_subscription_mrr_monthly') }}
    group by month_start

),

movements as (

    select
        snapshot.month_start,
        coalesce(sum(events.mrr_change), 0) as movement_mrr
    from snapshot
    left join {{ ref('fact_subscriptions') }} as events
        on events.event_at < cast(last_day(snapshot.month_start) as timestamp) + interval 1 day
    group by snapshot.month_start

)

select snapshot.month_start, snapshot_mrr, movement_mrr
from snapshot
inner join movements using (month_start)
where abs(snapshot_mrr - movement_mrr) > 0.01
