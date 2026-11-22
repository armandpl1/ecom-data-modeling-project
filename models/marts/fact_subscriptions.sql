-- Gold: subscription event fact (MRR movements and churn).
-- Grain: one row per subscription change that moves MRR or cancels the
-- subscription. Summing mrr_change over all events up to a date gives the
-- MRR on that date (tested against fact_subscription_mrr_monthly).
--
-- Paying MRR = mrr while the subscription is active or past_due, else 0.
-- movement_type:
--   new           first time the subscription pays
--   expansion     MRR goes up while paying (upgrade, repricing)
--   contraction   MRR goes down while paying (downgrade)
--   pause         paying -> paused (MRR to 0, not cancelled)
--   resume        paused -> paying again
--   reactivation  paying again after any other non-paying state
--   churn         paying -> cancelled (MRR to 0)
--   no_mrr_change cancellation of a subscription that was not paying
--                 (a trial or a paused subscription)

-- Incremental: recomputes every event of a subscription whose history, or
-- whose customer's history, was ingested in the latest load round or
-- var('incremental_lookback_hours') before it
-- (macros/incremental_window_start.sql; delete+insert on subscription_id, so
-- events that no longer exist are removed too).

{{ config(
    materialized='incremental',
    unique_key='subscription_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with versions as (

    select
        *,
        case
            when not is_deleted and subscription_status in ('active', 'past_due') then mrr
            else 0
        end as paying_mrr
    from {{ ref('int_subscriptions_history') }}
    {% if is_incremental() %}
        {% set lookback_start = incremental_window_start() %}
        -- Whole subscriptions only, so the window functions below see full timelines
        where subscription_id in (
            select changed_subscriptions.subscription_id
            from {{ ref('int_subscriptions_history') }} as changed_subscriptions
            where
                changed_subscriptions._max_ingested_at >= {{ lookback_start }}
                or changed_subscriptions.customer_id in (
                    select changed_customers.customer_id
                    from {{ ref('int_customers_history') }} as changed_customers
                    where changed_customers._max_ingested_at >= {{ lookback_start }}
                )
        )
    {% endif %}

),

with_previous as (

    select
        *,
        lag(paying_mrr, 1, 0) over w    as mrr_before,
        lag(subscription_status) over w as status_before,
        lag(plan_name) over w           as plan_before,
        coalesce(
            max(paying_mrr) over (w rows between unbounded preceding and 1 preceding),
            0
        ) > 0                           as has_paid_before
    from versions
    window w as (partition by subscription_id order by valid_from_timestamp)

),

events as (

    select
        *,
        paying_mrr - mrr_before                          as mrr_change,
        (subscription_status = 'cancelled' or is_deleted)
        and (status_before is distinct from 'cancelled') as is_churn_event
    from with_previous

),

classified as (

    select
        *,
        case
            when mrr_before = 0 and paying_mrr > 0
                then
                    case
                        when not has_paid_before then 'new'
                        when status_before = 'paused' then 'resume'
                        else 'reactivation'
                    end
            when mrr_before > 0 and paying_mrr = 0
                then
                    case when subscription_status = 'paused' then 'pause' else 'churn' end
            when paying_mrr > mrr_before then 'expansion'
            when paying_mrr < mrr_before then 'contraction'
            else 'no_mrr_change'
        end as movement_type
    from events
    where mrr_change <> 0 or is_churn_event

),

customers as (

    select * from {{ ref('int_customers_history') }}
    where not is_deleted

)

select
    -- Keys
    classified.subscription_version_sk                          as subscription_event_id,
    classified.subscription_id,                     -- degenerate dimension
    coalesce(customers.customer_sk, {{ unknown_member_key() }}) as customer_sk,
    cast(strftime(classified.valid_from_timestamp, '%Y%m%d') as integer)
        as event_date_key,
    classified.customer_id,
    classified.valid_from_timestamp                             as event_at,

    -- What changed
    classified.movement_type,
    classified.status_before,
    classified.subscription_status                              as status_after,
    classified.plan_before,
    classified.plan_name                                        as plan_after,
    classified.billing_period,

    -- Additive measures
    classified.mrr_change,
    case when classified.mrr_change > 0 then classified.mrr_change else 0 end
        as mrr_gained,
    case when classified.mrr_change < 0 then -classified.mrr_change else 0 end
        as mrr_lost,
    case when classified.is_churn_event then 1 else 0 end       as churned_subscriptions,

    -- Semi-additive: balances, sum only within one subscription
    classified.mrr_before,
    classified.paying_mrr                                       as mrr_after,

    -- Churn details
    classified.is_churn_event,
    case
        when not classified.is_churn_event then null
        when classified.status_before = 'trialing' then 'trial'
        when classified.status_before = 'paused' then 'paused'
        else 'paid'
    end                                                         as churn_type

from classified
left join customers
-- Customer version at the event. If the customer was already deleted,
    -- use their last version before the delete.
    on
        classified.customer_id = customers.customer_id
        and customers.valid_from_timestamp
        <= case
            when classified.valid_from_timestamp >= customers.deleted_at
                then customers.deleted_at - interval 1 microsecond
            else classified.valid_from_timestamp
        end
        and (
            customers.valid_to_timestamp
            > case
                when classified.valid_from_timestamp >= customers.deleted_at
                    then customers.deleted_at - interval 1 microsecond
                else classified.valid_from_timestamp
            end
            or customers.valid_to_timestamp is null
        )
