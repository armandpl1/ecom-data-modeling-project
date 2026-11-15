-- Silver: a customer's subscription tier over time.
-- One row per tier period. Tier is the plan of the customer's live
-- (not cancelled) subscription, or 'none'. If a customer ever has several
-- live subscriptions, the highest plan wins.

with subscription_versions as (

    select * from {{ ref('int_subscriptions_history') }}
    where not is_deleted

),

-- Every moment any of the customer's subscriptions changed
change_points as (

    select distinct customer_id, valid_from_timestamp as changed_at
    from subscription_versions
    union
    -- Moment a deleted subscription stopped existing
    select distinct customer_id, deleted_at
    from {{ ref('int_subscriptions_history') }}
    where deleted_at is not null

),

tier_at_change as (

    select
        change_points.customer_id,
        change_points.changed_at,
        coalesce(
            arg_max(
                subscription_versions.plan_name,
                case subscription_versions.plan_name
                    when 'premium' then 3
                    when 'plus'    then 2
                    when 'basic'   then 1
                end
            ) filter (where subscription_versions.subscription_status <> 'cancelled'),
            'none'
        ) as subscription_tier
    from change_points
    left join subscription_versions
        on  subscription_versions.customer_id = change_points.customer_id
        and subscription_versions.valid_from_timestamp <= change_points.changed_at
        and (subscription_versions.valid_to_timestamp > change_points.changed_at
             or subscription_versions.valid_to_timestamp is null)
    group by change_points.customer_id, change_points.changed_at

),

-- Keep only moments where the tier actually changed
tier_changes as (

    select *
    from tier_at_change
    qualify subscription_tier is distinct from lag(subscription_tier) over (
        partition by customer_id order by changed_at
    )

)

select
    customer_id,
    subscription_tier,
    changed_at as valid_from_timestamp,
    lead(changed_at) over (
        partition by customer_id order by changed_at
    ) as valid_to_timestamp
from tier_changes
