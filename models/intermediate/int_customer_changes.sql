{{ config(materialized='ephemeral') }}

-- Silver (ephemeral): one row per moment anything about a customer changed,
-- carrying their full state at that moment. Merges two timelines:
--   1. customer profile changes (stg_customers)
--   2. subscription tier changes (int_customer_tier_history)
-- int_customers_history turns this stream into SCD Type 2 versions.

with profile_changes as (

    select * from {{ ref('stg_customers') }}

),

tier_history as (

    select * from {{ ref('int_customer_tier_history') }}

),

change_points as (

    select customer_id, updated_at as changed_at from profile_changes
    union
    select customer_id, valid_from_timestamp from tier_history

),

state_at_change as (

    select
        profile.* exclude (updated_at),
        change_points.changed_at                      as updated_at,
        coalesce(tier.subscription_tier, 'none')     as subscription_tier
    from change_points
    -- Latest profile row at or before the change point
    asof join profile_changes as profile
        on  change_points.customer_id = profile.customer_id
        and change_points.changed_at >= profile.updated_at
    -- Tier in effect at the change point (none if never subscribed)
    asof left join tier_history as tier
        on  change_points.customer_id = tier.customer_id
        and change_points.changed_at >= tier.valid_from_timestamp

)

select *
from state_at_change
-- After a delete, keep only the delete itself
where not is_deleted
   or updated_at = (
        select min(d.updated_at) from profile_changes d
        where d.customer_id = state_at_change.customer_id and d.is_deleted
   )
