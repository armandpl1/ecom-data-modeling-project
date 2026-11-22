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

    select
        customer_id,
        updated_at as changed_at
    from profile_changes
    union
    select
        customer_id,
        valid_from_timestamp
    from tier_history

),

-- Each change point with the latest profile row at or before it, and the
-- tier in effect then (none if never subscribed). An as-of join written as a
-- join plus "keep the latest match", so it runs on any engine (DuckDB's ASOF
-- JOIN gives the same rows).
profile_at_change as (

    select
        change_points.customer_id,
        change_points.changed_at,
        profile.updated_at as profile_updated_at
    from change_points
    inner join profile_changes as profile
        on
            change_points.customer_id = profile.customer_id
            and change_points.changed_at >= profile.updated_at
    qualify row_number() over (
        partition by change_points.customer_id, change_points.changed_at
        order by profile.updated_at desc
    ) = 1

),

tier_at_change as (

    select
        change_points.customer_id,
        change_points.changed_at,
        tier.subscription_tier
    from change_points
    inner join tier_history as tier
        on
            change_points.customer_id = tier.customer_id
            and change_points.changed_at >= tier.valid_from_timestamp
    qualify row_number() over (
        partition by change_points.customer_id, change_points.changed_at
        order by tier.valid_from_timestamp desc
    ) = 1

),

state_at_change as (

    select
        {{ star_except(['updated_at'], 'profile') }},
        profile_at_change.changed_at             as updated_at,
        coalesce(tier.subscription_tier, 'none') as subscription_tier
    from profile_at_change
    inner join profile_changes as profile
        on
            profile_at_change.customer_id = profile.customer_id
            and profile_at_change.profile_updated_at = profile.updated_at
    left join tier_at_change as tier
        on
            profile_at_change.customer_id = tier.customer_id
            and profile_at_change.changed_at = tier.changed_at

)

select *
from state_at_change
-- After a delete, keep only the delete itself
where
    not is_deleted
    or updated_at = (
        select min(d.updated_at) from profile_changes as d
        where d.customer_id = state_at_change.customer_id and d.is_deleted
    )
