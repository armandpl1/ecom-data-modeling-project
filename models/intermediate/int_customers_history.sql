{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Silver: SCD Type 2 history of customers. One row per version.
-- A new version starts when a profile attribute or the subscription tier
-- changes (see int_customer_changes for how the two timelines are merged).
--
-- Incremental: per-entity rebuild. A customer is rebuilt when any of their
-- profile rows, or any row of a subscription ever linked to them (it can
-- change their tier), was ingested after this table's watermark.

{% set changed %}
    customer_id in (
        select customer_id from {{ ref('stg_customers') }}
        where ingested_at > (select max(_max_ingested_at) from {{ this }})
        union
        select customer_id from {{ ref('stg_subscriptions') }}
        where subscription_id in (
            select subscription_id from {{ ref('stg_subscriptions') }}
            where ingested_at > (select max(_max_ingested_at) from {{ this }})
        )
    )
{% endset %}

with versions as (

    {{ scd2_from_history(
        relation=ref('int_customer_changes'),
        unique_key='customer_id',
        surrogate_key='customer_sk',
        tracked_columns=[
            'first_name', 'last_name', 'email', 'phone_number', 'phone_extension',
            'street_address', 'city', 'state', 'postal_code', 'country',
            'customer_segment', 'marketing_opt_in', 'subscription_tier'
        ],
        where=changed if is_incremental() else none
    ) }}

),

subscription_ingestion as (

    -- Newest ingestion of any subscription ever linked to the customer
    select links.customer_id, max(subs.ingested_at) as ingested_at
    from (select distinct customer_id, subscription_id from {{ ref('stg_subscriptions') }}) as links
    inner join {{ ref('stg_subscriptions') }} as subs using (subscription_id)
    group by links.customer_id

),

watermark as (

    select
        profile.customer_id,
        greatest(max(profile.ingested_at), max(subscription_ingestion.ingested_at))
            as _max_ingested_at
    from {{ ref('stg_customers') }} as profile
    left join subscription_ingestion using (customer_id)
    group by profile.customer_id

)

select versions.*, watermark._max_ingested_at
from versions
inner join watermark using (customer_id)
