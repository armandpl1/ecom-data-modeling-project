{{ config(
    materialized='incremental',
    unique_key='subscription_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Silver: SCD Type 2 history of subscriptions. One row per version.
--
-- Incremental: per-entity rebuild of subscriptions with Bronze rows ingested
-- after this table's watermark (see int_products_history).

{% set changed %}
    subscription_id in (
        select subscription_id from {{ ref('stg_subscriptions') }}
        where ingested_at > (select max(_max_ingested_at) from {{ this }})
    )
{% endset %}

with versions as (

    {{ scd2_from_history(
        relation=ref('stg_subscriptions'),
        unique_key='subscription_id',
        surrogate_key='subscription_version_sk',
        tracked_columns=[
            'customer_id', 'plan_name', 'billing_period', 'mrr', 'subscription_status',
            'started_on', 'trial_ends_on', 'cancelled_at'
        ],
        where=changed if is_incremental() else none
    ) }}

),

watermark as (

    select subscription_id, max(ingested_at) as _max_ingested_at
    from {{ ref('stg_subscriptions') }}
    group by subscription_id

)

select versions.*, watermark._max_ingested_at
from versions
inner join watermark using (subscription_id)
