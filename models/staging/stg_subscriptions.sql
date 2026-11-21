{{ config(
    materialized='incremental',
    unique_key=['subscription_id', 'updated_at'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Bronze: one row per subscription change, cleaned and typed.

with source as (

    select * from {{ source('raw', 'subscriptions') }}
    {% if is_incremental() %}
    -- Incremental: only rows from loads newer than anything already here
    where _ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

cleaned as (

    select
        trim(subscription_id)                    as subscription_id,
        trim(customer_id)                        as customer_id,
        lower(trim(plan_name))                   as plan_name,
        lower(trim(billing_period))              as billing_period,
        cast(mrr as decimal(10, 2))              as mrr,
        lower(trim(status))                      as subscription_status,
        cast(nullif(started_at, '') as date)     as started_on,
        cast(nullif(trial_ends_at, '') as date)  as trial_ends_on,
        cast(nullif(cancelled_at, '') as timestamp)
                                                 as cancelled_at,

        cast(created_at as timestamp)            as created_at,
        cast(updated_at as timestamp)            as updated_at,
        _op                                      as source_operation,
        _op = 'D'                                as is_deleted,
        cast(_loaded_at as timestamp)            as loaded_at,
        _batch_id                                            as batch_id,
        _ingested_at                                         as ingested_at

    from source

),

candidates as (

    select * from cleaned
    {% if is_incremental() %}
    -- Rows already here for the same (key, updated_at) compete with the new
    -- ones, so the row loaded last still wins across runs
    union all by name
    select * from {{ this }} as existing
    where exists (
        select 1 from cleaned as new
        where new.subscription_id = existing.subscription_id and new.updated_at = existing.updated_at
    )
    {% endif %}

)

select *
from candidates
qualify row_number() over (
    partition by subscription_id, updated_at
    order by loaded_at desc, ingested_at desc
) = 1
