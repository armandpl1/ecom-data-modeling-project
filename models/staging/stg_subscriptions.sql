-- Bronze: one row per subscription change, cleaned and typed.

with source as (

    select * from {{ source('raw', 'subscriptions') }}

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
        cast(_loaded_at as timestamp)            as loaded_at

    from source

)

select *
from cleaned
qualify row_number() over (
    partition by subscription_id, updated_at
    order by loaded_at desc
) = 1
