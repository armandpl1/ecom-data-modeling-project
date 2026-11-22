{{ config(
    materialized='incremental',
    unique_key=['order_id', 'updated_at'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Bronze: one row per order header change (status, payment, total), cleaned
-- and typed. Products, quantities and prices are in stg_order_lines.

with source as (

    select * from {{ source('raw', 'orders') }}
    {% if is_incremental() %}
    -- Incremental: only rows from loads newer than anything already here
        where _ingested_at > (select max(existing.ingested_at) from {{ this }} as existing)
    {% endif %}

),

cleaned as (

    select
        trim(order_id)                      as order_id,
        trim(customer_id)                   as customer_id,
        cast(order_total as decimal(12, 2)) as order_total,
        upper(trim(currency))               as currency,
        lower(trim(payment_method))         as payment_method,
        lower(trim(status))                 as order_status,

        cast(order_placed_at as timestamp)  as order_placed_at,
        cast(updated_at as timestamp)       as updated_at,
        _op                                 as source_operation,
        _op = 'D'                           as is_deleted,
        cast(_loaded_at as timestamp)       as loaded_at,
        _batch_id                           as batch_id,
        _ingested_at                        as ingested_at

    from source

),

candidates as (

    select * from cleaned
    {% if is_incremental() %}
    -- Rows already here for the same (key, updated_at) compete with the new
    -- ones, so the row loaded last still wins across runs
        {{ union_all_same_columns() }}
        select * from {{ this }} as existing
        where exists (
            select 1 from cleaned as new
            where new.order_id = existing.order_id and new.updated_at = existing.updated_at
        )
    {% endif %}

)

select *
from candidates
qualify row_number() over (
    partition by order_id, updated_at
    order by loaded_at desc, ingested_at desc
) = 1
