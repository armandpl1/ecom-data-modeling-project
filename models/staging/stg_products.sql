{{ config(
    materialized='incremental',
    unique_key=['product_id', 'updated_at'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Bronze: one row per product change, cleaned and typed.

with source as (

    select * from {{ source('raw', 'products') }}
    {% if is_incremental() %}
    -- Incremental: only rows from loads newer than anything already here
        where _ingested_at > (select max(existing.ingested_at) from {{ this }} as existing)
    {% endif %}

),

cleaned as (

    select
        trim(product_id)                   as product_id,
        upper(trim(sku))                   as sku,
        nullif(trim(product_name), '')     as product_name,
        nullif(trim(category), '')         as category,
        cast(unit_price as decimal(10, 2)) as unit_price,
        upper(trim(currency))              as currency,
        cast(is_active as boolean)         as is_active,

        cast(created_at as timestamp)      as created_at,
        cast(updated_at as timestamp)      as updated_at,
        _op                                as source_operation,
        _op = 'D'                          as is_deleted,
        cast(_loaded_at as timestamp)      as loaded_at,
        _batch_id                          as batch_id,
        _ingested_at                       as ingested_at

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
            where new.product_id = existing.product_id and new.updated_at = existing.updated_at
        )
    {% endif %}

)

select *
from candidates
qualify row_number() over (
    partition by product_id, updated_at
    order by loaded_at desc, ingested_at desc
) = 1
