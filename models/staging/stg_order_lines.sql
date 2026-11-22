{{ config(
    materialized='incremental',
    unique_key=['order_line_id', 'updated_at'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Bronze: one row per order line version, cleaned and typed. A line changes
-- when its order is corrected or discounted, or when the line is returned.

with source as (

    select * from {{ source('raw', 'order_lines') }}
    {% if is_incremental() %}
    -- Incremental: only rows from loads newer than anything already here
        where _ingested_at > (select max(existing.ingested_at) from {{ this }} as existing)
    {% endif %}

),

cleaned as (

    select
        trim(order_line_id)                as order_line_id,
        trim(order_id)                     as order_id,
        cast(line_number as integer)       as line_number,
        trim(product_id)                   as product_id,
        cast(quantity as integer)          as quantity,
        cast(unit_price as decimal(10, 2)) as unit_price,
        coalesce(cast(discount_pct as decimal(5, 4)), 0)
            as discount_pct,
        cast(line_total as decimal(12, 2)) as line_total,
        lower(trim(line_status))           as line_status,

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
        {{ union_all_same_columns() }}
        select * from {{ this }} as existing
        where exists (
            select 1 from cleaned as new
            where
                new.order_line_id = existing.order_line_id
                and new.updated_at = existing.updated_at
        )
    {% endif %}

)

select *
from candidates
qualify row_number() over (
    partition by order_line_id, updated_at
    order by loaded_at desc, ingested_at desc
) = 1
