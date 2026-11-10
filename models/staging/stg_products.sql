-- Bronze: one row per product change, cleaned and typed.

with source as (

    select * from {{ source('raw', 'products') }}

),

cleaned as (

    select
        trim(product_id)                         as product_id,
        upper(trim(sku))                         as sku,
        nullif(trim(product_name), '')           as product_name,
        nullif(trim(category), '')               as category,
        cast(unit_price as decimal(10, 2))       as unit_price,
        upper(trim(currency))                    as currency,
        cast(is_active as boolean)               as is_active,

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
    partition by product_id, updated_at
    order by loaded_at desc
) = 1
