-- Bronze: one row per order status change, cleaned and typed.

with source as (

    select * from {{ source('raw', 'orders') }}

),

cleaned as (

    select
        trim(order_id)                           as order_id,
        trim(customer_id)                        as customer_id,
        trim(product_id)                         as product_id,
        cast(quantity as integer)                as quantity,
        cast(unit_price as decimal(10, 2))       as unit_price,
        coalesce(cast(discount_pct as decimal(5, 4)), 0)
                                                 as discount_pct,
        cast(order_total as decimal(12, 2))      as order_total,
        upper(trim(currency))                    as currency,
        lower(trim(payment_method))              as payment_method,
        lower(trim(status))                      as order_status,

        cast(order_placed_at as timestamp)       as order_placed_at,
        cast(updated_at as timestamp)            as updated_at,
        _op                                      as source_operation,
        _op = 'D'                                as is_deleted,
        cast(_loaded_at as timestamp)            as loaded_at

    from source

)

select *
from cleaned
qualify row_number() over (
    partition by order_id, updated_at
    order by loaded_at desc
) = 1
