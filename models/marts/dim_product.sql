-- Gold: product dimension, SCD Type 2. One row per product version.
-- Price changes and discontinuation start new versions, so facts joined on
-- product_sk see the product as it was at the time of the event.
-- Launch and discontinuation dates are product-level and repeated on every
-- version.

with versions as (

    select * from {{ ref('int_products_history') }}
    where not is_deleted

),

lifecycle as (

    select
        product_id,
        min(created_at)                                       as launched_at,
        min(valid_from_timestamp) filter (where not is_active) as discontinued_at
    from versions
    group by product_id

)

select
    -- Keys
    versions.product_sk,
    versions.product_id,
    versions.sku,

    -- Descriptive attributes
    versions.product_name,
    -- Names follow "<style> <type> - <color>", e.g. "Deluxe Tent - MediumOrchid"
    split_part(versions.product_name, ' - ', 1)                      as product_line,
    split_part(split_part(versions.product_name, ' - ', 1), ' ', 1)  as product_style,
    nullif(regexp_replace(split_part(versions.product_name, ' - ', 1), '^\S+\s*', ''), '')
                                                                     as product_type,
    nullif(split_part(versions.product_name, ' - ', 2), '')          as color,
    versions.category,
    split_part(versions.sku, '-', 1)                                 as category_code,

    -- Pricing
    versions.unit_price,
    versions.currency,
    case
        when versions.unit_price <  25 then '1. Under $25'
        when versions.unit_price <  50 then '2. $25–49'
        when versions.unit_price < 100 then '3. $50–99'
        else                                '4. $100+'
    end                                                              as price_band,

    -- Lifecycle
    versions.is_active,
    lifecycle.launched_at,
    cast(lifecycle.launched_at as date)                              as launch_date,
    cast(strftime(lifecycle.launched_at, '%Y%m%d') as integer)       as launch_date_key,
    lifecycle.discontinued_at,
    cast(strftime(lifecycle.discontinued_at, '%Y%m%d') as integer)   as discontinued_date_key,
    versions.deleted_at,
    versions.deleted_at is not null                                  as is_deleted_product,

    -- SCD2
    versions.version_number,
    versions.valid_from_timestamp,
    versions.valid_to_timestamp,
    versions.is_current

from versions
inner join lifecycle using (product_id)
