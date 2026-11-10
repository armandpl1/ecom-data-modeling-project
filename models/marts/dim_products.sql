-- Gold: product dimension, SCD Type 2. One row per product version.

select
    product_sk,
    product_id,
    sku,
    product_name,
    category,
    unit_price,
    currency,
    is_active,
    created_at                         as product_created_at,

    version_number,
    valid_from,
    valid_to,
    is_current,
    deleted_at,
    deleted_at is not null             as is_deleted_product

from {{ ref('int_products_history') }}
where not is_deleted
