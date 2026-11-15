-- Silver: SCD Type 2 history of products. One row per version.
{{ scd2_from_history(
    relation=ref('stg_products'),
    unique_key='product_id',
    surrogate_key='product_sk',
    tracked_columns=[
        'sku', 'product_name', 'category', 'unit_price', 'currency', 'is_active'
    ]
) }}
