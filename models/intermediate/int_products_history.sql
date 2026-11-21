{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Silver: SCD Type 2 history of products. One row per version.
--
-- Incremental: per-entity rebuild. A product with any Bronze row ingested
-- after this table's watermark gets its whole history rebuilt (delete+insert
-- on product_id), so a late row lands in the right place in its timeline.
-- _max_ingested_at is the newest ingestion among the product's Bronze rows.

{% set changed %}
    product_id in (
        select product_id from {{ ref('stg_products') }}
        where ingested_at > (select max(_max_ingested_at) from {{ this }})
    )
{% endset %}

with versions as (

    {{ scd2_from_history(
        relation=ref('stg_products'),
        unique_key='product_id',
        surrogate_key='product_sk',
        tracked_columns=[
            'sku', 'product_name', 'category', 'unit_price', 'currency', 'is_active'
        ],
        where=changed if is_incremental() else none
    ) }}

),

watermark as (

    select product_id, max(ingested_at) as _max_ingested_at
    from {{ ref('stg_products') }}
    group by product_id

)

select versions.*, watermark._max_ingested_at
from versions
inner join watermark using (product_id)
