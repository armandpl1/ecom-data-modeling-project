-- Gold must not expose raw personal data. Checks the warehouse's own column
-- list, so any Gold table (including future ones) that adds a raw
-- personal-data column fails the build. Raw values belong in
-- restricted.dim_customer_pii.
-- depends_on: {{ ref('dim_customer') }}
-- depends_on: {{ ref('dim_product') }}
-- depends_on: {{ ref('dim_date') }}
-- depends_on: {{ ref('fact_orders') }}
-- depends_on: {{ ref('fact_subscriptions') }}
-- depends_on: {{ ref('fact_subscription_mrr_monthly') }}

select
    table_name,
    column_name
from information_schema.columns
where
    table_schema = 'gold'
    and lower(column_name) in (
        'first_name', 'last_name', 'full_name', 'email', 'phone', 'phone_number',
        'phone_raw', 'phone_digits', 'phone_extension', 'street_address',
        'postal_code'
    )
