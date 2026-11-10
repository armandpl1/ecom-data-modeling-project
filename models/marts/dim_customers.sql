-- Gold: customer dimension, SCD Type 2.
-- One row per customer version. Use is_current = true for today's view,
-- or join on customer_sk from a fact table for the view at event time.

select
    customer_sk,
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name     as full_name,
    email,
    phone_number,
    phone_extension,
    street_address,
    city,
    state,
    postal_code,
    country,
    customer_segment,
    marketing_opt_in,
    created_at                         as customer_created_at,

    version_number,
    valid_from,
    valid_to,
    is_current,
    deleted_at,
    deleted_at is not null             as is_deleted_customer

from {{ ref('int_customers_history') }}
where not is_deleted
