-- Gold: customer dimension, SCD Type 2.
-- One row per customer version. Use is_current = true for today's view,
-- or join on customer_sk from a fact table for the view at event time.
-- A new version starts when any profile attribute or the subscription tier changes.
--
-- Personal data is masked here (macros/mask_pii.sql): no names, email, phone
-- or street address. The raw values, for the few who need them, are in
-- restricted.dim_customer_pii with the same customer_sk.

select
    customer_sk,
    customer_id,
    {{ mask_pii('first_name', 'initial') }} || ' '
    || {{ mask_pii('last_name', 'initial') }}     as initials,
    {{ mask_pii('email', 'email_hash') }}         as email_hash,
    {{ mask_pii('email', 'email_domain') }}       as email_domain,
    {{ mask_pii('phone_number', 'phone_last4') }} as phone_last4,
    city,
    state,
    {{ mask_pii('postal_code', 'zip3') }}         as zip3,
    country,
    customer_segment,
    marketing_opt_in,
    subscription_tier,
    created_at                                    as customer_created_at,

    version_number,
    valid_from_timestamp,
    valid_to_timestamp,
    is_current,
    deleted_at,
    deleted_at is not null                        as is_deleted_customer

from {{ ref('int_customers_history') }}
where not is_deleted
