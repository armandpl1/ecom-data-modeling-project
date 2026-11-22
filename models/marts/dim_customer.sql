-- Gold: customer dimension, SCD Type 2.
-- One row per customer version. Use is_current = true for today's view,
-- or join on customer_sk from a fact table for the view at event time.
-- A new version starts when any profile attribute or the subscription tier changes.
--
-- Personal data is masked here (macros/mask_pii.sql): no names, email, phone
-- or street address. The raw values, for the few who need them, are in
-- restricted.dim_customer_pii with the same customer_sk.
--
-- The last row is the unknown member (customer_sk '-1'): facts whose customer
-- version isn't known yet point to it (macros/unknown_member_key.sql). Its
-- validity range is closed, so it is never current.

with known_versions as (

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

)

-- Both sides list every column in contract order, so the union lines up on
-- any engine (macros/cross_db.sql: contract_columns)
select
    {{ contract_columns() }}
from known_versions

union all

select
    {{ contract_columns({
        'customer_sk': unknown_member_key(),
        'customer_id': unknown_member_key(),
        'initials': "'Unknown'",
        'city': "'Unknown'",
        'state': "'Unknown'",
        'country': "'Unknown'",
        'customer_segment': "'Unknown'",
        'subscription_tier': "'unknown'",
        'version_number': 0,
        'valid_from_timestamp': "timestamp '1900-01-01 00:00:00'",
        'valid_to_timestamp': "timestamp '9999-12-31 00:00:00'",
        'is_current': false,
        'is_deleted_customer': false
    }) }}
