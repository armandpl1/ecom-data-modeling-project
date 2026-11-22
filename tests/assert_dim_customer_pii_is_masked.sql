-- The masked columns in gold.dim_customer hold masked values only:
-- a 64-character hex hash, 4 phone digits, a domain without @ and a 3-digit
-- ZIP prefix. Catches a masking macro that silently passes raw data through.

select
    customer_sk,
    email_hash,
    phone_last4,
    email_domain,
    zip3
from {{ ref('dim_customer') }}
where
    (email_hash is not null and not {{ regexp_full_match('email_hash', "'[0-9a-f]{64}'") }})
    or (phone_last4 is not null and not {{ regexp_full_match('phone_last4', "'[0-9]{4}'") }})
    or email_domain like '%@%'
    or length(zip3) > 3
