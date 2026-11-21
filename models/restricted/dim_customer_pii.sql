-- Restricted: raw personal data for each dim_customer version.
-- One row per customer version, the same rows and customer_sk as
-- gold.dim_customer, so the two join one to one.
--
-- It lives in its own schema, `restricted`, so access can be granted
-- separately. DuckDB has no roles; on Snowflake or BigQuery, use a role or
-- column masking policies instead of a separate schema.

{{ config(schema='restricted', materialized='table') }}

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
    postal_code

from {{ ref('int_customers_history') }}
where not is_deleted
