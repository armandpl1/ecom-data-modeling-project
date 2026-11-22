-- Restricted: raw personal data for each dim_customer version.
-- One row per customer version, the same rows and customer_sk as
-- gold.dim_customer, so the two join one to one.
--
-- It lives in its own schema, `restricted`, so access can be granted
-- separately. On Databricks every column here also gets a Unity Catalog
-- column mask (macros/unity_catalog.sql): only the pii_readers group sees
-- the values. DuckDB has no roles or masks.

{{ config(
    schema='restricted',
    materialized='table',
    post_hook="{{ apply_pii_masks(this) }}"
) }}

select
    customer_sk,
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    phone_number,
    phone_extension,
    street_address,
    postal_code

from {{ ref('int_customers_history') }}
where not is_deleted
