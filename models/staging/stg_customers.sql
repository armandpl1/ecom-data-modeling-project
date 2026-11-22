{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'updated_at'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Bronze: one row per customer change, cleaned and typed.
-- Exact duplicates and same-timestamp corrections are resolved by keeping
-- the row loaded last for each (customer_id, updated_at).

with source as (

    select * from {{ source('raw', 'customers') }}
    {% if is_incremental() %}
    -- Incremental: only rows from loads newer than anything already here
        where _ingested_at > (select max(existing.ingested_at) from {{ this }} as existing)
    {% endif %}

),

cleaned as (

    select
        trim(customer_id)                                as customer_id,
        nullif(trim(first_name), '')                     as first_name,
        nullif(trim(last_name), '')                      as last_name,
        nullif(lower(trim(email)), '')                   as email,

        -- Phones arrive in many formats: keep digits only, drop the US
        -- country code (1 or 001) and split off any extension.
        nullif(trim(phone), '')                          as phone_raw,
        nullif(regexp_replace(split_part(phone, 'x', 1), '[^0-9]', '', 'g'), '')
            as phone_digits,
        nullif(split_part(phone, 'x', 2), '')            as phone_extension,

        nullif(trim(street_address), '')                 as street_address,
        nullif(trim(city), '')                           as city,
        upper(nullif(trim(state), ''))                   as state,
        nullif(trim(postal_code), '')                    as postal_code,  -- text: keeps leading zeros
        upper(coalesce(nullif(trim(country), ''), 'US')) as country,
        lower(trim(customer_segment))                    as customer_segment,
        cast(marketing_opt_in as boolean)                as marketing_opt_in,

        cast(created_at as timestamp)                    as created_at,
        cast(updated_at as timestamp)                    as updated_at,
        _op                                              as source_operation,
        _op = 'D'                                        as is_deleted,
        cast(_loaded_at as timestamp)                    as loaded_at,
        _batch_id                                        as batch_id,
        _ingested_at                                     as ingested_at

    from source

),

standardized as (

    select
        * exclude (phone_digits),
        case
            when length(phone_digits) = 11 and phone_digits like '1%' then substr(phone_digits, 2)
            when length(phone_digits) = 13 and phone_digits like '001%' then substr(phone_digits, 4)
            else phone_digits
        end as phone_number
    from cleaned

),

candidates as (

    select * from standardized
    {% if is_incremental() %}
    -- Rows already here for the same (key, updated_at) compete with the new
    -- ones, so the row loaded last still wins across runs
        union all by name
        select * from {{ this }} as existing
        where exists (
            select 1 from standardized as new
            where new.customer_id = existing.customer_id and new.updated_at = existing.updated_at
        )
    {% endif %}

)

select *
from candidates
qualify row_number() over (
    partition by customer_id, updated_at
    order by loaded_at desc, ingested_at desc
) = 1
