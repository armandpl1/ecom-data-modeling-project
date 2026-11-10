-- Every SCD2 version must have a positive validity period, and each record
-- can have at most one open-ended (current) version.

with versions as (

    select 'customers' as dim, customer_id as record_id, valid_from, valid_to
    from {{ ref('dim_customers') }}
    union all
    select 'products', product_id, valid_from, valid_to
    from {{ ref('dim_products') }}

),

bad_periods as (

    select dim, record_id, 'valid_to is not after valid_from' as problem
    from versions
    where valid_to <= valid_from

),

multiple_current as (

    select dim, record_id, 'more than one open version' as problem
    from versions
    where valid_to is null
    group by dim, record_id
    having count(*) > 1

)

select * from bad_periods
union all
select * from multiple_current
