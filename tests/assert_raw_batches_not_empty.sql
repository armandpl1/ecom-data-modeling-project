-- Every loaded batch must contain rows, and the row count in the load log
-- must match what actually landed in the table. An empty or short batch
-- means a truncated extract.

with landed as (

    select
        _batch_id as batch_id,
        count(*)  as rows_landed
    from {{ source('raw', 'customers') }}
    group by 1
    union all
    select
        _batch_id as batch_id,
        count(*)  as rows_landed
    from {{ source('raw', 'products') }}
    group by 1
    union all
    select
        _batch_id as batch_id,
        count(*)  as rows_landed
    from {{ source('raw', 'orders') }}
    group by 1
    union all
    select
        _batch_id as batch_id,
        count(*)  as rows_landed
    from {{ source('raw', 'subscriptions') }}
    group by 1

)

select
    log.batch_id,
    log.table_name,
    log.row_count,
    coalesce(landed.rows_landed, 0) as rows_landed
from {{ source('raw', '_load_log') }} as log
left join landed on log.batch_id = landed.batch_id
where
    log.row_count = 0
    or log.row_count != coalesce(landed.rows_landed, 0)
