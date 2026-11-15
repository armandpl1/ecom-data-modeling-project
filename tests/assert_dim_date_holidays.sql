-- Every full year in dim_date has the 11 US federal holidays, Thanksgiving
-- is a Thursday between Nov 22 and 28, and Black Friday follows it.

with by_year as (

    select
        year,
        count(*) filter (where is_holiday)                                  as holidays,
        max(date_day) filter (where holiday_name = 'Thanksgiving Day')      as thanksgiving,
        max(date_day) filter (where retail_event_name = 'Black Friday')     as black_friday,
        count(*)                                                            as days
    from {{ ref('dim_date') }}
    group by year

)

select *
from by_year
where days >= 365
  and (
        holidays <> 11
     or isodow(thanksgiving) <> 4
     or day(thanksgiving) not between 22 and 28
     or black_friday <> thanksgiving + interval 1 day
  )
