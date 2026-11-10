-- Gold: calendar dimension, one row per day.

with days as (

    select cast(range as date) as date_day
    from range(
        date '{{ var("date_spine_start") }}',
        date '{{ var("date_spine_end") }}' + interval 1 day,
        interval 1 day
    )

)

select
    cast(strftime(date_day, '%Y%m%d') as integer)   as date_key,
    date_day,
    year(date_day)                                  as year,
    quarter(date_day)                               as quarter,
    month(date_day)                                 as month,
    monthname(date_day)                             as month_name,
    cast(date_trunc('month', date_day) as date)     as month_start,
    cast(last_day(date_day) as date)                as month_end,
    weekofyear(date_day)                            as iso_week,
    isodow(date_day)                                as iso_day_of_week,
    dayname(date_day)                               as day_name,
    isodow(date_day) in (6, 7)                      as is_weekend
from days
