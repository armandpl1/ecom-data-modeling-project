-- Gold: calendar dimension, one row per day.
-- Includes calendar and fiscal attributes, US federal holidays (with the
-- observed day off), retail events, and rolling-window flags relative to
-- an as-of date (var as_of_date, or the last order date in the data).

{%- set fy_start = var('fiscal_year_start_month') | int %}

with as_of as (

    select
        {%- if var('as_of_date') %}
        date '{{ var("as_of_date") }}' as as_of_date
        {%- else %}
            (select max(cast(order_placed_at as date)) from {{ ref('stg_orders') }}) as as_of_date
        {%- endif %}

),

days as (

    select cast(range as date) as date_day
    from range(
        date '{{ var("date_spine_start") }}',
        date '{{ var("date_spine_end") }}' + interval 1 day,
        interval 1 day
    )

),

years as (

    -- One extra year either side, so observed dates that cross a year
    -- boundary (e.g. New Year's Day on a Saturday) are covered
    select range as year
    from range(
        year(date '{{ var("date_spine_start") }}') - 1,
        year(date '{{ var("date_spine_end") }}') + 2
    )

),

-- Fixed-date federal holidays, and the weekday they are observed on
fixed_holidays as (

    select
        make_date(years.year, h.month, h.day) as holiday_date,
        h.holiday_name
    from years
    cross join (
        values
        ('New Year''s Day', 1, 1),
        ('Juneteenth', 6, 19),
        ('Independence Day', 7, 4),
        ('Veterans Day', 11, 11),
        ('Christmas Day', 12, 25)
    ) as h (holiday_name, month, day)

),

-- Floating federal holidays: "nth weekday of month" or "last weekday of month"
floating_holidays as (

    select
        date_day as holiday_date,
        case
            when month(date_day) = 1 and isodow(date_day) = 1 and nth = 3 then 'Martin Luther King Jr. Day'
            when month(date_day) = 2 and isodow(date_day) = 1 and nth = 3 then 'Presidents'' Day'
            when month(date_day) = 5 and isodow(date_day) = 1 and is_last then 'Memorial Day'
            when month(date_day) = 9 and isodow(date_day) = 1 and nth = 1 then 'Labor Day'
            when month(date_day) = 10 and isodow(date_day) = 1 and nth = 2 then 'Columbus Day'
            when month(date_day) = 11 and isodow(date_day) = 4 and nth = 4 then 'Thanksgiving Day'
        end      as holiday_name
    from (
        select
            date_day,
            (day(date_day) - 1) // 7 + 1                        as nth,
            month(date_day + interval 7 day) <> month(date_day) as is_last
        from days
    ) as weekday_positions

),

federal_holidays as (

    select
        holiday_date,
        holiday_name
    from fixed_holidays
    union all
    select
        holiday_date,
        holiday_name
    from floating_holidays
    where holiday_name is not null

),

observed_holidays as (

    -- Saturday holidays are observed on Friday, Sunday holidays on Monday
    select
        cast(case isodow(holiday_date)
            when 6 then holiday_date - interval 1 day
            when 7 then holiday_date + interval 1 day
            else holiday_date
        end as date) as observed_date,
        holiday_name
    from federal_holidays

),

retail_events as (

    select
        holiday_date + interval 1 day as event_date,
        'Black Friday'                as event_name
    from federal_holidays
    where holiday_name = 'Thanksgiving Day'
    union all
    select
        holiday_date + interval 4 day as event_date,
        'Cyber Monday'                as event_name
    from federal_holidays
    where holiday_name = 'Thanksgiving Day'

),

calendar as (

    select
        days.date_day,
        as_of.as_of_date,

        -- Fiscal month 1 is the month named by fiscal_year_start_month
        ((month(days.date_day) - {{ fy_start }} + 12) % 12) + 1 as fiscal_month,
        year(days.date_day)
        + case
            when {{ fy_start }} > 1 and month(days.date_day) >= {{ fy_start }}
                then 1
            else 0
        end                                                     as fiscal_year

    from days
    cross join as_of

)

select
    -- Keys
    cast(strftime(c.date_day, '%Y%m%d') as integer)        as date_key,
    c.date_day,

    -- Calendar
    year(c.date_day)                                       as year,
    quarter(c.date_day)                                    as quarter,
    'Q' || quarter(c.date_day)                             as quarter_name,
    month(c.date_day)                                      as month,
    monthname(c.date_day)                                  as month_name,
    strftime(c.date_day, '%Y-%m')                          as year_month,
    day(c.date_day)                                        as day_of_month,
    dayofyear(c.date_day)                                  as day_of_year,
    isodow(c.date_day)                                     as iso_day_of_week,
    dayname(c.date_day)                                    as day_name,
    weekofyear(c.date_day)                                 as iso_week,
    cast(date_trunc('week', c.date_day) as date)           as week_start,
    cast(date_trunc('month', c.date_day) as date)          as month_start,
    cast(last_day(c.date_day) as date)                     as month_end,
    cast(date_trunc('quarter', c.date_day) as date)        as quarter_start,
    c.date_day = last_day(c.date_day)                      as is_month_end,
    c.date_day = cast(last_day(date_trunc('quarter', c.date_day) + interval 2 month) as date)
        as is_quarter_end,

    -- Fiscal calendar
    c.fiscal_year,
    (c.fiscal_month - 1) // 3 + 1                          as fiscal_quarter,
    c.fiscal_month,
    'FY' || c.fiscal_year                                  as fiscal_year_name,
    'FY' || c.fiscal_year || '-Q' || ((c.fiscal_month - 1) // 3 + 1)
        as fiscal_quarter_name,

    -- Weekends, holidays and retail events
    isodow(c.date_day) in (6, 7)                           as is_weekend,
    fh.holiday_name,
    fh.holiday_name is not null                            as is_holiday,
    oh.holiday_name                                        as observed_holiday_name,
    oh.holiday_name is not null                            as is_observed_holiday,
    isodow(c.date_day) not in (6, 7) and oh.holiday_name is null
        as is_business_day,
    re.event_name                                          as retail_event_name,
    re.event_name is not null                              as is_retail_event,

    -- Rolling windows relative to as_of_date
    c.as_of_date,
    date_diff('day', c.as_of_date, c.date_day)             as days_from_as_of,
    date_diff('month', date_trunc('month', c.as_of_date), date_trunc('month', c.date_day))
        as months_from_as_of,
    c.date_day > c.as_of_date                              as is_future,
    c.date_day between c.as_of_date - 6 and c.as_of_date   as is_last_7_days,
    c.date_day between c.as_of_date - 29 and c.as_of_date  as is_last_30_days,
    c.date_day between c.as_of_date - 89 and c.as_of_date  as is_last_90_days,
    c.date_day between c.as_of_date - 364 and c.as_of_date as is_last_365_days,
    date_trunc('month', c.date_day) = date_trunc('month', c.as_of_date)
        as is_current_month,
    date_trunc('month', c.date_day) = date_trunc('month', c.as_of_date) - interval 1 month
        as is_previous_month,
    date_trunc('month', c.date_day) = date_trunc('month', c.as_of_date)
    and c.date_day <= c.as_of_date                         as is_month_to_date,
    year(c.date_day) = year(c.as_of_date)
    and c.date_day <= c.as_of_date                         as is_year_to_date,
    year(c.date_day) = year(c.as_of_date) - 1
    and c.date_day <= c.as_of_date - interval 1 year       as is_prior_year_to_date,
    c.fiscal_year = (
        select as_of.fiscal_year from calendar as as_of
        where as_of.date_day = c.as_of_date
    )
    and c.date_day <= c.as_of_date                         as is_fiscal_year_to_date

from calendar as c
left join federal_holidays as fh on c.date_day = fh.holiday_date
left join observed_holidays as oh on c.date_day = oh.observed_date
left join retail_events as re on c.date_day = re.event_date
