with dates as (
    select distinct created_date_only as date_day
    from {{ ref('stg_sr_311') }}
    where created_date_only is not null
)

select
    date_day,
    extract(year  from date_day) as year,
    extract(month from date_day) as month,
    extract(day   from date_day) as day_of_month,
    dayofweek(date_day)          as day_of_week,
    to_char(date_day, 'Mon')     as month_name,
    dayofweek(date_day) in (0, 6) as is_weekend
from dates
