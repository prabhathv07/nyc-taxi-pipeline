-- average fare and tip percentage by hour of day
with trips as (
    select * from {{ ref('stg_yellow_trips') }}
)
select
    pickup_hour,
    count(*)                                                   as trip_count,
    round(avg(fare_amount), 2)                                 as avg_fare,
    round(avg(tip_amount), 2)                                  as avg_tip,
    round(100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0), 2) as tip_pct_of_fare
from trips
group by pickup_hour
order by pickup_hour
