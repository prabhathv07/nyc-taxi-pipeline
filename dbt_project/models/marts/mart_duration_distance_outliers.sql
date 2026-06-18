-- trips that passed cleaning but look suspicious - flagged for review
-- (very fast, very long, or long time with almost no distance)
with trips as (
    select * from {{ ref('stg_yellow_trips') }}
)
select
    source_month,
    pickup_at,
    trip_distance,
    trip_duration_min,
    speed_mph,
    fare_amount,
    case
        when speed_mph > 70                                  then 'implausible_speed'
        when trip_duration_min > 180                         then 'very_long_trip'
        when trip_distance > 50                              then 'very_long_distance'
        when trip_duration_min > 30 and trip_distance < 0.5  then 'long_time_no_distance'
    end as outlier_reason
from trips
where speed_mph > 70
   or trip_duration_min > 180
   or trip_distance > 50
   or (trip_duration_min > 30 and trip_distance < 0.5)
