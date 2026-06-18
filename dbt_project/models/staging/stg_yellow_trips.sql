-- staging view: rename columns, cast types, add derived fields
with src as (
    select * from {{ source('silver', 'yellow_trips') }}
)
select
    cast(VendorID as integer)                      as vendor_id,
    tpep_pickup_datetime                           as pickup_at,
    tpep_dropoff_datetime                          as dropoff_at,
    date_trunc('hour', tpep_pickup_datetime)       as pickup_hour_ts,
    extract(hour from tpep_pickup_datetime)        as pickup_hour,
    cast(source_month as varchar)                  as source_month,
    cast(passenger_count as integer)               as passenger_count,
    cast(trip_distance as double)                  as trip_distance,
    cast(trip_duration_min as double)              as trip_duration_min,
    cast(PULocationID as integer)                  as pickup_location_id,
    cast(DOLocationID as integer)                  as dropoff_location_id,
    cast(payment_type as integer)                  as payment_type,
    cast(fare_amount as double)                    as fare_amount,
    cast(tip_amount as double)                     as tip_amount,
    cast(total_amount as double)                   as total_amount,
    round(trip_distance / nullif(trip_duration_min / 60.0, 0), 1) as speed_mph
from src
