-- revenue and trip count per pickup zone, joined to the zone lookup
with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),
zones as (
    select * from {{ ref('taxi_zone_lookup') }}
)
select
    t.pickup_location_id,
    z.Borough                              as borough,
    z.Zone                                 as zone,
    count(*)                               as trip_count,
    round(sum(t.total_amount), 2)          as total_revenue,
    round(avg(t.fare_amount), 2)           as avg_fare,
    round(avg(t.trip_distance), 2)         as avg_distance_mi
from trips t
left join zones z on t.pickup_location_id = z.LocationID
group by 1, 2, 3
order by total_revenue desc
