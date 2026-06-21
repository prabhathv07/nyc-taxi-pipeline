-- GOLD: compact cross-dimensional aggregate that powers the Streamlit dashboard.
-- month x borough x payment_method x pickup_hour -> trips/revenue/fare/tip/dist/dur.
-- Scoped to the loaded period (Jan-Feb 2024); this also drops the handful of
-- rows carrying garbage pickup timestamps (2002/2008/2009) that the upstream
-- filters can't catch by time-order alone.
with trips as (
    select * from {{ ref('stg_yellow_trips') }}
    where pickup_at >= timestamp '2024-01-01'
      and pickup_at <  timestamp '2024-03-01'
),
zones as (
    select * from {{ ref('taxi_zone_lookup') }}
)
select
    t.source_month,
    coalesce(z.Borough, 'Unknown')                  as borough,
    case t.payment_type
        when 1 then 'Credit card' when 2 then 'Cash'
        when 3 then 'No charge'   when 4 then 'Dispute'
        when 5 then 'Unknown'     when 6 then 'Voided trip'
        else 'Other' end                            as payment_method,
    t.pickup_hour,
    count(*)                                         as trips,
    round(sum(t.total_amount), 2)                    as revenue,
    round(sum(t.fare_amount), 2)                     as fare_sum,
    round(sum(t.tip_amount), 2)                      as tip_sum,
    round(sum(t.trip_distance), 2)                   as dist_sum,
    round(sum(t.trip_duration_min), 2)              as dur_sum
from trips t
left join zones z on t.pickup_location_id = z.LocationID
group by 1, 2, 3, 4
