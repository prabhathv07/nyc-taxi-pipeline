-- payment method breakdown by month: trips, revenue, and share
with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),
labeled as (
    select
        source_month,
        case payment_type
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided trip'
        end                       as payment_method,
        total_amount
    from trips
)
select
    source_month,
    payment_method,
    count(*)                                                                   as trip_count,
    round(sum(total_amount), 2)                                                as total_revenue,
    round(100.0 * count(*) / sum(count(*)) over (partition by source_month), 2) as pct_of_month_trips
from labeled
group by source_month, payment_method
order by source_month, trip_count desc
