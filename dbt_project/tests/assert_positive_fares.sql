-- fails if any non-positive fares made it through the silver cleaning
select fare_amount, total_amount
from {{ ref('stg_yellow_trips') }}
where fare_amount <= 0 or total_amount <= 0
