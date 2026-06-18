-- fails if passenger_count is outside 1-9 after cleaning
select passenger_count
from {{ ref('stg_yellow_trips') }}
where passenger_count < 1 or passenger_count > 9
