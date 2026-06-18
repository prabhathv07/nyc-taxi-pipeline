# Bronze table profiling

Basic profiling to figure out what needs cleaning in the silver layer.

```python
import duckdb
con = duckdb.connect("../warehouse/nyc_taxi.duckdb")

con.sql("""
  SELECT
    count(*)                                            AS rows,
    count(*) FILTER (WHERE fare_amount <= 0)            AS bad_fare,
    count(*) FILTER (WHERE trip_distance <= 0)          AS zero_distance,
    count(*) FILTER (WHERE tpep_dropoff_datetime IS NULL
                       OR tpep_dropoff_datetime <= tpep_pickup_datetime) AS bad_ts,
    count(*) FILTER (WHERE passenger_count <= 0)        AS bad_pax
  FROM bronze.yellow_trips
""").show()
```

What I found:
- ~2% of rows have negative or zero fare_amount / total_amount
- ~3% have trip_distance = 0
- ~1.5% have dropoff at or before pickup, ~0.5% have null dropoff
- ~1% have passenger_count = 0

Total ~7.8% of rows are junk and get dropped in silver. The rows that look
weird but aren't clearly wrong (high speed, very long trips) get kept and
surfaced in `mart_duration_distance_outliers` instead of silently dropped.
