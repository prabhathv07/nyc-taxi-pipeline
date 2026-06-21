"""
Build the small, deploy-friendly data bundle the dashboard reads.

The full warehouse is ~530 MB; the dashboard only needs compact aggregates, so
we roll the 5.4M clean trips up into a few thousand rows (month x borough x
payment x hour, plus a zone roll-up) and write them as parquet into
dashboard/data/. These files are what gets deployed to Streamlit Cloud / HF
Spaces — fast cold-start, no Spark/large warehouse needed at serve time.

Source: gold.stg_yellow_trips in the DuckDB warehouse (real Jan-Feb 2024 TLC).
Stray out-of-period rows (18 of 5.4M, with garbage 2002/2008/2009 timestamps)
are excluded here so the time-series views are clean.
"""
import os
import json
import duckdb

WH = os.environ.get("NYC_WAREHOUSE", "/tmp/nyc_taxi.duckdb")
OUT = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(OUT, exist_ok=True)
VALID = "('2024-01','2024-02')"

con = duckdb.connect(WH, read_only=True)

# 1) main cross-filterable aggregate: comes straight from the dbt-governed
#    gold.mart_trips_agg (month x borough x payment x hour), already scoped to
#    the loaded period and tested.
con.execute(f"""
    COPY (SELECT * FROM gold.mart_trips_agg)
    TO '{OUT}/agg_main.parquet' (FORMAT parquet);
""")

# 2) zone roll-up for the "top zones" view
con.execute(f"""
    COPY (
        SELECT
            source_month,
            coalesce(z.Borough,'Unknown') AS borough,
            coalesce(z.Zone,'Unknown')    AS zone,
            count(*)                      AS trips,
            sum(total_amount)             AS revenue
        FROM gold.stg_yellow_trips t
        LEFT JOIN gold.taxi_zone_lookup z ON t.pickup_location_id = z.LocationID
        WHERE source_month IN {VALID}
        GROUP BY 1,2,3
    ) TO '{OUT}/agg_zone.parquet' (FORMAT parquet);
""")

# 3) outlier summary
con.execute(f"""
    COPY (
        SELECT outlier_reason, count(*) AS n
        FROM gold.mart_duration_distance_outliers
        GROUP BY 1 ORDER BY 2 DESC
    ) TO '{OUT}/agg_outliers.parquet' (FORMAT parquet);
""")

# 4) pipeline meta (headline numbers for the KPI strip / banner)
b = con.execute("SELECT count(*) FROM bronze.yellow_trips").fetchone()[0]
s = con.execute("SELECT count(*) FROM silver.yellow_trips").fetchone()[0]
meta = {
    "data_source": "REAL — NYC TLC Yellow Taxi, Jan–Feb 2024",
    "bronze_rows": b, "silver_rows": s,
    "rows_dropped": b - s, "pct_dropped": round(100 * (b - s) / b, 2),
}
json.dump(meta, open(f"{OUT}/meta.json", "w"), indent=2)

rows = con.execute(f"SELECT count(*) FROM read_parquet('{OUT}/agg_main.parquet')").fetchone()[0]
zrows = con.execute(f"SELECT count(*) FROM read_parquet('{OUT}/agg_zone.parquet')").fetchone()[0]
con.close()
print(f"agg_main: {rows} rows  |  agg_zone: {zrows} rows")
print("meta:", json.dumps(meta))
print("bundle written to", OUT)
