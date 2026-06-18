"""
Bronze layer: load raw TLC parquet files into DuckDB as-is.
No transformations - just gets the data in and prints row count.
"""
import duckdb
from config import RAW_DIR, WAREHOUSE


def run() -> int:
    src = str(RAW_DIR / "*.parquet")
    con = duckdb.connect(str(WAREHOUSE))
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze.yellow_trips AS
        SELECT *, regexp_extract(filename, 'yellow_tripdata_([0-9]{{4}}-[0-9]{{2}})', 1) AS source_month
        FROM read_parquet('{src}', union_by_name=true, filename=true);
    """)
    n = con.execute("SELECT COUNT(*) FROM bronze.yellow_trips").fetchone()[0]
    months = con.execute("SELECT COUNT(DISTINCT source_month) FROM bronze.yellow_trips").fetchone()[0]
    con.close()
    print(f"[bronze] loaded {n:,} rows from {months} month(s) into bronze.yellow_trips")
    return n


if __name__ == "__main__":
    run()
