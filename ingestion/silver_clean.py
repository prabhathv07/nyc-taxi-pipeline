"""
Silver layer: PySpark cleaning job.

Reads raw TLC parquet, drops bad rows, typecasts columns, adds trip_duration_min,
and writes a clean parquet dataset to data/silver/. Also loads into DuckDB so
dbt can build the gold marts on top.

What gets dropped:
- fare_amount or total_amount <= 0
- trip_distance <= 0
- null pickup/dropoff timestamps
- dropoff before or equal to pickup
- trip longer than 24 hours
- passenger_count <= 0 or null
- payment_type not in 1-6
"""
from pyspark.sql import SparkSession, functions as F
from config import RAW_DIR, SILVER_DIR, WAREHOUSE


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("nyc_taxi_silver_clean")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def clean(spark: SparkSession):
    raw = spark.read.parquet(str(RAW_DIR / "*.parquet"))
    raw = raw.withColumn(
        "source_month",
        F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM"))
    total_in = raw.count()

    df = (
        raw
        .withColumn("trip_duration_min",
                    (F.unix_timestamp("tpep_dropoff_datetime")
                     - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn("PULocationID", F.col("PULocationID").cast("int"))
        .withColumn("DOLocationID", F.col("DOLocationID").cast("int"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
    )

    clean = df.filter(
        (F.col("fare_amount") > 0) & (F.col("total_amount") > 0)
        & (F.col("trip_distance") > 0)
        & F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (F.col("trip_duration_min") <= 1440)
        & (F.col("passenger_count") > 0)
        & (F.col("payment_type").between(1, 6))
    )
    total_out = clean.count()
    dropped = total_in - total_out

    (clean.repartition("source_month")
          .write.mode("overwrite").partitionBy("source_month")
          .parquet(str(SILVER_DIR / "yellow_trips_clean")))

    print(f"[silver] in={total_in:,}  out={total_out:,}  "
          f"dropped={dropped:,} ({100*dropped/total_in:.2f}%)")
    return total_in, total_out


def load_into_duckdb():
    import duckdb
    con = duckdb.connect(str(WAREHOUSE))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.yellow_trips AS
        SELECT * FROM read_parquet('{SILVER_DIR}/yellow_trips_clean/**/*.parquet',
                                   hive_partitioning=true);
    """)
    n = con.execute("SELECT COUNT(*) FROM silver.yellow_trips").fetchone()[0]
    con.close()
    print(f"[silver] loaded {n:,} clean rows into silver.yellow_trips")


if __name__ == "__main__":
    spark = build_spark()
    clean(spark)
    spark.stop()
    load_into_duckdb()
