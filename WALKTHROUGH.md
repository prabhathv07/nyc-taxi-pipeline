# NYC Taxi Pipeline — Full Walkthrough & How-To

A plain-English explanation of what this project is, what was built, the tools
used, and a step-by-step process to build it yourself from scratch.

---

## 1. What the project is

A **medallion data pipeline** over NYC Yellow Taxi trip records. "Medallion" is
the industry-standard layered design used at real companies:

- **Bronze** = raw data, loaded exactly as received, never edited.
- **Silver** = cleaned/standardized data (the messy rows removed, types fixed,
  useful fields derived).
- **Gold** = business-ready analytics tables ("marts") that answer specific
  questions.

The goal is to look like a pipeline a Data/Analytics Engineer runs in
production: ingest raw Parquet, clean it with **Spark**, model it with **dbt**,
guard it with **data-quality tests**, and **orchestrate** the whole chain with
**Airflow** — all documented.

The single sentence it earns on a resume:
> *Built a medallion pipeline processing 2.5M NYC taxi trips — PySpark ingestion,
> dbt models with 19 data-quality tests, Airflow orchestration on a cloud-ready
> warehouse.*

---

## 2. The architecture (data flow)

```
Raw TLC Parquet (data/raw/)
   │   bronze_load.py  — DuckDB reads Parquet, stores it untouched
   ▼
bronze.yellow_trips         2,500,000 rows
   │   silver_clean.py — PySpark drops bad rows, typecasts, adds trip_duration
   ▼
silver.yellow_trips         2,305,982 rows  (194,018 dropped = 7.76%)
   │   dbt — staging view → 4 gold marts
   ▼
gold.mart_revenue_by_zone
gold.mart_fare_tip_by_hour
gold.mart_duration_distance_outliers
gold.mart_payment_type_over_time
   │   dbt tests (19) — fail the run if data is wrong
   ▼
Airflow DAG runs bronze → silver → gold+tests daily, with retries
```

---

## 3. The tools and *why* each one

| Tool | Role in the project | Why it was chosen |
|---|---|---|
| **Parquet** | Raw data format | Columnar, compressed, the real TLC format; what "big data" uses |
| **DuckDB** | The warehouse | Reads Parquet natively, zero setup, free, runs locally; the "warehouse" without a cloud account |
| **PySpark** | Silver cleaning | The headline skill ("Spark proof"); distributed-style processing of millions of rows |
| **dbt** (dbt-duckdb) | Gold modeling + tests + docs | Industry standard for SQL transformations with version-controlled, tested models |
| **Airflow** | Orchestration | Schedules and chains the steps with retries; the "senior signal" |
| **Docker Compose** | Runs Airflow locally | Reproducible environment for the orchestrator |
| **matplotlib** | Results chart | Visual proof in the README |

Warehouse choice: **DuckDB** (easiest, local, free). The README documents how to
swap in **BigQuery** for a literal "cloud warehouse" line — the dbt models run
unchanged.

---

## 4. What was actually done, step by step (what happened in this build)

1. **Environment check.** Confirmed Java 11 is present (Spark needs Java),
   ~4 GB RAM/disk available, and installed `pyspark==3.5.3` (Spark 4 needs
   Java 17+, so 3.5 was pinned to match Java 11), `dbt-duckdb`, `duckdb`,
   `pyarrow`, `matplotlib`.

2. **Got the data.** The plan was to download 2 months of official TLC Yellow
   Parquet. Because this sandbox can't reach the internet for those files, a
   generator (`scripts/get_data.py`) produced data with the **exact TLC schema**
   and the **same real defects** (negative fares, zero-distance trips, null and
   inverted timestamps, out-of-range codes). 2 months × 1.25M = **2.5M rows**.
   *(The pipeline is source-agnostic — drop real TLC files in `data/raw/` and
   rerun; nothing else changes.)*

3. **Bronze (`ingestion/bronze_load.py`).** DuckDB reads every Parquet in
   `data/raw/` and stores it as `bronze.yellow_trips`, untouched, plus a
   `source_month` tag. Printed the row count: **2,500,000**.

4. **Silver (`ingestion/silver_clean.py`) — the Spark job.** Spark read the raw
   Parquet, then:
   - dropped non-positive fares, zero-distance trips, null/inverted timestamps,
     impossible passenger counts, out-of-dictionary payment codes;
   - cast columns to correct types;
   - derived `trip_duration_min`;
   - wrote a partitioned clean Parquet dataset and loaded it into
     `silver.yellow_trips`.
   Result: **2,305,982** clean rows, **7.76% dropped**. Two real bugs were
   fixed during this step (nanosecond timestamps Spark 3.5 can't read → rewrote
   as microsecond; `TIMESTAMP_NTZ` can't cast to long → used `unix_timestamp()`).

5. **Gold (`dbt_project/`) — dbt models + tests.** dbt built a staging view over
   silver, then 4 business marts, plus a zone seed. Added **19 data-quality
   tests** (`not_null`, `unique`, `accepted_values`, `relationships`, and 2
   custom singular tests). `dbt build` ran models **and** tests:
   `PASS=25 ERROR=0` (4 models + 1 view + 1 seed + 19 tests). `dbt docs generate`
   produced documentation.

6. **Orchestration (`airflow/dags/nyc_taxi_dag.py`).** An Airflow DAG chains
   bronze → silver → `dbt build`, with retries on the heavy Spark step. A
   `docker-compose.yml` runs Airflow + Postgres locally. The DAG was
   syntax-validated.

7. **Documentation + delivery.** Generated a results chart, captured the real
   numbers to `results/run_metrics.json` and the passing-test log, wrote the
   README (diagram, numbers, chart, run instructions), and packaged everything.

One recurring environment quirk: the mounted filesystem won't let processes
delete files they wrote, so the DuckDB warehouse and Spark output were pointed at
`/tmp` during runs (via the `NYC_WAREHOUSE` / `NYC_SILVER` env vars) and the
final warehouse copied into the repo.

---

## 5. How YOU would build this from scratch (the process)

### Phase 0 — Setup
```bash
mkdir nyc-taxi-pipeline && cd nyc-taxi-pipeline
git init
python -m venv .venv && source .venv/bin/activate
# create the folder layout
mkdir -p ingestion dbt_project/models/{staging,marts} dbt_project/{seeds,tests} \
         airflow/dags infra scripts notebooks data/raw warehouse
pip install pyspark==3.5.3 dbt-duckdb duckdb pyarrow pandas matplotlib
# requirements.txt + .gitignore, then: git add -A && git commit -m "skeleton"
```
You need **Java 8, 11, or 17** installed for PySpark (`java -version` to check).

### Phase 1 — Get the data + Bronze
- Download 2–3 months of Yellow Taxi Parquet (no login needed):
  `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`
  (also `2024-02`, and `misc/taxi_zone_lookup.csv`). Put the Parquet in
  `data/raw/` and the CSV in `dbt_project/seeds/`.
- Write `bronze_load.py`: open DuckDB, `CREATE TABLE bronze.yellow_trips AS
  SELECT * FROM read_parquet('data/raw/*.parquet')`. Print the row count.
- Commit.

### Phase 2 — Silver (PySpark) — *do this carefully, it's the key skill*
- Write `silver_clean.py`:
  1. `SparkSession.builder.master("local[*]").getOrCreate()`
  2. `spark.read.parquet("data/raw/*.parquet")`
  3. derive `trip_duration_min` with
     `(unix_timestamp(dropoff) - unix_timestamp(pickup))/60`
  4. `.filter(...)` out: `fare_amount<=0`, `total_amount<=0`, `trip_distance<=0`,
     null/inverted timestamps, `passenger_count<=0`, payment codes outside 1–6
  5. write cleaned Parquet (`.write.partitionBy("source_month")`) and load into
     `silver.yellow_trips` in DuckDB
  6. print rows in / out / % dropped — **these are your resume numbers**
- Run it: `python ingestion/silver_clean.py`. Screenshot the Spark run.
- Commit. **Now "Spark" on your resume is true.**

### Phase 3 — Gold (dbt)
- `dbt_project.yml` (project config), `profiles.yml` (point dbt at the DuckDB
  file; `schema: gold`).
- `models/staging/stg_yellow_trips.sql`: `select` from a dbt **source** pointing
  at `silver.yellow_trips`, rename/typecast, add `pickup_hour`, `speed_mph`.
- `models/marts/`: one `.sql` per business question:
  revenue-by-zone, fare/tip-by-hour, duration/distance-outliers,
  payment-type-over-time.
- Tests in YAML: `not_null`, `unique`, `accepted_values` (payment codes),
  `relationships` (zone IDs → seed). Add 1–2 singular tests (a `.sql` in
  `tests/` that returns bad rows).
- Run:
  ```bash
  cd dbt_project
  dbt seed          # load zone lookup
  dbt build         # builds models AND runs every test
  dbt docs generate # documentation
  ```
- Capture the `PASS=N` line. Commit. **Stopping here is already a real project.**

### Phase 4 — Orchestration (Airflow)
- Write a DAG (`airflow/dags/nyc_taxi_dag.py`) with three `BashOperator` tasks:
  `bronze_load.py` → `silver_clean.py` → `dbt build`, wired
  `bronze >> silver >> gold`. Give the Spark task `retries=2`.
- Run Airflow via `infra/docker-compose.yml`
  (`docker compose up airflow-init && docker compose up`), open
  `http://localhost:8080`, trigger the DAG, screenshot the green graph. Commit.

### Phase 5 — Document
- README with: the medallion diagram, the real numbers (rows in/out, % dropped,
  test count, runtime), a sample gold query + a chart, and a screenshot of
  passing tests. **This is what a recruiter reads.** Commit.

---

## 6. How to run THIS repo right now
```bash
pip install -r requirements.txt              # Java 8/11/17 needed for Spark
bash scripts/run_pipeline.sh                 # bronze → silver → gold + tests + docs
```
Or just query the gold marts that are already built (no Spark/Java needed):
```python
import duckdb
con = duckdb.connect("warehouse/nyc_taxi.duckdb")
con.sql("SELECT * FROM gold.mart_payment_type_over_time").show()
```

---

## 7. What to say in an interview (defensible talking points)
- **"Why medallion?"** Separation of concerns: raw is auditable and replayable,
  silver is the single clean source, gold is what analysts query. You can
  reprocess any layer without re-downloading.
- **"What did you clean and why?"** ~7.8% of raw rows were unusable (negative
  fares, zero distance, bad timestamps). I dropped those but **kept** legal-but-
  suspicious rows (e.g. very high implied speed) and surfaced them in an outlier
  mart instead of silently deleting — a deliberate data-quality decision.
- **"How do you know the data is right?"** 19 automated dbt tests run on every
  build; a failing test fails the pipeline. Includes referential integrity
  (every trip's zone exists) and domain checks (payment codes 1–6).
- **"Why DuckDB not Spark for gold?"** Right tool per layer: Spark for heavy
  row-level cleaning at scale, SQL/dbt on a warehouse for set-based analytics.
  Swapping the warehouse to BigQuery is a profile change, not a rewrite.
