# NYC Taxi Medallion Pipeline — PySpark, dbt, DuckDB & Airflow

[![CI](https://github.com/prabhathv07/nyc-taxi-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/prabhathv07/nyc-taxi-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?logo=dbt&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-1.5-FFF000?logo=duckdb&logoColor=black)
![Airflow](https://img.shields.io/badge/Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A full-stack data engineering pipeline over **5,972,150 real NYC TLC Yellow Taxi trips (Jan–Feb 2024)**, built on the medallion architecture: raw Parquet → PySpark cleaning → dbt gold analytics marts → 19 data-quality tests → daily Airflow orchestration, all on a DuckDB warehouse.

All numbers below come from a verified run on the official [NYC TLC trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — not synthetic data.

---

## TL;DR

- **5,972,150** real TLC trips ingested across Jan–Feb 2024 into `bronze.yellow_trips` (DuckDB)
- PySpark silver job dropped **528,764 bad rows (8.85%)** — bad fares, zero-distance, null/inverted timestamps, impossible occupancy — leaving **5,443,386 clean trips**
- dbt built **4 gold marts** and ran **19 data-quality tests — all passing** on real data
- Manhattan accounts for **75% of revenue ($111.9M of $149.1M total)** — the borough split confirms yellow taxis are overwhelmingly a Manhattan product
- Credit card generates **$128.3M (86%)** of total revenue; cash accounts for just **$19.3M (13%)**
- Airport-run hours (4–6 AM) have the **highest average fares ($23–$28)** and lowest tip rates; evening hours (5–9 PM) flip to the highest tip rates (20%+) with lower individual fares
- Daily Airflow DAG chains bronze → silver → gold+tests with retries on the PySpark step
- CI runs the full pipeline end-to-end on every push

---

## Architecture

```
NYC TLC Yellow Parquet (data/raw/)
            │
            ▼
┌───────────────────────────────────────────────────────────┐
│  BRONZE  ingestion/bronze_load.py                         │
│                                                           │
│  · read_parquet() with union_by_name (handles schema      │
│    drift across monthly TLC files)                        │
│  · extract source_month from filename via regexp          │
│  · CREATE OR REPLACE TABLE bronze.yellow_trips            │
│  · no transformations — raw TLC schema preserved          │
└────────────────────────┬──────────────────────────────────┘
                         │  5,972,150 rows
                         ▼
┌───────────────────────────────────────────────────────────┐
│  SILVER  ingestion/silver_clean.py  (PySpark 3.5)         │
│                                                           │
│  · SparkSession local[*], 2 GB driver                     │
│  · typecast 7 columns (passenger_count, payment_type,     │
│    PULocationID, DOLocationID, fare/total/distance)       │
│  · derive trip_duration_min from unix timestamps          │
│  · filter: 7 documented cleaning rules (see table below)  │
│  · write partitioned parquet → data/silver/               │
│  · load into silver.yellow_trips (DuckDB)                 │
└────────────────────────┬──────────────────────────────────┘
                         │  5,443,386 clean rows (528,764 dropped, 8.85%)
                         ▼
┌───────────────────────────────────────────────────────────┐
│  STAGING  dbt_project/models/staging/stg_yellow_trips     │
│                                                           │
│  · rename raw TLC columns to clean snake_case names       │
│  · add pickup_hour, pickup_hour_ts, speed_mph             │
│  · materialized as view (no data copy)                    │
└────────────────────────┬──────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┬─────────────────┐
          ▼              ▼              ▼                  ▼
┌──────────────┐ ┌─────────────┐ ┌──────────────┐ ┌────────────────┐
│ mart_revenue │ │ mart_fare_  │ │ mart_payment │ │ mart_duration_ │
│ _by_zone     │ │ tip_by_hour │ │ _type_over_  │ │ distance_      │
│              │ │             │ │ time         │ │ outliers       │
│ revenue +    │ │ avg fare    │ │ trips,revenue│ │ suspicious but │
│ trips per    │ │ tip % by    │ │ share by     │ │ legal trips    │
│ zone/borough │ │ hour of day │ │ method/month │ │ flagged for ops│
└──────┬───────┘ └──────┬──────┘ └──────┬───────┘ └───────┬────────┘
       │                │               │                  │
       └────────────────┴───────────────┴──────────────────┘
                                        │
                                        ▼
                         ┌──────────────────────────┐
                         │  dbt build — 19 tests     │
                         │                           │
                         │  not_null       ×10       │
                         │  accepted_values ×4       │
                         │  unique          ×2       │
                         │  relationships   ×1       │
                         │  singular tests  ×2       │
                         └──────────────┬────────────┘
                                        │
                                        ▼
                         ┌──────────────────────────┐
                         │  Airflow DAG (daily)      │
                         │  airflow/dags/            │
                         │  nyc_taxi_dag.py          │
                         │                           │
                         │  bronze_load              │
                         │      → silver_clean (×2   │
                         │        retries)           │
                         │      → gold_dbt_build     │
                         │        (no retry)         │
                         └──────────────────────────┘
```

---

## Approach

### Bronze — Raw Load (`ingestion/bronze_load.py`)

The bronze layer loads every raw TLC parquet file into DuckDB with zero transformations — the goal is to preserve the original schema exactly so any downstream issue can always be traced back to the source.

`read_parquet()` is called with `union_by_name=true` to handle the fact that TLC monthly files occasionally add or reorder columns, and `filename=true` to extract `source_month` from the file path with a regexp, so every row knows which month it came from without relying on the timestamp.

The table is created with `CREATE OR REPLACE TABLE` to make the step idempotent.

### Silver — PySpark Cleaning (`ingestion/silver_clean.py`)

The silver job reads the raw parquet files with Spark, derives `trip_duration_min` from the unix-timestamp difference, typecasts seven columns to eliminate implicit type ambiguity in downstream aggregations, and applies seven filters that each target a documented defect in the real TLC feed.

**Cleaning rules applied (verified on real Jan–Feb 2024 TLC data):**

| Rule | What it catches | Notes |
|------|----------------|-------|
| `fare_amount ≤ 0` or `total_amount ≤ 0` | Refunded / negative-fare records | Real TLC defect; ~2% of raw rows |
| `trip_distance ≤ 0` | Meter-on / door-slam / data-entry zero trips | Most common defect; ~3% of raw rows |
| `tpep_pickup_datetime IS NULL` or `tpep_dropoff_datetime IS NULL` | Missing timestamps | ~0.5% of raw rows |
| `tpep_dropoff_datetime ≤ tpep_pickup_datetime` | Inverted or simultaneous timestamps | ~1.5% of raw rows |
| `trip_duration_min > 1440` | Runaway meter (trip > 24 h) | Rare but present in real data |
| `passenger_count ≤ 0` or `NULL` | Impossible occupancy | ~1% of raw rows |
| `payment_type NOT IN (1–6)` | Out-of-dictionary TLC payment codes | Rare; real data contains stray codes |
| **Total dropped** | | **528,764 rows — 8.85% of 5,972,150** |

After filtering, the clean dataset is written to `data/silver/` partitioned by `source_month`, then bulk-loaded into `silver.yellow_trips` in DuckDB so dbt can build on top of it.

Rows that look suspicious but are not clearly impossible (high implied speed, very long trips) are **intentionally kept** and surfaced in `mart_duration_distance_outliers` rather than silently dropped.

### Gold — dbt Marts (`dbt_project/models/marts/`)

Four business-facing gold marts are built as tables on top of the `stg_yellow_trips` staging view.

| Mart | Business question | Key SQL technique |
|------|------------------|-------------------|
| `mart_revenue_by_zone` | Revenue + trip count per pickup zone/borough | `LEFT JOIN` to `taxi_zone_lookup` seed |
| `mart_fare_tip_by_hour` | Average fare and tip % by hour of day | `nullif()` guard on `SUM(fare_amount)` |
| `mart_payment_type_over_time` | Payment method trips/revenue/share per month | Window `SUM() OVER (PARTITION BY source_month)` for share % |
| `mart_duration_distance_outliers` | Suspicious-but-legal trips flagged for ops review | Multi-condition `CASE` for `outlier_reason` |

### Data-Quality Tests

dbt runs 19 tests across the staging and gold layers on every `dbt build`.

| Test type | Count | What it covers |
|-----------|------:|----------------|
| `not_null` | 10 | `pickup_at`, `fare_amount`, `passenger_count`, `payment_type`, `pickup_location_id` in staging; `outlier_reason`, `pickup_hour`, `payment_method`, `pickup_location_id`, `total_revenue` in marts |
| `accepted_values` | 4 | Payment codes 1–6 in staging; payment method strings in `mart_payment_type_over_time`; hours 0–23 in `mart_fare_tip_by_hour`; outlier reason strings in `mart_duration_distance_outliers` |
| `unique` | 2 | `pickup_hour` in `mart_fare_tip_by_hour`; `pickup_location_id` in `mart_revenue_by_zone` |
| `relationships` | 1 | `pickup_location_id` → `taxi_zone_lookup.LocationID` (referential integrity to real TLC zone seed) |
| `singular` | 2 | `assert_positive_fares` (no non-positive fares in staging); `assert_passenger_count_in_range` (occupancy 1–9) |
| **Total** | **19** | All passing on real Jan–Feb 2024 data |

---

## Results

All numbers are from a verified run on the official TLC parquet files.

### Pipeline Run

| Stage | Metric | Value |
|-------|--------|-------|
| Source | TLC Yellow Taxi months | Jan 2024, Feb 2024 |
| Bronze | Rows loaded | 5,972,150 |
| Silver | Rows dropped | 528,764 |
| Silver | Drop rate | 8.85% |
| Silver | Rows after cleaning | 5,443,386 |
| Gold | Marts built | 4 |
| Tests | dbt tests run | 19 |
| Tests | Passing | 19 |
| Tests | Failing | 0 |

### Gold Mart: Revenue by Payment Method

| Payment Method | Total Revenue | Trips | Share of Revenue |
|----------------|-------------:|------:|-----------------:|
| Credit card | $128,257,944 | 4,568,631 | 86.0% |
| Cash | $19,277,688 | 809,464 | 12.9% |
| Dispute | $1,155,111 | 45,775 | 0.8% |
| No charge | $444,196 | 19,516 | 0.3% |
| **Total** | **$149,134,939** | **5,443,386** | |

Credit card dominates at 86% of revenue. Cash trips average a lower total fare, consistent with shorter inner-city trips where cash is still preferred.

### Gold Mart: Revenue by Pickup Borough

| Borough | Total Revenue | Trips | Share |
|---------|-------------:|------:|------:|
| Manhattan | $111,902,187 | 4,898,037 | 75.0% |
| Queens | $35,151,077 | 482,159 | 23.6% |
| Brooklyn | $1,136,955 | 34,880 | 0.8% |
| Unknown | $486,715 | 17,464 | 0.3% |
| Bronx | $359,678 | 9,725 | 0.2% |
| Others | $98,327 | 1,121 | 0.1% |

Manhattan accounts for 75% of all yellow-taxi revenue — yellow taxis are overwhelmingly a Manhattan product. Queens's 23.6% is largely driven by JFK and LaGuardia airport trips, which command significantly higher fares.

### Gold Mart: Fare & Tip by Hour of Day

| Hour | Trips | Avg Fare | Avg Tip | Tip % of Fare |
|-----:|------:|---------:|--------:|--------------:|
| 0 (midnight) | 138,607 | $19.03 | $3.54 | 18.6% |
| 4 AM | 25,757 | $23.30 | $3.66 | 15.7% |
| 5 AM | 30,163 | $27.71 | $4.15 | 15.0% |
| 6 AM | 70,456 | $21.93 | $3.40 | 15.5% |
| 8 AM | 210,727 | $17.40 | $3.18 | 18.3% |
| 12 PM | 300,509 | $18.00 | $3.26 | 18.1% |
| 5 PM | 381,223 | $18.09 | $3.64 | **20.1%** |
| 6 PM | **398,100** | $17.01 | $3.52 | **20.7%** |
| 9 PM | 305,325 | $18.13 | $3.66 | 20.2% |

Key patterns from real data:
- **4–6 AM airport fares are 37–63% higher** than the daytime average ($23–28 vs. $17) — these are JFK/LaGuardia runs
- **Evening rush (5–9 PM) has the highest tip rates (20%+)** despite lower average fares — tipping behavior changes after work hours
- **6 PM is the single busiest hour** (398,100 trips), 15× more trips than the 4 AM trough (25,757)
- **Quietest hours (3–5 AM)** have the smallest trip counts but the highest average fares

### Gold Mart: Duration/Distance Outliers

Trips that passed all cleaning rules but are operationally suspicious:

| Outlier reason | Count | Condition | Interpretation |
|----------------|------:|-----------|----------------|
| `very_long_trip` | 3,826 | duration > 3 h | Meter left running; traffic delay; fare dispute |
| `implausible_speed` | 1,769 | speed > 70 mph | GPS/timestamp error or mis-geocoded highway trip |
| `very_long_distance` | 601 | distance > 50 mi | Airport outer-borough runs; not wrong but worth auditing |
| `long_time_no_distance` | 368 | >30 min, <0.5 mi | Traffic standstill or meter left on while parked |
| **Total flagged** | **6,564** | | 0.12% of clean trips |

### dbt Test Results

```
 3 of 25 PASS accepted_values_stg_yellow_trips_payment_type               [0.04s]
 4 of 25 PASS assert_passenger_count_in_range                             [0.04s]
 5 of 25 PASS assert_positive_fares                                       [0.04s]
 6 of 25 PASS not_null_stg_yellow_trips_fare_amount                       [0.04s]
 7 of 25 PASS not_null_stg_yellow_trips_passenger_count                   [0.02s]
 8 of 25 PASS not_null_stg_yellow_trips_payment_type                      [0.02s]
 9 of 25 PASS not_null_stg_yellow_trips_pickup_at                         [0.02s]
10 of 25 PASS not_null_stg_yellow_trips_pickup_location_id                [0.02s]
11 of 25 PASS relationships_stg_yellow_trips_pickup_location_id           [0.09s]
16 of 25 PASS accepted_values_mart_duration_distance_outliers_outlier_reason [0.04s]
17 of 25 PASS not_null_mart_duration_distance_outliers_outlier_reason     [0.02s]
18 of 25 PASS accepted_values_mart_fare_tip_by_hour_pickup_hour           [0.02s]
19 of 25 PASS not_null_mart_fare_tip_by_hour_pickup_hour                  [0.02s]
20 of 25 PASS unique_mart_fare_tip_by_hour_pickup_hour                    [0.03s]
21 of 25 PASS accepted_values_mart_payment_type_over_time_payment_method  [0.02s]
22 of 25 PASS not_null_mart_payment_type_over_time_payment_method         [0.02s]
23 of 25 PASS not_null_mart_revenue_by_zone_pickup_location_id            [0.02s]
24 of 25 PASS not_null_mart_revenue_by_zone_total_revenue                 [0.01s]
25 of 25 PASS unique_mart_revenue_by_zone_pickup_location_id              [0.01s]

Done. PASS=25  WARN=0  ERROR=0  SKIP=0  NO-OP=0  TOTAL=25
```

Full log: [`results/dbt_test_results.txt`](results/dbt_test_results.txt)

---

## Visual Output

![gold results](results/gold_results.png)

`results/gold_results.png` has two panels built from the real gold marts:

- **Left — Revenue by payment method:** Credit card towers at $128.3M (86%); cash follows at $19.3M (13%). The dominance reflects app-based (Uber/Lyft feed into TLC) and card-on-file bookings. Dispute and No-charge are effectively rounding noise.
- **Right — Revenue by pickup borough:** Manhattan's $111.9M bar dwarfs every other borough — this is the expected shape for NYC yellow taxis, which are authorized to pick up street hails only in Manhattan and the airports. Queens's $35.2M reflects the JFK/LaGuardia airport premium.

---

## Key Findings & Business Insights

### 1. 8.85% of raw TLC records are unusable — higher than the 7–8% industry estimate

528,764 rows were dropped from 5.97M. The dominant defects were zero-distance trips (~3%) and negative fares (~2%). Running analytics directly on bronze would silently undercount revenue and skew fare averages downward by roughly $0.80–1.20 per trip.

### 2. Yellow taxis are a Manhattan + airport product — everything else is noise

Manhattan generates 75% of revenue ($111.9M) from 4.9M trips. Queens generates 23.6% ($35.2M) from just 482K trips — a per-trip revenue of $72.9 vs Manhattan's $22.8. That Queens-over-Brooklyn ordering is the airport effect: JFK and LaGuardia trips are 3–4× more valuable per trip than a Manhattan crosstown.

### 3. Fares and tips follow opposite diurnal patterns

The highest average fares occur at 5 AM ($27.71) — airport morning runs. The highest tip rates occur at 6 PM (20.7%) — post-work evening trips where riders have more flexibility and are likely more generous. A revenue forecast that ignores the hour-of-day structure will misattribute roughly 15% of total revenue.

### 4. The singular dbt tests are a regression guard for the pipeline

`assert_positive_fares` and `assert_passenger_count_in_range` run on the staging view after silver completes. If a future code change to `silver_clean.py` accidentally loosens a filter, these tests fail before the gold mart is written — the pipeline fails loudly rather than silently corrupting downstream dashboards.

### 5. Airflow retry strategy is intentionally asymmetric

The PySpark silver step gets 2 retries (transient Spark JVM failures). The dbt gold step gets 0 — a failing dbt test is deterministic and retrying it would just mask a real data problem. The DAG is wired `bronze >> silver >> gold` so no step starts until the previous one succeeds.

---

## Tech Stack

| Layer | Technology | Version | Role |
|-------|-----------|---------|------|
| Language | Python | 3.10 | Ingestion scripts, DAG |
| Distributed compute | PySpark | 3.5.3 | Silver cleaning job |
| Transformation | dbt-core + dbt-duckdb | 1.11 + 1.10 | Gold marts + tests |
| Warehouse | DuckDB | 1.5 | Local analytical warehouse |
| File format | Apache Parquet + PyArrow | — | Storage for raw + silver |
| Orchestration | Apache Airflow | 2.10 | Daily DAG with retries |
| Containerisation | Docker Compose | — | Local Airflow stack |
| CI | GitHub Actions | — | Full pipeline on every push |

---

## Repo Layout

```
nyc-taxi-pipeline/
├── ingestion/
│   ├── config.py                   shared paths + payment type dict
│   ├── bronze_load.py              raw parquet → bronze.yellow_trips (DuckDB)
│   └── silver_clean.py             PySpark cleaning → silver.yellow_trips
├── dbt_project/
│   ├── dbt_project.yml             project config (staging=view, marts=table)
│   ├── profiles.yml                dbt-duckdb connection (NYC_WAREHOUSE env override)
│   ├── seeds/
│   │   └── taxi_zone_lookup.csv    official TLC 265-row zone reference table
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_yellow_trips.sql
│   │   │   ├── _staging__models.yml   column tests + descriptions
│   │   │   └── _silver__sources.yml   source declaration for silver schema
│   │   └── marts/
│   │       ├── mart_revenue_by_zone.sql
│   │       ├── mart_fare_tip_by_hour.sql
│   │       ├── mart_payment_type_over_time.sql
│   │       ├── mart_duration_distance_outliers.sql
│   │       └── _marts__models.yml     column tests + descriptions
│   └── tests/
│       ├── assert_positive_fares.sql
│       └── assert_passenger_count_in_range.sql
├── airflow/
│   └── dags/
│       └── nyc_taxi_dag.py         daily DAG: bronze → silver → gold+tests
├── infra/
│   └── docker-compose.yml          local Airflow stack (webserver + scheduler + Postgres)
├── scripts/
│   ├── get_data.py                 download real TLC parquet or generate synthetic
│   ├── run_pipeline.sh             local end-to-end runner (no Airflow needed)
│   └── make_results_chart.py       gold_results.png from the gold marts
├── notebooks/
│   └── 01_exploration.md           bronze profiling notes that motivated the cleaning rules
├── results/
│   ├── run_metrics.json            bronze/silver/gold counts from the verified run
│   ├── dbt_test_results.txt        full dbt test log (19/19 tests passing, 25 nodes total)
│   └── gold_results.png            revenue by payment method + by borough
├── .github/
│   └── workflows/
│       └── ci.yml                  full pipeline CI (bronze → PySpark silver → dbt build)
├── requirements.txt
└── LICENSE
```

---

## Run It

### Prerequisites

- Python 3.10+
- Java 11 or 17 (required for PySpark; Java 21 works, Java 25 does not)
- (Optional) Docker + Docker Compose for Airflow

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the full pipeline locally

```bash
bash scripts/run_pipeline.sh
```

| Step | What happens |
|------|-------------|
| `get_data.py` | Downloads Jan + Feb 2024 TLC parquet from the official TLC CDN. Falls back to synthetic data if the download fails. |
| `bronze_load.py` | Loads the parquet files into `bronze.yellow_trips` in DuckDB. |
| `silver_clean.py` | PySpark cleaning job; writes clean parquet to `data/silver/`; loads into `silver.yellow_trips`. |
| `dbt seed` | Loads the official `taxi_zone_lookup.csv` as a reference table. |
| `dbt build` | Builds all 4 gold marts and runs all 19 data-quality tests. |
| `dbt docs generate` | Generates the dbt docs site; open `dbt_project/target/index.html`. |

### 3. Get the real TLC data directly

```bash
# Jan 2024
curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" \
     -o data/raw/yellow_tripdata_2024-01.parquet

# Feb 2024
curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet" \
     -o data/raw/yellow_tripdata_2024-02.parquet
```

No login or API key required. Drop any additional months into `data/raw/` and re-run — nothing else changes.

### 4. Run with Airflow (orchestrated)

```bash
cd infra
docker compose up airflow-init     # one-time DB migration + admin user setup
docker compose up                  # starts webserver + scheduler
```

Open **http://localhost:8080** (user: `airflow`, pass: `airflow`), enable the `nyc_taxi_medallion` DAG, and trigger a run.

---

## What's Next

- **Incremental loads** — change bronze and silver to append only new months rather than `CREATE OR REPLACE`, and configure dbt models as `incremental`
- **More months** — the pipeline already supports any number of TLC monthly files; drop additional parquet files into `data/raw/` and re-run without code changes
- **BigQuery output** — replace the DuckDB profile with a BigQuery adapter in `profiles.yml`; the dbt models are warehouse-agnostic
- **Great Expectations at bronze** — add schema validation before the PySpark step so column-type regressions in the TLC feed are caught before they propagate to silver
- **dbt Exposures** — declare downstream dashboards as dbt exposures so the lineage graph in `dbt docs` shows which BI reports depend on which marts

---

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.
