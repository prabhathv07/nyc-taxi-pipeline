---
title: NYC Taxi Analytics
emoji: 🚕
colorFrom: blue
colorTo: green
sdk: streamlit
sdk_version: 1.58.0
app_file: app.py
pinned: false
license: mit
---

# NYC Yellow Taxi — Interactive Analytics Dashboard

A live, clickable Streamlit dashboard over the gold marts of the
[nyc-taxi-pipeline](https://github.com/prabhathv07/nyc-taxi-pipeline) medallion
project. Built on **real NYC TLC Yellow Taxi data, Jan–Feb 2024**
(5,972,150 raw trips → 5,443,386 clean after PySpark + dbt).

**What you can do:** filter by month, payment method, pickup borough, and hour
of day, and every KPI and chart recomputes live — revenue by borough, payment
mix, trips & average fare by hour, tip % by hour, and the top-15 pickup zones.

## How it's wired (and why it's tiny to deploy)
The full DuckDB warehouse is ~530 MB. The dashboard doesn't ship that — instead
`build_dashboard_data.py` rolls the 5.4M clean trips up into a **1,136-row**
month×borough×payment×hour aggregate plus a zone roll-up (~60 KB of parquet in
`data/`). DuckDB reads those parquet files at serve time, so cold start is fast
and no Spark/warehouse is needed in the hosted app.

```
gold.stg_yellow_trips (5.4M rows, DuckDB)
        │  build_dashboard_data.py  (roll-up, exclude 18 stray-timestamp rows)
        ▼
data/agg_main.parquet  (1,136 rows)  + agg_zone + agg_outliers + meta.json
        │  app.py  (Streamlit + Plotly, DuckDB reader)
        ▼
interactive dashboard
```

## Run locally
```bash
pip install -r requirements.txt
streamlit run app.py
# open http://localhost:8501
```

## Deploy — option A: Hugging Face Spaces (free, no card)
This folder *is* a Space. With the HF CLI:
```bash
pip install huggingface_hub
huggingface-cli login                      # paste a write token from hf.co/settings/tokens
huggingface-cli repo create nyc-taxi-analytics --type space --space_sdk streamlit
git clone https://huggingface.co/spaces/<your-username>/nyc-taxi-analytics
cp -r app.py requirements.txt README.md data/ nyc-taxi-analytics/
cd nyc-taxi-analytics && git add -A && git commit -m "NYC taxi dashboard" && git push
# live at https://huggingface.co/spaces/<your-username>/nyc-taxi-analytics
```
(The YAML front-matter at the top of this README is what HF Spaces reads to pick
the Streamlit runtime and `app.py` entry point.)

## Deploy — option B: Streamlit Community Cloud (free)
1. Push the repo to GitHub (the dashboard lives in `dashboard/`).
2. Go to share.streamlit.io → New app → pick the repo →
   set **Main file path** to `dashboard/app.py` → Deploy.

## Refresh the data
After re-running the pipeline on new months, regenerate the bundle:
```bash
NYC_WAREHOUSE=../warehouse/nyc_taxi.duckdb python build_dashboard_data.py
```
