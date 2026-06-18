#!/usr/bin/env bash
# Run the full medallion pipeline locally without Airflow:
#   bronze -> silver (PySpark) -> gold (dbt build = models + tests)
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-127.0.0.1}
export SPARK_LOCAL_HOSTNAME=${SPARK_LOCAL_HOSTNAME:-localhost}
export PYTHONPATH="$PWD/ingestion:${PYTHONPATH:-}"

echo "==> data (real download if reachable, else synthetic TLC-schema)"
python scripts/get_data.py

echo "==> bronze"
python ingestion/bronze_load.py

echo "==> silver (PySpark)"
python ingestion/silver_clean.py

echo "==> gold + tests (dbt)"
cd dbt_project
export DBT_PROFILES_DIR="$PWD"
dbt seed
dbt build
dbt docs generate
echo "==> done. open dbt_project/target/index.html for docs."
