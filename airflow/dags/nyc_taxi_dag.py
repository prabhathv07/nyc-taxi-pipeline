"""
Airflow DAG for the NYC taxi medallion pipeline.
Runs bronze -> silver (PySpark) -> dbt gold + tests daily.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

# Repo root. In the Docker stack the repo is mounted at /opt/project while the
# DAG lives in Airflow's own /opt/airflow/dags, so the relative ".parents[2]"
# guess is wrong there — honor NYC_PROJECT_ROOT when set (compose sets it).
REPO = Path(os.environ.get("NYC_PROJECT_ROOT", Path(__file__).resolve().parents[2]))
ENV = {**os.environ, "PYTHONPATH": str(REPO / "ingestion"),
       "SPARK_LOCAL_IP": "127.0.0.1", "SPARK_LOCAL_HOSTNAME": "localhost"}

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="nyc_taxi_medallion",
    description="Bronze -> Silver (PySpark) -> Gold (dbt) with data-quality tests",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc-taxi", "medallion", "pyspark", "dbt"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_load",
        bash_command=f"cd {REPO}/ingestion && python bronze_load.py",
        env=ENV,
    )

    silver = BashOperator(
        task_id="silver_clean_pyspark",
        bash_command=f"cd {REPO}/ingestion && python silver_clean.py",
        env=ENV,
    )

    # dbt build runs models + tests together; a test failure fails the task
    gold_and_tests = BashOperator(
        task_id="gold_dbt_build",
        bash_command=(
            f"cd {REPO}/dbt_project && "
            f"DBT_PROFILES_DIR={REPO}/dbt_project dbt build"
        ),
        env=ENV,
        retries=0,
    )

    bronze >> silver >> gold_and_tests
