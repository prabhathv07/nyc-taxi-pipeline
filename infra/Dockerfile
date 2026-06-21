# Airflow image with everything the medallion DAG needs baked in:
# Java (for PySpark), plus pyspark / dbt-duckdb / duckdb. Baking these into the
# image (instead of PIP_ADDITIONAL_REQUIREMENTS) makes container start fast and
# reliable.
FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
# default-jdk symlinks here; Spark 3.5 supports this JDK (17)
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    dbt-duckdb==1.10.1 \
    duckdb==1.5.4 \
    pandas pyarrow
