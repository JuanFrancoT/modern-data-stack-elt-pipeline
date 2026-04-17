FROM apache/airflow:2.9.0

RUN pip install --no-cache-dir dbt-core dbt-snowflake

USER airflow

RUN pip install --no-cache-dir snowflake-connector-python