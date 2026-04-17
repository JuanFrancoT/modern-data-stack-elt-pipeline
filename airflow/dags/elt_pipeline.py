from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
import snowflake.connector
import os
from dotenv import load_dotenv


default_args = {
    "owner": "juan",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="elt_pipeline_snowflake",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["elt", "snowflake", "dbt"],
) as dag:

    # 1. Extract + Load (simulado por ahora)
    def load_to_snowflake():
        load_dotenv("/opt/airflow/.env")  
        conn = snowflake.connector.connect(
            user=os.environ.get("SNOWFLAKE_USER"),
            password=os.environ.get("SNOWFLAKE_PASSWORD"),
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema="RAW"
        )

        cursor = conn.cursor()

        cursor.execute("""
            COPY INTO raw.orders
            FROM @my_s3_stage/olist_orders_dataset.csv
            FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1);
        """)

        cursor.close()
        conn.close()

    extract_load = PythonOperator(
        task_id="extract_load",
        python_callable=load_to_snowflake
    )

    # 2. dbt run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        set -a
        source /opt/airflow/.env
        set +a

        cd /opt/airflow/elt_project
        /home/airflow/.local/bin/dbt run --profiles-dir /opt/airflow/elt_project
        """
    )

    # 3. dbt test (data quality)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        set -a
        source /opt/airflow/.env
        set +a

        cd /opt/airflow/elt_project
        /home/airflow/.local/bin/dbt test --profiles-dir /opt/airflow/elt_project
        """
    )

    extract_load >> dbt_run >> dbt_test