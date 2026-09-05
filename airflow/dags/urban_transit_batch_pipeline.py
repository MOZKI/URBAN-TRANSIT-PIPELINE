"""
urban_transit_batch_pipeline
-----------------------------
Batch layer: MinIO Bronze -> MotherDuck Staging -> Gold -> DQ test.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "hernandez",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="urban_transit_batch_pipeline",
    description="MinIO Bronze -> MotherDuck Staging -> dbt run -> dbt snapshot -> dbt test",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["urban-transit", "batch"],
) as dag:

    load_bronze_to_staging = BashOperator(
        task_id="load_bronze_to_staging",
        bash_command="python /opt/airflow/scripts/load_bronze_to_staging.py",
    )
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"cd {DBT_DIR} && dbt run")
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=f"cd {DBT_DIR} && dbt snapshot")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"cd {DBT_DIR} && dbt test")

    load_bronze_to_staging >> dbt_run >> dbt_snapshot >> dbt_test