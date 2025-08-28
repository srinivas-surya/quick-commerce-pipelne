from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import sys

PROJECT_DIR = "/quick-commerece-pipeline/"
PYTHON_BIN = "python3"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="quickcommerce_dag",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["quick-commerce", "batch"],
) as dag:

    export_orders = BashOperator(
        task_id="export_orders",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/ingestion/batch/export_orders.py",
    )

    export_inventory = BashOperator(
        task_id="export_inventory",
        bash_command=f"{PYTHON_BIN} {PROJECT_DIR}/ingestion/batch/export_inventory.py",
    )

    spark_batch = BashOperator(
        task_id="spark_batch_transform",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            f"--py-files {PROJECT_DIR}/processing/spark_batch_job.py "
            f"{PROJECT_DIR}/processing/spark_batch_job.py"
        ),
    )

    export_orders >> export_inventory >> spark_batch
