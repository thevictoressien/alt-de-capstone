from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from scripts.bq_utils import load_schema





# dag args
dag_name = f"dbt_transformation"
schedule_interval = timedelta(days=1)
description = f"DAG that triggers dbt transformations of BigQuery tables"
tags = ["dbt", "transformation", "model"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    description=description,
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    catchup=False,
    tags=tags,
)

start = DummyOperator(dag=dag, task_id="start")

run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command= 'dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    dag=dag
)

start >> run_dbt_models