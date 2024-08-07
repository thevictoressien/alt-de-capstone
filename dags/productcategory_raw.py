from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from scripts.extract_load_table import (
    extract_from_postgres,
    upload_to_gcs,
    load_to_bigquery,
)


# table and schema name for this specific DAG
TABLE_NAME = "productcategorynametranslation"
SCHEMA_NAME = "ecomm"
dag_name = f"{TABLE_NAME}_raw"
schedule_interval = timedelta(days=1)
description = f"ETL DAG for {SCHEMA_NAME}.{TABLE_NAME}"
tags = ["postgres", "ecommerce", "raw"]


# retreieve variables from Airflow variables
bucket_name = Variable.get("bucket_name")
location = Variable.get("location")
storage_class = Variable.get("storage_class")
dataset_id = Variable.get("dataset_id")
project_id = Variable.get("project_id")
google_cloud_credentials = Variable.get("google_cloud_credentials")

schema = []

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
    description=f"ETL DAG for {SCHEMA_NAME}.{TABLE_NAME}",
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    tags=tags,
)

start = DummyOperator(dag=dag, task_id="start")


extract_task = PythonOperator(
    task_id="extract_from_postgres",
    python_callable=extract_from_postgres,
    op_kwargs={"table_name": TABLE_NAME, "schema_name": SCHEMA_NAME},
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_gcs",
    python_callable=upload_to_gcs,
    op_kwargs={
        "credentials": google_cloud_credentials,
        "project_id": project_id,
        "bucket_name": bucket_name,
        "location": location,
        "storage_class": storage_class,
        "blob_name": f"{TABLE_NAME}.csv",
        "table_name": TABLE_NAME,
        "schema_name": SCHEMA_NAME,
    },
    dag=dag,
)


load_task = PythonOperator(
    task_id="load_to_bigquery",
    python_callable=load_to_bigquery,
    op_kwargs={
        'project_id': project_id,
        "table_name": TABLE_NAME,
        "schema_name": SCHEMA_NAME,
        # "schema": schema,
        "bucket_name": bucket_name,
        "location": location,
        "storage_class": storage_class,
        "dataset_id": dataset_id,
    },
    dag=dag,
)

start >> extract_task >> upload_task >> load_task
