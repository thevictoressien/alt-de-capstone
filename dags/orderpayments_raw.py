from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from scripts.bq_utils import load_schema

# postgres variables
TABLE_NAME = "orderpayments"
SCHEMA_NAME = Variable.get("pg_schema_name")
CONNECTION_ID = Variable.get("postgres_capstone_conn")

# GCP variables
GCP_CONNECTION_ID = Variable.get("bigquery_capstone_conn")
BQ_PROJECT_ID =  Variable.get("project_id") 
BQ_DATASET_ID = Variable.get("dataset_id")
BQ_SCHEMA = Variable.get(f"bq_{TABLE_NAME}_schema") # schema stored in gcs bucket
BQ_TABLE_ID = f"{TABLE_NAME}_raw"

GCS_BUCKET_NAME = Variable.get("bucket_name")

CSV_FILENAME = f"{TABLE_NAME}.csv"

# dag args
dag_name = f"postgres_{TABLE_NAME}_full_load"
schedule_interval = timedelta(days=1)
description = f"ETL DAG for {SCHEMA_NAME}.{TABLE_NAME}"
tags = ["postgres", "ecommerce", "raw", "full load"]

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

start = EmptyOperator(dag=dag, task_id="start")

postgres_to_gcs = PostgresToGCSOperator(
    task_id=f"postgres_{TABLE_NAME}_to_gcs",
    postgres_conn_id=CONNECTION_ID,
    sql=f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME};",
    bucket=GCS_BUCKET_NAME,
    filename=CSV_FILENAME,
    field_delimiter=",",
    export_format="csv",
    gzip=False,
    task_concurrency=1,
    gcp_conn_id= GCP_CONNECTION_ID,
    dag=dag,
)

gcs_to_bigquery = GCSToBigQueryOperator(
    task_id=f"gcs_{TABLE_NAME}_to_bigquery",
    bucket=GCS_BUCKET_NAME,
    source_objects=[CSV_FILENAME],
    schema_fields=load_schema(BQ_SCHEMA), 
    destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id=GCP_CONNECTION_ID,
    dag=dag,
)

start >> postgres_to_gcs >> gcs_to_bigquery