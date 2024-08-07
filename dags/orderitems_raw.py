from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.extract_load_table import extract_from_postgres, upload_to_gcs, load_to_bigquery
from scripts.config import bucket_name, location, storage_class

# Define the table and schema name for this specific DAG
TABLE_NAME = "orderitems"
SCHEMA_NAME = "ecomm"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    f'{TABLE_NAME}_etl',
    default_args=default_args,
    description=f'ETL DAG for {SCHEMA_NAME}.{TABLE_NAME}',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_from_postgres,
    op_kwargs={'table_name': TABLE_NAME, 'schema_name': SCHEMA_NAME},
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={
        'bucket_name': bucket_name,
        'location': location,
        'storage_class': storage_class,
        'blob_name': f"{TABLE_NAME}.csv"
    },
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag,
)

extract_task >> upload_task >> load_task
