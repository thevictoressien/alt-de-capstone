import psycopg2
from scripts.gcp_manager import BigQueryManager, GCSManager
import pandas as pd
from scripts.config import host, database, user, password, table_name, schema_name, bucket_name, location, storage_class

def connect_to_postgres():
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    return conn

def extract_from_postgres(table_name, schema_name):
    # Connect to PostgreSQL
    connection = connect_to_postgres()
    query = f"SELECT * FROM {schema_name}.{table_name};"
    df = pd.read_sql(query, connection)
    connection.close()
    return df

def upload_to_gcs(bucket_name, location,storage_class, blob_name):
    storage_client = GCSManager()
    bucket = storage_client.create_bucket(bucket_name, location, storage_class)
    df = extract_from_postgres(table_name, schema_name)
    buffer = df.to_csv()
    blob = storage_client.upload_file_from_string(bucket, buffer, destination_blob_name=blob_name, content_type='text/csv')
    
    return blob

def load_to_bigquery():
    bigquery_client = BigQueryManager()
    blob = upload_to_gcs(bucket_name, location, storage_class, blob_name=f"{table_name}.csv")
    bigquery_client.load_from_gcs(blob, table_name, schema_name)