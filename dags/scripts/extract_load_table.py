import logging
from typing import Any

import pandas as pd
import psycopg2
from scripts.gcp_manager import BigQueryManager, GCSManager
from airflow.models import Variable



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_to_postgres() -> psycopg2.extensions.connection:
    """
    Establishes a connection to the PostgreSQL database using credentials from Airflow Variables.
    
    Returns:
        psycopg2.extensions.connection: A connection object to the PostgreSQL database.
    
    Raises:
        psycopg2.Error: If there's an error connecting to the database.
    """

    try:
        conn = psycopg2.connect(
            host=Variable.get("postgres_host"),
            port=Variable.get("postgres_port"),
            database=Variable.get("postgres_database"),
            user=Variable.get("postgres_user"),
            password=Variable.get("postgres_password"),
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise


def extract_from_postgres(table_name: str, schema_name: str) -> pd.DataFrame:
    """
    Extracts data from a specified table in PostgreSQL and returns it as a pandas DataFrame.

    Args:
        table_name (str): The name of the table to extract data from.
        schema_name (str): The name of the schema containing the table.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the extracted data.

    Raises:
        psycopg2.Error: If there's an error connecting to or querying the database.
        pd.io.sql.DatabaseError: If there's an error reading the SQL query into a DataFrame.
    """

    try:
        with connect_to_postgres() as connection:
            query = f"SELECT * FROM {schema_name}.{table_name};"
            df = pd.read_sql(query, connection)
        logger.info(f"Extracted {len(df)} rows from {schema_name}.{table_name}")
        return df
    except (psycopg2.Error, pd.io.sql.DatabaseError) as e:
        logger.error(f"Error extracting data from PostgreSQL: {e}")
        raise


def upload_to_gcs(
    credentials,
    project_id: str,
    bucket_name: str,
    location: str,
    storage_class: str,
    blob_name: str,
    table_name: str,
    schema_name: str,
) -> Any:
    """
    Uploads data from a PostgreSQL table to Google Cloud Storage (GCS).

    This function extracts data from a specified PostgreSQL table, converts it to CSV format,
    and uploads it to a GCS bucket as a blob.

    Args:
        credentials: The credentials for authenticating with Google Cloud.
        project_id (str): The ID of the Google Cloud project.
        bucket_name (str): The name of the GCS bucket to upload to.
        location (str): The location for the GCS bucket.
        storage_class (str): The storage class for the GCS bucket.
        blob_name (str): The name of the blob (file) in the GCS bucket.
        table_name (str): The name of the PostgreSQL table to extract data from.
        schema_name (str): The name of the schema containing the PostgreSQL table.

    Returns:
        Any: The uploaded blob object.

    Raises:
        Exception: If there's an error during the upload process.
    """

    try:
        storage_client = GCSManager(project_id, credentials)
        bucket = storage_client.create_bucket(bucket_name, location, storage_class)
        df = extract_from_postgres(table_name, schema_name)
        buffer = df.to_csv(index=False)
        blob = storage_client.upload_file_from_string(
            bucket, buffer, destination_blob_name=blob_name, content_type="text/csv"
        )
        logger.info(f"Uploaded {blob_name} to GCS bucket {bucket_name}")
        return blob
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        raise


def load_to_bigquery(
    project_id: str,
    table_name: str,
    schema_name: str,
    schema: Any,
    bucket_name: str,
    location: str,
    storage_class: str,
    dataset_id: str,
) -> None:
    """
    Loads data from Google Cloud Storage (GCS) to BigQuery.

    This function creates a BigQuery dataset and table, uploads data from a PostgreSQL table to GCS,
    and then loads that data from GCS into the BigQuery table.

    Args:
        project_id (str): The ID of the Google Cloud project.
        table_name (str): The name of the table to be created in BigQuery.
        schema_name (str): The name of the schema containing the PostgreSQL table.
        schema (Any): The schema definition for the BigQuery table.
        bucket_name (str): The name of the GCS bucket to use as an intermediary.
        location (str): The location for the BigQuery dataset and GCS bucket.
        storage_class (str): The storage class for the GCS bucket.
        dataset_id (str): The ID of the BigQuery dataset to create or use.

    Raises:
        Exception: If there's an error during the data loading process.
    """

    try:
        bq_client = BigQueryManager(project_id, location)
        bq_client.create_dataset(dataset_id)
        bq_client.create_table(dataset_id, table_id=table_name, schema=schema)
        blob = upload_to_gcs(
            bucket_name,
            location,
            storage_class,
            blob_name=f"{table_name}.csv",
            table_name=table_name,
            schema_name=schema_name,
        )
        bq_client.load_from_gcs(
            dataset_id, source_uris=blob, source_format="csv", schema=schema
        )
        logger.info(f"Loaded data from GCS to BigQuery table {dataset_id}.{table_name}")
    except Exception as e:
        logger.error(f"Error loading data to BigQuery: {e}")
        raise
