from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.get_s3_files import transfer_s3_files
from ingestion.get_gsheet_data import get_gsheet_data
from ingestion.get_postgres_data import get_postgres_data

# default arguments for the dag
default_args = {
    "owner": "joshua",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# define the dag
with DAG(
    dag_id="supplychain360_ingestion_dag",
    default_args=default_args,
    description="extracts data from all sources and loads to s3 as parquet",
    schedule="@daily",
    start_date=datetime(2026, 3, 10),
    catchup=False,
    tags=["supplychain360", "ingestion"],
) as dag:

    # extract static files from bootcamp s3 bucket
    extract_s3_files = PythonOperator(
        task_id="extract_s3_files",
        python_callable=transfer_s3_files,
    )

    # extract store locations from google sheets
    extract_gsheet_data = PythonOperator(
        task_id="extract_gsheet_data",
        python_callable=get_gsheet_data,
    )

    # extract sales transactions from postgres
    extract_postgres_data = PythonOperator(
        task_id="extract_postgres_data",
        python_callable=get_postgres_data,
    )

    # define task dependencies
    # all three run in parallel since they are independent sources
    [extract_s3_files, extract_gsheet_data, extract_postgres_data]