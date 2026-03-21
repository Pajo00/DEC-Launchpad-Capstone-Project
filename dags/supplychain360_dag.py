from airflow.sdk import DAG
from pendulum import datetime
from datetime import timedelta
from airflow.providers.standard.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping

from data_ingestion.get_s3_files import transfer_s3_files
from data_ingestion.get_gsheet_data import get_gsheet_data
from data_ingestion.get_postgres_data import get_postgres_data

# configure dbt profile
profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="dev",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_conn",
        profile_args={"schema": "landing"},
    )
)

# path to dbt project inside airflow container
dbt_project_path = "/opt/airflow/dags/dbt_project"

# define task success and failure callbacks
def success_callback(context):
    with open("/opt/airflow/logs/task_events.log", "a") as f:
        f.write(f"{datetime.now()} SUCCESS: {context['task_instance'].task_id}\n")

def failure_callback(context):
    with open("/opt/airflow/logs/task_events.log", "a") as f:
        f.write(f"{datetime.now()} FAILED: {context['task_instance'].task_id}\n")

# default args for the dag
default_args = {
    "owner": "joshua",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": success_callback,
    "on_failure_callback": failure_callback
}

# define the dag
with DAG(
    dag_id="supplychain360_pipeline",
    start_date=datetime(2026, 3, 10),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["supplychain360", "ingestion", "dbt"],
) as dag:

    # extract static files from bootcamp s3 bucket
    ingest_s3_files = PythonOperator(
        task_id="ingest_s3_files",
        python_callable=transfer_s3_files,
    )

    # extract store locations from google sheets
    ingest_gsheet_data = PythonOperator(
        task_id="ingest_gsheet_data",
        python_callable=get_gsheet_data,
    )

    # extract sales transactions from postgres
    ingest_postgres_data = PythonOperator(
        task_id="ingest_postgres_data",
        python_callable=get_postgres_data,
    )

    # run dbt models after all ingestion is complete
    run_dbt_models = DbtTaskGroup(
        group_id="run_dbt_models",
        project_config=ProjectConfig(dbt_project_path),
        profile_config=profile_config,
        default_args={"retries": 2}
    )

    # all three ingestion tasks run in parallel
    # then dbt runs after all three complete
    [ingest_s3_files, ingest_gsheet_data, ingest_postgres_data] >> run_dbt_models