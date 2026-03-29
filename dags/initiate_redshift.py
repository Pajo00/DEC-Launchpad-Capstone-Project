from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="initialize_redshift",
    start_date=datetime(2026, 3, 27),
    description="creates redshift schemas and loads data from s3",
    schedule=None
):
    # create raw, staging and marts schemas in redshift
    create_redshift_schemas = RedshiftDataOperator(
        task_id="create_redshift_schemas",
        cluster_identifier="dec-capstone-joshua-cluster",
        database="dec_capstone_joshua",
        db_user="adminuser",
        region_name="us-east-1",
        aws_conn_id="aws_dest",
        sql="dags/sql/redshift_schemas.sql"
    )

    # copy all parquet files from s3 into redshift raw schema
    load_redshift_tables = RedshiftDataOperator(
        task_id="load_s3_to_redshift",
        cluster_identifier="dec-capstone-joshua-cluster",
        database="dec_capstone_joshua",
        db_user="adminuser",
        region_name="us-east-1",
        aws_conn_id="aws_dest",
        sql="dags/sql/data_load.sql"
    )

    # trigger the main pipeline dag after loading
    trigger_main_dag = TriggerDagRunOperator(
        task_id="trigger_main_pipeline",
        trigger_dag_id="supplychain360_pipeline"
    )

    create_redshift_schemas >> load_redshift_tables >> trigger_main_dag