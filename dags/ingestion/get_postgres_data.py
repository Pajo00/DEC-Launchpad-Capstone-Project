import pandas as pd
import io
from datetime import datetime
import boto3
import psycopg2
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# destination bucket
dest_bucket = "dec-capstone-joshua-raw-data"

# sales tables to extract
sales_tables = [
    "sales_2026_03_10",
    "sales_2026_03_11",
    "sales_2026_03_12",
    "sales_2026_03_13",
    "sales_2026_03_14",
    "sales_2026_03_15",
    "sales_2026_03_16"
]


def get_boto3_client(conn_id, region, service):
    # a wrapper function around boto3 to interact with aws
    hook = AwsBaseHook(aws_conn_id=conn_id, region_name=region, client_type=service)
    return hook.get_client_type()


def get_db_credentials():
    # fetch database credentials from aws ssm parameter store
    ssm = boto3.client("ssm", region_name="eu-west-2")

    def get_param(name):
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]

    return {
        "host": get_param("/supplychain360/db/host"),
        "port": get_param("/supplychain360/db/port"),
        "dbname": get_param("/supplychain360/db/dbname"),
        "user": get_param("/supplychain360/db/user"),
        "password": get_param("/supplychain360/db/password")
    }


def get_postgres_data():
    # extracts sales tables from postgres and loads to s3 as parquet
    s3_dest = get_boto3_client("aws_dest", "us-east-1", "s3")

    # check existing files in destination
    objs_dest = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix="raw/sales")
    dest_keys = [obj["Key"] for obj in objs_dest.get("Contents", [])]

    # get database credentials from ssm
    creds = get_db_credentials()

    # connect to postgres
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["user"],
        password=creds["password"]
    )

    print("connected to postgres database successfully!")

    for table in sales_tables:
        dest_key = f"raw/sales/{table}.parquet"

        # skip if already exists
        if dest_key in dest_keys:
            print(f"{dest_key} already exists in {dest_bucket}")
            continue

        # extract table from postgres
        df = pd.read_sql(f"SELECT * FROM store_sales.{table}", conn)
        print(f"{len(df)} rows extracted from {table}")

        # add ingestion timestamp
        df["ingested_at"] = datetime.now()

        # convert to parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)

        # upload to s3
        s3_dest.put_object(
            Bucket=dest_bucket,
            Key=dest_key,
            Body=parquet_buffer.getvalue(),
            Metadata={
                "load_time": datetime.utcnow().isoformat(),
                "source_file": f"postgres.store_sales.{table}",
                "record_count": str(len(df)),
                "no_of_columns": str(len(df.columns))
            }
        )
        print(f"{dest_key} written to {dest_bucket} successfully!")

    conn.close()
    print("postgres data extraction completed!")
