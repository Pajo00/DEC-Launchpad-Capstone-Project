import pandas as pd
import io
from datetime import datetime
from sqlalchemy import create_engine, text
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
    # fetch database credentials from bootcamp aws ssm parameter store
    ssm = get_boto3_client("aws_source", "eu-west-2", "ssm")

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
    print("starting extraction: postgres source -> s3 raw layer")
    s3_dest = get_boto3_client("aws_dest", "us-east-1", "s3")

    # check existing files to ensure idempotency
    try:
        objs_dest = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix="raw/sales/")
        dest_keys = [obj["Key"] for obj in objs_dest.get("Contents", [])]
    except Exception as e:
        print(f"notice: could not list existing s3 objects: {e}")
        dest_keys = []

    # get database credentials from ssm
    creds = get_db_credentials()

    # create sqlalchemy engine with ssl and keepalive settings
    connection_uri = (
        f"postgresql+psycopg2://{creds['user']}:{creds['password']}"
        f"@{creds['host']}:{creds['port']}/{creds['dbname']}"
    )

    engine = create_engine(
        connection_uri,
        connect_args={
            "sslmode": "require",
            "connect_timeout": 30,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5
        },
        pool_pre_ping=True,
        pool_recycle=300
    )

    print("connected to postgres database successfully!")

    for table in sales_tables:
        dest_key = f"raw/sales/{table}.parquet"

        # skip if already exists in destination
        if dest_key in dest_keys:
            print(f"skipping {table}: already exists in s3")
            continue

        try:
            print(f"extracting {table}...")

            # extract table from postgres
            with engine.connect() as conn:
                df = pd.read_sql(text(f"SELECT * FROM public.{table}"), conn)

            if df.empty:
                print(f"warning: {table} returned no data")
                continue

            print(f"{len(df)} rows extracted from {table}")

            # convert uuid columns to string to avoid pyarrow conversion error
            for col in df.columns:
                if df[col].dtype == object:
                    try:
                        df[col] = df[col].astype(str)
                    except Exception:
                        pass

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
                    "source_file": f"postgres.public.{table}",
                    "record_count": str(len(df)),
                    "no_of_columns": str(len(df.columns))
                }
            )
            print(f"{dest_key} written to {dest_bucket} successfully!")

        except Exception as e:
            print(f"error processing {table}: {e}")

    engine.dispose()
    print("postgres data extraction completed!")