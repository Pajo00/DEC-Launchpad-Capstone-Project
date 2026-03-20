import pandas as pd
import io
from datetime import datetime
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# bucket parameters
source_bucket = "supplychain360-data"
dest_bucket = "dec-capstone-joshua-raw-data"

# s3 folders to extract
s3_keys = ["products", "suppliers", "warehouses", "inventory", "shipments"]


def get_boto3_client(conn_id, region, service):
    # a wrapper function around boto3 to interact with aws
    hook = AwsBaseHook(aws_conn_id=conn_id, region_name=region, client_type=service)
    return hook.get_client_type()


def transfer_s3_files():
    # transfers and converts s3 files from bootcamp account to personal account
    s3_source = get_boto3_client("aws_source", "eu-west-2", "s3")
    s3_dest = get_boto3_client("aws_dest", "us-east-1", "s3")

    for s3_key in s3_keys:
        # list all objects in source and destination
        objs_source = s3_source.list_objects_v2(Bucket=source_bucket, Prefix=f"raw/{s3_key}")
        objs_dest = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix=f"raw/{s3_key}")

        source_keys = [obj["Key"] for obj in objs_source.get("Contents", [])]
        dest_keys = [obj["Key"] for obj in objs_dest.get("Contents", [])]

        for key in source_keys:
            # skip directories
            if key.endswith("/"):
                print(f"{key} is not a file")
                continue

            # only process csv and json files
            if key.endswith(".csv"):
                dest_key = f"raw/{s3_key}/{key.split('/')[-1].replace('.csv', '.parquet')}"
            elif key.endswith(".json"):
                dest_key = f"raw/{s3_key}/{key.split('/')[-1].replace('.json', '.parquet')}"
            else:
                print(f"{key} not a target file to extract!")
                continue

            # skip if file already exists in destination
            if dest_key in dest_keys:
                print(f"{dest_key} already exists in {dest_bucket}")
                continue

            # read source file
            file = s3_source.get_object(Bucket=source_bucket, Key=key)

            if key.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(file["Body"].read()))
            elif key.endswith(".json"):
                df = pd.read_json(io.BytesIO(file["Body"].read()))

            # add ingestion timestamp
            df["ingested_at"] = datetime.now()

            # convert to parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine="pyarrow", index=False)

            # upload to destination bucket
            s3_dest.put_object(
                Bucket=dest_bucket,
                Key=dest_key,
                Body=parquet_buffer.getvalue(),
                Metadata={
                    "load_time": datetime.utcnow().isoformat(),
                    "source_file": key,
                    "record_count": str(len(df)),
                    "no_of_columns": str(len(df.columns))
                }
            )
            print(f"{dest_key} written to {dest_bucket} successfully!")

        print(f"{s3_key} data transfer to {dest_bucket} completed!")