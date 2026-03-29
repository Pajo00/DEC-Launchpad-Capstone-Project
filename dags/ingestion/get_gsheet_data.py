import pandas as pd
import io
import os
from datetime import datetime
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import gspread

# destination bucket
dest_bucket = "dec-capstone-joshua-raw-data"

# google sheet id
SHEET_ID = '1vfAfvWLCW3_juM6XiP0ChPEqy1pN6lncLTY-ClxMtN8'


def get_boto3_client(conn_id, region, service):
    hook = AwsBaseHook(aws_conn_id=conn_id, region_name=region, client_type=service)
    return hook.get_client_type()


def get_gsheet_data():
    # extracts store locations from google sheets and loads to s3 as parquet
    s3_dest = get_boto3_client("aws_dest", "us-east-1", "s3")

    # check if file already exists in destination
    dest_key = "raw/stores/stores.parquet"
    objs_dest = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix="raw/stores")
    dest_keys = [obj["Key"] for obj in objs_dest.get("Contents", [])]

    if dest_key in dest_keys:
        print(f"{dest_key} already exists in {dest_bucket}")
        return

    # get credentials from airflow google connection
    hook = GoogleBaseHook(gcp_conn_id="google_sheets_conn")
    creds = hook.get_credentials()
    client = gspread.authorize(creds)

    # open sheet and extract data
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.get_worksheet(0)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    print(f"{len(df)} rows extracted from google sheets")

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
            "source_file": "google_sheets",
            "record_count": str(len(df)),
            "no_of_columns": str(len(df.columns))
        }
    )
    print(f"{dest_key} written to {dest_bucket} successfully!")
