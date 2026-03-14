import os
import io
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def get_lakehouse_client():
    """Create and return an authenticated Fabric Lakehouse client."""
    credential = ClientSecretCredential(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET")
    )

    client = DataLakeServiceClient(
        account_url=f"https://onelake.dfs.fabric.microsoft.com",
        credential=credential
    )

    return client


def write_parquet_to_lakehouse(df: pd.DataFrame, folder: str, filename: str):
    """
    Write a pandas DataFrame as a parquet file to the Fabric Lakehouse.
    
    Args:
        df: The DataFrame to write
        folder: The folder inside the Lakehouse Files section e.g. 'raw/products'
        filename: The parquet filename e.g. 'products_20260314.parquet'
    """
    client = get_lakehouse_client()

    workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
    lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID")

    # Convert DataFrame to parquet bytes
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Get the file system client (this is your lakehouse)
    file_system_client = client.get_file_system_client(workspace_id)

    # Full path inside the lakehouse
    file_path = f"{lakehouse_id}/Files/{folder}/{filename}"

    # Create and upload the file
    file_client = file_system_client.get_file_client(file_path)
    file_client.upload_data(buffer.read(), overwrite=True)

    print(f"✅ Written to Lakehouse: {file_path}")


