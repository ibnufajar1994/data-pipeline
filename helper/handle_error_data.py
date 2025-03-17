
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
from minio import Minio
from io import BytesIO
load_dotenv(".env", override=True)
ACCESS_KEY_MINIO = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY_MINIO = os.getenv("MINIO_SECRET_KEY")
import csv


def handle_error(data, bucket_name:str, table_name:str, process=None):

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Initialize MinIO client
    client = Minio('localhost:9000',
                access_key=ACCESS_KEY_MINIO,
                secret_key=SECRET_KEY_MINIO,
                secure=False)

    # Make a bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Convert DataFrame to CSV and then to bytes
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    # Upload the CSV file to the bucket
    client.put_object(
        bucket_name=bucket_name,
        object_name=f"{table_name}_{current_date}.csv", #name the fail source name and current etl date
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv'
    )

    # List objects in the bucket
    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        print(obj.object_name)