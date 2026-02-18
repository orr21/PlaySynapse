import boto3
import json
import os
import polars as pl
from datetime import datetime


@data_exporter
def export_to_bronze(df, **kwargs):
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password123"),
        region_name="us-east-1",
    )

    records = df.to_dicts()

    for record in records:

        season = record["season"]
        ingested_at = record["ingested_at"]

        if isinstance(ingested_at, str):
            ingested_at = datetime.fromisoformat(ingested_at)

        raw_json = json.loads(record["raw_content"])

        partition_path = f"ingested_at={ingested_at.strftime('%Y-%m-%d')}"

        file_name = f"{season}.json"

        object_key = f"nba/teams/{partition_path}/{file_name}"

        s3_client.put_object(
            Bucket="bronze",
            Key=object_key,
            Body=json.dumps(raw_json),
            ContentType="application/json",
        )

        print(f"✅ Archivo guardado: {object_key}")
