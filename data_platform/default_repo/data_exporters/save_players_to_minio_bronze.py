"""
Data Exporter: save_players_to_minio_bronze.

Saves raw Player data to MinIO (Bronze Layer).
Partitions by ingestion date.
"""

import boto3
import json
import os
import polars as pl
from datetime import datetime
from typing import Any, Dict, List

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_to_bronze(df: pl.DataFrame, **kwargs) -> None:
    """
    Exports Player DataFrame to MinIO Bronze bucket.

    Args:
        df (pl.DataFrame): Raw player data.
    """
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'admin'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'password123'),
        region_name='us-east-1' 
    )

    records = df.to_dicts()

    for record in records:

        season = record['season']
        ingested_at = record['ingested_at']

        if isinstance(ingested_at, str):
            ingested_at = datetime.fromisoformat(ingested_at)
        
        raw_json = json.loads(record['raw_content'])
        
        # 1. Carpeta por Fecha de Ingestión (para auditoría y Time Travel)
        partition_path = f"ingested_at={ingested_at.strftime('%Y-%m-%d')}"
        
        # 2. Nombre del archivo con Semana Y Fecha de inicio
        # Ejemplo: week_15_from_20241022.json
        file_name = f"{season}.json"
        
        object_key = f"nba/players/{partition_path}/{file_name}"
        
        s3_client.put_object(
            Bucket='bronze',
            Key=object_key,
            Body=json.dumps(raw_json),
            ContentType='application/json'
        )
        
        print(f"✅ Archivo guardado: {object_key}")