"""
Data Exporter: save_pbp_to_silver.

Saves cleaned PBP data to MinIO (Silver Layer) using Delta Lake.
Applies partitioning by year/month/day/game_id.
"""

import os
import polars as pl
from typing import Any, Dict, List

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_to_delta(df: pl.DataFrame, **kwargs) -> None:
    """
    Exports PBP DataFrame to Silver Delta Table.

    Args:
        df (pl.DataFrame): Cleaned PBP data.
    """
    if df.height == 0: return

    # Predicado para idempotencia basado en los IDs del partido
    game_ids = df["game_id"].unique().to_list()
    ids_string = ", ".join([f"'{gid}'" for gid in game_ids])
    
    storage_options = {
        "AWS_ENDPOINT_URL": os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        "AWS_ACCESS_KEY_ID": os.getenv('MINIO_ROOT_USER', 'admin'),
        "AWS_SECRET_ACCESS_KEY": os.getenv('MINIO_ROOT_PASSWORD', 'password123'),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    # Definimos el orden de las carpetas: Año -> Mes -> Día -> Partido
    delta_write_options = {
        "partition_by": ["year", "month", "day", "game_id"],
        "predicate": f"game_id IN ({ids_string})",
        "schema_mode": "overwrite"
    }

    df.write_delta(
        "s3://silver/nba/pbp/",
        mode="overwrite",
        storage_options=storage_options,
        overwrite_schema=True,
        delta_write_options=delta_write_options
    )
    print(f"✅ PBP Silver organizado por Year/Month/Day para los juegos: {ids_string}")