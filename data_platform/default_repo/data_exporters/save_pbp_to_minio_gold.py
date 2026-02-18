"""
Data Exporter: save_pbp_to_minio_gold.

Merges PBP data into the Gold Layer (Delta Lake) with idempotency.
Deletes existing records for the game before appending new ones.
"""

import os
import polars as pl
from deltalake import DeltaTable
from typing import Any, Dict

if "data_exporter" not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_to_delta(df: pl.DataFrame, **kwargs) -> None:
    """
    Exports data to Gold Layer (Delta Lake).

    Args:
        df (pl.DataFrame): PBP data to export.
    """
    if df.height == 0:
        return

    game_ids = df["game_id"].drop_nulls().unique().to_list()

    if not game_ids:
        print("No valid game_ids found to process.")
        return

    is_numeric = df["game_id"].dtype in [
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt32,
        pl.UInt64,
    ]

    if is_numeric:

        ids_list_str = ", ".join([str(gid) for gid in game_ids])
        predicate = f"game_id in ({ids_list_str})"
    else:

        ids_list_str = ", ".join([f"'{gid}'" for gid in game_ids])
        predicate = f"game_id in ({ids_list_str})"

    table_path = "s3://gold/nba/pbp/"

    storage_options = {
        "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "password123"),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    try:
        dt = DeltaTable(table_path, storage_options=storage_options)
        dt.delete(predicate)
        print(f"🗑️ Deleted old data for games: {ids_list_str}")
    except Exception as e:

        print(
            f"⚠️ Table might not exist or delete failed (safe to ignore on first run): {e}"
        )

    delta_write_options = {
        "partition_by": ["season", "game_date"],
        "schema_mode": "merge",
    }

    df.write_delta(
        table_path,
        mode="append",
        storage_options=storage_options,
        delta_write_options=delta_write_options,
    )

    print(f"✅ PBP Gold updated (Delete+Append) for games: {ids_list_str}")
