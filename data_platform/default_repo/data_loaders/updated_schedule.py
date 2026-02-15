"""
Data Loader: updated_schedule.

Fetches the NBA league schedule and normalizes it for the data platform.
Handles nested JSON structures and ensures schema consistency.
"""

import polars as pl
import requests
import json
from datetime import datetime
from typing import Any, Dict, List

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_and_transform_nba_schedule(*args, **kwargs) -> pl.DataFrame:
    """
    Loads and transforms the NBA schedule from the CDN.

    Returns:
        pl.DataFrame: Flattened and normalized schedule data.
    """
    # 1. Extraction
    url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"❌ Failed to fetch schedule: {e}")
        return pl.DataFrame()

    # 2. Initial Processing & Flattening
    # We create a temporary DF to use Polars' powerful JSON/Struct expressions
    df = (
        pl.DataFrame({"raw": [json.dumps(data)]})
        .with_columns(pl.col("raw").str.json_decode())
        .with_columns(
            pl.col("raw")
            .struct.field("leagueSchedule")
            .struct.field("gameDates")
            .alias("game_dates_list")
        )
        .explode("game_dates_list")
        .unnest("game_dates_list")
        .explode("games")
        .unnest("games")
        .drop("raw")
    )

    # 3. Normalization
    # Standardize column names to lowercase for SQL-friendly environments
    df.columns = [c.lower() for c in df.columns]

    # Handle Null/Unknown types to prevent schema evolution issues in Delta
    cols_to_cast = []
    for col_name, dtype in df.schema.items():
        if "Null" in str(dtype):
            cols_to_cast.append(pl.col(col_name).cast(pl.Utf8))

    if cols_to_cast:
        df = df.with_columns(cols_to_cast)

    # Add ingestion metadata
    df = df.with_columns(pl.lit(datetime.now()).alias("ingested_at"))

    print(f"✅ Processed {df.height} games from schedule.")
    return df