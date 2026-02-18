"""
Data Loader: fetch_yesterday_games_pbp.

Downloads play-by-play data for games played in the last 3 days.
Checks for completed games and avoids re-downloading existing data if possible.
"""

import polars as pl
import requests
import json
from datetime import date, timedelta
import os
import time
from typing import Any, Dict, List

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_pbp_data(schedule_df: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
    """
    Downloads PBP data for recent finished games.

    Args:
        schedule_df (pl.DataFrame): Schedule data to identify recent games.

    Returns:
        pl.DataFrame: Raw PBP data in JSON format.
    """

    storage_options = {
        "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "password123"),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    now = date.today()
    last_3_days = [now - timedelta(days=i) for i in range(1, 4)]

    current_year = now.year
    if now.month < 7:
        season_str = f"{current_year-1}-{str(current_year)[-2:]}"
    else:
        season_str = f"{current_year}-{str(current_year+1)[-2:]}"

    target_games = schedule_df.with_columns(
        pl.col("gamedate").str.to_datetime(format="%m/%d/%Y %H:%M:%S").dt.date()
    ).filter((pl.col("gamedate").is_in(last_3_days)) & (pl.col("gamestatus") == 3))

    game_ids = target_games.get_column("gameid").unique().to_list()

    if not game_ids:
        print(f"📭 No hay partidos finalizados en el rango: {last_3_days}")
        return pl.DataFrame()

    print(f"🏀 Se encontraron {len(game_ids)} partidos. Iniciando descarga...")

    all_pbp = []
    for i, gid in enumerate(game_ids, 1):
        url = (
            f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{gid}.json"
        )
        try:
            res = requests.get(url, timeout=15)
            if res.status_code == 200:
                all_pbp.append(
                    {
                        "game_id": gid,
                        "season": season_str,
                        "raw_content": json.dumps(res.json()),
                        "ingested_at": now,
                    }
                )
                print(f"✅ [{i}/{len(game_ids)}] Game ID: {gid} OK")
            else:
                print(f"⚠️ [{i}/{len(game_ids)}] Error {res.status_code} en ID: {gid}")

            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Error crítico en {gid}: {e}")

    return pl.DataFrame(all_pbp)
