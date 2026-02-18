"""
Data Loader: fetch_players_raw.

Downloads raw player statistics for the current season using the NbaConnector.
"""

from default_repo.utils.connectors.basketball.league.nba import NbaConnector
import polars as pl
import time
from datetime import datetime
import json
from typing import Any, Dict, List

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_current_season_players(*args, **kwargs) -> pl.DataFrame:
    """
    Fetches raw player data for the current NBA season.

    Returns:
        pl.DataFrame: DataFrame containing the raw JSON response and metadata.
    """
    connector = NbaConnector()

    now = datetime.now()

    if now.month < 10:
        year_start = now.year - 1
        year_end = str(now.year)[2:]
    else:
        year_start = now.year
        year_end = str(now.year + 1)[2:]

    current_season = f"{year_start}-{year_end}"

    all_season_responses = []

    print(f"📥 Downloading Current Players Profile - Season: {current_season}")

    json_response = connector.fetch_data(
        metric_type="season_players", season=current_season, perMode="Totals"
    )

    if json_response:
        all_season_responses.append(
            {
                "season": current_season,
                "raw_content": json.dumps(json_response),
                "ingested_at": now.isoformat(),
            }
        )

    return pl.DataFrame(all_season_responses)
