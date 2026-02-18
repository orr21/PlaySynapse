"""
Data Loader: load_historical_players.

Downloads historical player data for multiple seasons.
Stores raw JSON responses for subsequent processing.
"""

from default_repo.utils.connectors.basketball.league.nba import NbaConnector
import polars as pl
import time
from datetime import datetime
import json
from typing import Any, Dict, List


@data_loader
def load_multi_season_players(*args, **kwargs) -> pl.DataFrame:
    """
    Fetches player data for a list of historical seasons.

    Returns:
        pl.DataFrame: DataFrame with raw player data per season.
    """
    connector = NbaConnector()

    seasons = ["2023-24", "2024-25"]

    all_season_responses = []

    for season in seasons:
        print(f"📥 Downloading Players Profile - Season: {season}")

        json_response = connector.fetch_data(
            metric_type="season_players", season=season, perMode="Totals"
        )

        if json_response:

            all_season_responses.append(
                {
                    "season": season,
                    "raw_content": json.dumps(json_response),
                    "ingested_at": datetime.now().isoformat(),
                }
            )

        time.sleep(1.5)

    return pl.DataFrame(all_season_responses)
