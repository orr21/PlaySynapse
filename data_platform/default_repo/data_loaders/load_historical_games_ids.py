"""
Data Loader: load_historical_games_ids.

Fetches all game IDs for specified historical seasons.
Used to bootstrap the PBP downloader.
"""

from default_repo.utils.connectors.basketball.league.nba import NbaConnector
import polars as pl
import time
from typing import Any, Dict, List


@data_loader
def load_all_ids(*args, **kwargs) -> pl.DataFrame:
    """
    Fetches game IDs for historical seasons.

    Returns:
        pl.DataFrame: DataFrame with unique GAME_IDs.
    """
    connector = NbaConnector()
    seasons = ["2024-25", "2025-26"]
    all_dfs = []

    for season in seasons:
        data = connector.fetch_data(metric_type="season_games", season=season)
        res = data["resultSets"][0]

        df_season = pl.DataFrame(res["rowSet"], schema=res["headers"], orient="row")

        df_clean = df_season.select(
            [pl.col("GAME_ID"), pl.col("GAME_DATE"), pl.lit(season).alias("SEASON")]
        ).unique()

        all_dfs.append(df_clean)
        time.sleep(1)

    return pl.concat(all_dfs).unique(subset=["GAME_ID"])
