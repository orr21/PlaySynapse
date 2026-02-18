"""
Transformer: dashboard_pbp_gold.

This block aggregates Play-by-Play Silver data into player stats for the Gold Dashboard.
It calculates points, rebounds, assists, and efficiency metrics, joining with team metadata.
"""

import polars as pl
from typing import Any, Dict, List
from default_repo.utils.schemas import (
    DASHBOARD_GOLD_SCHEMA,
    DASHBOARD_GOLD_OUTPUT_CRITICAL,
)

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer

if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_pbp_to_dashboard_schema(
    pbp_df: pl.DataFrame, *args, **kwargs
) -> pl.DataFrame:
    """
    Aggregates PBP data into Gold Dashboard stats.

    Args:
        pbp_df (pl.DataFrame): Input Play-by-Play data.

    Returns:
        pl.DataFrame: Aggregated player statistics with calculated metrics.
    """

    game_opponents = (
        pbp_df.select(["game_id", "teamtricode"])
        .filter(pl.col("teamtricode").is_not_null() & (pl.col("teamtricode") != ""))
        .group_by("game_id")
        .agg(pl.col("teamtricode").unique().alias("teams"))
    )

    df = pbp_df.with_columns(
        [
            pl.col("personid").cast(pl.Int64),
            pl.col("assistpersonid").cast(pl.Int64).fill_null(0),
            pl.col("actiontype").str.to_lowercase().fill_null(""),
            pl.col("shotresult").str.to_lowercase().fill_null(""),
            (
                pl.col("action_timestamp")
                .str.to_datetime()
                .dt.date()
                .alias("game_date")
                if pbp_df.schema.get("action_timestamp") == pl.Utf8
                else pl.col("action_timestamp").dt.date().alias("game_date")
            ),
        ]
    ).with_columns(
        pl.when(
            (pl.col("shotresult") == "made")
            & (pl.col("actiontype").str.contains("3pt"))
        )
        .then(3)
        .when(
            (pl.col("shotresult") == "made")
            & (pl.col("actiontype").str.contains("2pt"))
        )
        .then(2)
        .when(
            (pl.col("shotresult") == "made")
            & (pl.col("actiontype").str.contains("free"))
        )
        .then(1)
        .otherwise(0)
        .alias("points_value")
    )

    stats_main = (
        df.filter(pl.col("personid") != 0)
        .group_by(["game_id", "personid", "playername", "teamtricode"])
        .agg(
            [
                pl.col("points_value").sum().cast(pl.Int64).alias("pts"),
                pl.col("actionnumber")
                .filter(pl.col("actiontype").str.contains("rebound"))
                .count()
                .cast(pl.Int64)
                .alias("reb"),
                pl.col("actionnumber")
                .filter(pl.col("actiontype").str.contains("pt"))
                .count()
                .cast(pl.Int64)
                .alias("fga"),
                pl.col("actionnumber")
                .filter(
                    (pl.col("actiontype").str.contains("pt"))
                    & (pl.col("shotresult") == "made")
                )
                .count()
                .cast(pl.Int64)
                .alias("fgm"),
                pl.col("actionnumber")
                .filter(pl.col("actiontype").str.contains("3pt"))
                .count()
                .cast(pl.Int64)
                .alias("3pa"),
                pl.col("actionnumber")
                .filter(
                    (pl.col("actiontype").str.contains("3pt"))
                    & (pl.col("shotresult") == "made")
                )
                .count()
                .cast(pl.Int64)
                .alias("3pm"),
                pl.col("actionnumber")
                .filter(pl.col("actiontype").str.contains("free"))
                .count()
                .cast(pl.Int64)
                .alias("fta"),
                pl.col("actionnumber")
                .filter(
                    (pl.col("actiontype").str.contains("free"))
                    & (pl.col("shotresult") == "made")
                )
                .count()
                .cast(pl.Int64)
                .alias("ftm"),
                pl.col("turnovertotal").sum().fill_null(0).cast(pl.Int64).alias("tov"),
                pl.col("stealpersonid").is_not_null().sum().cast(pl.Int64).alias("stl"),
                pl.col("blockpersonid").is_not_null().sum().cast(pl.Int64).alias("blk"),
            ]
        )
    )

    stats_ast = (
        df.filter(pl.col("assistpersonid") != 0)
        .group_by(["game_id", "assistpersonid"])
        .agg(pl.len().cast(pl.Int64).alias("ast"))
        .rename({"assistpersonid": "personid"})
    )

    stats = stats_main.join(
        stats_ast, on=["game_id", "personid"], how="left"
    ).with_columns(pl.col("ast").fill_null(0))

    meta = (
        df.group_by("game_id")
        .agg(
            [
                pl.col("season").first().alias("season"),
                pl.col("game_date").first().alias("game_date"),
            ]
        )
        .join(game_opponents, on="game_id")
    )

    final_df = stats.join(meta, on="game_id")

    final_df = final_df.with_columns(
        [
            (pl.col("fgm") / pl.col("fga")).fill_nan(0).alias("pct_fg"),
            (pl.col("ftm") / pl.col("fta")).fill_nan(0).alias("pct_ft"),
            (pl.col("3pm") / pl.col("3pa")).fill_nan(0).alias("pct_3"),
            pl.lit(0).alias("plus_minus"),
            pl.struct(["teamtricode", "teams"])
            .map_elements(
                lambda x: (
                    [t for t in x["teams"] if t != x["teamtricode"]][0]
                    if len(x["teams"]) > 1
                    else "Unknown"
                ),
                return_dtype=pl.Utf8,
            )
            .alias("opponent"),
        ]
    )

    final_df = final_df.with_columns(
        [
            pl.col("personid").alias("player_id"),
            pl.col("playername").alias("player_name"),
            pl.col("teamtricode").alias("team"),
        ]
    )

    for col_name, dtype in DASHBOARD_GOLD_SCHEMA.items():
        if col_name not in final_df.columns:
            final_df = final_df.with_columns(pl.lit(None, dtype=dtype).alias(col_name))

    final_cols = []
    for col_name, dtype in DASHBOARD_GOLD_SCHEMA.items():
        final_cols.append(pl.col(col_name).cast(dtype, strict=False))

    return final_df.select(final_cols).sort("pts", descending=True).unique()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    from default_repo.utils.schemas import (
        validate_schema,
        DASHBOARD_GOLD_SCHEMA,
        DASHBOARD_GOLD_OUTPUT_CRITICAL,
    )

    assert output is not None, "The output is undefined"

    validate_schema(output, DASHBOARD_GOLD_SCHEMA, DASHBOARD_GOLD_OUTPUT_CRITICAL)
