"""
Transformer: extract_unique_teams.

This block extracts unique team information from Bronze data to populate the Silver Teams table.
It handles JSON decoding, deduplication, and schema validation.
"""

import polars as pl
from datetime import datetime
from typing import Any, Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(df_bronze: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
    """
    Transforms Bronze data to unique Teams Silver format.

    Args:
        df_bronze (pl.DataFrame): Input DataFrame with raw team data.

    Returns:
        pl.DataFrame: DataFrame containing unique team records.
    """
    # 1. Decodificación del JSON
    df_transformed = df_bronze.with_columns([
        pl.col("raw_content").str.json_decode()
    ]).with_columns([
        pl.col("raw_content").struct.field("resultSets").list.get(0).alias("result_set")
    ])

    # 2. Explode (¡IMPORTANTE!: Añadimos ingested_at a la selección)
    df_exploded = df_transformed.select([
        "ingested_at",  # <--- Faltaba esta columna aquí
        pl.col("season"), # <--- Mantenemos la temporada aquí
        pl.col("result_set").struct.field("headers").alias("headers"),
        pl.col("result_set").struct.field("rowSet").alias("rows")
    ]).explode("rows")

    # 3. Mapeo Dinámico TOTAL
    headers_list = df_exploded["headers"].to_list()[0]
    
    df_final = df_exploded.with_columns([
        pl.col("rows").list.get(i).alias(header.lower())
        for i, header in enumerate(headers_list)
    ])

    # 4. Enriquecimiento y Limpieza
    # Ahora 'ingested_at' sí está disponible en el DataFrame
    df_final = df_final.with_columns([
        pl.col("ingested_at").str.to_datetime().dt.date().alias("ingested_date"),
        
        pl.format("{} {}", pl.col("teamcity"), pl.col("teamname")).alias("full_name"),
        pl.format("https://cdn.nba.com/logos/nba/{}/global/L/logo.svg", pl.col("teamid")).alias("logo_url"),
        pl.col("season").cast(pl.Utf8)
    ])

    # Si no existe 'abbreviation', intentamos usar 'teamslug' o dejarla nula
    if "abbreviation" not in df_final.columns:
        if "teamslug" in df_final.columns:
             df_final = df_final.with_columns(pl.col("teamslug").alias("abbreviation"))
        else:
             df_final = df_final.with_columns(pl.lit(None, dtype=pl.Utf8).alias("abbreviation"))

    # 5. Cumplimiento Estricto del Esquema
    from default_repo.utils.schemas import TEAMS_SILVER_SCHEMA
    
    # Creamos columnas faltantes
    for col_name, dtype in TEAMS_SILVER_SCHEMA.items():
        if col_name not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None, dtype=dtype).alias(col_name))
    
    # Casteamos y Seleccionamos
    final_cols = []
    for col_name, dtype in TEAMS_SILVER_SCHEMA.items():
        final_cols.append(pl.col(col_name).cast(dtype, strict=False))
        
    df_final = df_final.select(final_cols).unique()

    return df_final

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    from default_repo.utils.schemas import validate_schema, TEAMS_SILVER_SCHEMA, TEAMS_SILVER_CRITICAL
    assert output is not None, 'The output is undefined'
    
    # Validamos esquema
    validate_schema(output, TEAMS_SILVER_SCHEMA, TEAMS_SILVER_CRITICAL)