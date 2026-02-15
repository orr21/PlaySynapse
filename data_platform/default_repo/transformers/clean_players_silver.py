"""
Transformer: clean_players_silver.

This block transforms raw Bronze Player data into the Silver layer schema (SCD Type 2).
It explodes the JSON structure, maps headers to columns, and ensures schema compliance.
"""

import polars as pl
from datetime import datetime
from typing import Any, Dict, List
from default_repo.utils.schemas import PLAYERS_SILVER_SCHEMA, PLAYERS_SILVER_CRITICAL

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform_to_weekly_scd2(df_bronze: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
    """
    Transforms Bronze Player data to Silver SCD2 format.

    Args:
        df_bronze (pl.DataFrame): Input DataFrame from Bronze layer.

    Returns:
        pl.DataFrame: Transformed DataFrame ready for Silver layer.
    """
    # 1. Decodificación del JSON y extracción de la temporada
    # Asumimos que df_bronze tiene las columnas: ["season", "raw_content"]
    df_transformed = df_bronze.with_columns([
        pl.col("raw_content").str.json_decode()
    ]).with_columns([
        pl.col("raw_content").struct.field("resultSets").list.get(0).alias("result_set")
    ])

    # 2. Explode: Una fila por registro (jugador)
    # Mantenemos la columna "season" durante el select
    df_exploded = df_transformed.select([
        pl.col("season"), # <--- Mantenemos la temporada aquí
        pl.col("result_set").struct.field("headers").alias("headers"),
        pl.col("result_set").struct.field("rowSet").alias("rows")
    ]).explode("rows")

    # 3. Mapeo Dinámico
    headers_list = df_exploded["headers"].to_list()[0]
    
    df_final = df_exploded.with_columns([
        pl.col("rows").list.get(i).alias(header.lower())
        for i, header in enumerate(headers_list)
    ])

    # 4. Lógica de Fechas y Limpieza Final
    df_final = df_final.with_columns(
        pl.lit(datetime.now().date()).alias("ingested_date")
    )
    
    # Garantizar cumplimiento estricto del PLAYERS_SILVER_SCHEMA
    
    # 1. Crear columnas faltantes como Nulos
    for col_name, dtype in PLAYERS_SILVER_SCHEMA.items():
        if col_name not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None, dtype=dtype).alias(col_name))

    # 2. Castear y Seleccionar
    final_cols = []
    for col_name, dtype in PLAYERS_SILVER_SCHEMA.items():
        final_cols.append(pl.col(col_name).cast(dtype, strict=False))

    return df_final.select(final_cols).unique()

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    from default_repo.utils.schemas import validate_schema
    assert output is not None, 'The output is undefined'
    
    # Validamos esquema
    validate_schema(output, PLAYERS_SILVER_SCHEMA, PLAYERS_SILVER_CRITICAL)