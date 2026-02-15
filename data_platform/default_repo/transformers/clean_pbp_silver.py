import polars as pl
import json

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform_pbp_batch_optimized(df_bronze, *args, **kwargs):
    # Convertimos a lista de dicts para iterar rápido en Python puro
    raw_data = df_bronze.to_dicts()
    total_games = len(raw_data)
    
    print(f"⚙️ Procesando {total_games} partidos con estrategia de lotes...")

    # Aumenta el BATCH_SIZE si tienes RAM de sobra (ej. 1000 o 2000)
    # Si ves que va lento, bájalo a 500.
    BATCH_SIZE = 200 
    dfs_chunks = [] 

    for i in range(0, total_games, BATCH_SIZE):
        batch = raw_data[i : i + BATCH_SIZE]
        batch_actions = []

        for row in batch:
            try:
                # El json.loads de Python es muy rápido
                content = json.loads(row['raw_content'])
            except (json.JSONDecodeError, TypeError):
                continue
            
            actions = content.get('game', {}).get('actions', [])
            if not actions: continue

            # Inyección de metadatos ligera
            game_id = row['game_id']
            season = row.get('season', 'N/A')
            
            for action in actions:
                action['game_id'] = game_id
                action['season'] = season
                batch_actions.append(action)

        # Convertir a Polars por lote es casi instantáneo
        if batch_actions:
            chunk_df = pl.DataFrame(batch_actions, infer_schema_length=None) 
            # infer_schema_length=None acelera la carga al no escanear todo estrictamente
            
            # Normalización rápida
            chunk_df.columns = [c.lower() for c in chunk_df.columns]
            
            # Casteo preventivo para evitar errores al unir
            # Añade aquí otras columnas si fallan al unir
            # Casteo preventivo para evitar errores al unir
            # Definimos tipos base para las columnas clave
            cols_to_cast = {
                "actionnumber": pl.Int64, "period": pl.Int64, "personid": pl.Int64, 
                "teamid": pl.Int64, "possession": pl.Int64, "ordernumber": pl.Int64,
                "x": pl.Float64, "y": pl.Float64, "shotdistance": pl.Float64,
                "reboundtotal": pl.Int64, "pointstotal": pl.Int64, "assisttotal": pl.Int64,
                "turnovertotal": pl.Int64, "foulpersonaltotal": pl.Int64,
                "assistpersonid": pl.Int64, "stealpersonid": pl.Int64, "blockpersonid": pl.Int64,
                "officialid": pl.Int64, "jumpballwonpersonid": pl.Int64, "jumpballlostpersonid": pl.Int64,
                "jumpballrecoverdpersonid": pl.Int64, "fouldrawnpersonid": pl.Int64,
                "shotactionnumber": pl.Int64, "xlegacy": pl.Int64, "ylegacy": pl.Int64,
                "isfieldgoal": pl.Int64, "istargetscorelastperiod": pl.Int64,
                "rebounddefensivetotal": pl.Int64, "reboundoffensivetotal": pl.Int64,
                "foultechnicaltotal": pl.Int64
            }
            
            # Garantizar que todas las columnas del esquema existan
            # Si no vienen en el JSON (ej. jumpball en un lote sin saltos), las creamos como nulos
            for col_name, dtype in cols_to_cast.items():
                if col_name not in chunk_df.columns:
                    chunk_df = chunk_df.with_columns(pl.lit(None, dtype=dtype).alias(col_name))
            
            # Casteo seguro de las que sí existen o acabamos de crear
            chunk_df = chunk_df.with_columns([
                pl.col(c).cast(t, strict=False) for c, t in cols_to_cast.items()
            ])

            dfs_chunks.append(chunk_df)
            del batch_actions # Liberar RAM

        print(f"📦 Lote {i}/{total_games} completado.")

    if not dfs_chunks:
        return pl.DataFrame()

    print("🔄 Uniendo lotes...")
    df_final = pl.concat(dfs_chunks, how="diagonal")

    if "timeactual" in df_final.columns:
        df_final = df_final.with_columns(
            pl.col("timeactual")
            .str.to_datetime(strict=False)
            # Primero le decimos a Polars que estos datos vienen en UTC
            .dt.replace_time_zone("UTC")
            # Luego los convertimos a la hora de Nueva York
            .dt.convert_time_zone("America/New_York")
            .alias("action_timestamp")
        )

    if "action_timestamp" in df_final.columns:
        df_final = df_final.with_columns([
            pl.col("action_timestamp").dt.year().alias("year"),
            pl.col("action_timestamp").dt.month().alias("month"),
            pl.col("action_timestamp").dt.day().alias("day")
        ])
    else:
        print("⚠️ Advertencia: No se pudo generar action_timestamp. Rellenando particiones con nulos.")
        df_final = df_final.with_columns([
            pl.lit(None, dtype=pl.Int32).alias("year"),
            pl.lit(None, dtype=pl.Int8).alias("month"),
            pl.lit(None, dtype=pl.Int8).alias("day")
        ])

    print(f"✅ Hecho. {df_final.height} filas generadas. Columnas: {df_final.columns}")
    return df_final

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    from default_repo.utils.schemas import validate_schema, PBP_SILVER_SCHEMA, PBP_SILVER_CRITICAL
    assert output is not None, 'The output is undefined'
    
    # Validamos esquema
    validate_schema(output, PBP_SILVER_SCHEMA, PBP_SILVER_CRITICAL)