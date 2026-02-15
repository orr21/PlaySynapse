import os
import polars as pl

@data_exporter
def export_players_to_delta_silver(df, **kwargs):
    if df.height == 0:
        return

    # 1. Aseguramos que la columna 'season' existe (viene del transformer)
    # Ya no necesitamos crear year/month si vamos a particionar por season
    if "season" not in df.columns:
        raise ValueError("La columna 'season' es necesaria para el particionamiento.")

    # 2. Obtenemos las temporadas únicas para el predicado
    # Esto asegura que el overwrite solo afecte a las temporadas presentes en el DataFrame
    seasons = df.select("season").unique().to_series().to_list()
    
    # Construimos el predicado: (season = '2024-25') OR (season = '2025-26')
    predicate = " OR ".join([f"season = '{s}'" for s in seasons])

    storage_options = {
        "AWS_ENDPOINT_URL": os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        "AWS_ACCESS_KEY_ID": os.getenv('MINIO_ROOT_USER', 'admin'),
        "AWS_SECRET_ACCESS_KEY": os.getenv('MINIO_ROOT_PASSWORD', 'password123'),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    # 3. Escritura Delta particionada por Season
    delta_write_options = {
        "partition_by": ["season"],
        "predicate": predicate,
        "schema_mode": "overwrite"
    }

    try:
        df.write_delta(
            "s3://silver/nba/players/",
            mode="overwrite", 
            storage_options=storage_options,
            overwrite_schema=True,
            delta_write_options=delta_write_options
        )
        print(f"✅ Silver Players actualizado por Season. Predicado: {predicate}")
    except Exception as e:
        if "partition" in str(e).lower():
            print("⚠️ Error de partición. Si antes particionabas por year/month, debes borrar s3://silver/nba/players/ en MinIO para cambiar el esquema a 'season'.")
        raise e