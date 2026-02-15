import boto3
import os
import polars as pl

@data_exporter
def export_to_bronze(df, **kwargs):
    if df.height == 0:
        print("⚠️ No hay datos para exportar a Bronze.")
        return

    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
        aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'admin'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'password123'),
        region_name='us-east-1'
    )

    if isinstance(df['ingested_at'][0], str):
        df = df.with_columns(
            pl.col("ingested_at").str.to_datetime()
        )
    
    # Iteramos sobre el DataFrame
    for row in df.to_dicts():
        gid = row['game_id']
        # Usamos el timestamp de ingesta para la ruta
        # row['ingested_at'] es un objeto datetime
        ts = row['ingested_at']
        
        # Estructura dinámica: nba/pbp/YYYY/MM/DD/ID_raw.json
        key = (
            f"nba/pbp/"
            f"{ts.year}/"
            f"{ts.month:02d}/"
            f"{ts.day:02d}/"
            f"{gid}_raw.json"
        )

        print(key)
        
        s3_client.put_object(
            Bucket='bronze',
            Key=key,
            Body=row['raw_content'],
            ContentType='application/json'
        )
    
    print(f"✅ {df.height} partidos guardados en Bronze con rutas dinámicas.")