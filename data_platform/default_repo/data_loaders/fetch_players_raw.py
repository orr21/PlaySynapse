from default_repo.utils.connectors.basketball.league.nba import NbaConnector
import polars as pl
import time
from datetime import datetime, timedelta
import json

@data_loader
def load_current_season_players(*args, **kwargs):
    connector = NbaConnector()
    
    # 1. Calculamos la season actual dinámicamente
    now = datetime.now()
    # Si estamos antes de octubre, la temporada actual es (AñoAnterior)-(AñoActual corto)
    # Si estamos en octubre o después, es (AñoActual)-(AñoSiguiente corto)
    if now.month < 10:
        year_start = now.year - 1
        year_end = str(now.year)[2:]
    else:
        year_start = now.year
        year_end = str(now.year + 1)[2:]
    
    current_season = f"{year_start}-{year_end}"
    
    all_season_responses = []

    print(f"📥 Downloading Current Players Profile - Season: {current_season}")
    
    # Hacemos la petición para la temporada calculada
    json_response = connector.fetch_data(
        metric_type='season_players', 
        season=current_season, 
        perMode='Totals' 
    )
    
    if json_response:
        all_season_responses.append({
            "season": current_season,
            "raw_content": json.dumps(json_response),
            "ingested_at": now.isoformat()
        })
            
    return pl.DataFrame(all_season_responses)