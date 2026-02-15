from default_repo.utils.connectors.basketball.league.nba import NbaConnector
import polars as pl
import time
from datetime import datetime, timedelta
import json

@data_loader
def load_multi_season_teams(*args, **kwargs):
    connector = NbaConnector()
    # Definimos el rango de temporadas que quieres traer
    seasons = ['2023-24', '2024-25']
    
    all_season_responses = []

    for season in seasons:
        print(f"📥 Downloading Teams Profile - Season: {season}")
        
        # Hacemos la petición sin DateFrom/DateTo para obtener 
        # la ficha más actualizada del jugador en esa temporada.
        json_response = connector.fetch_data(
            metric_type='season_teams', 
            season=season, 
            perMode='Totals' 
        )
        
        if json_response:
            # Guardamos el contenido indicando a qué temporada pertenece
            all_season_responses.append({
                "season": season,
                "raw_content": json.dumps(json_response),
                "ingested_at": datetime.now().isoformat()
            })
        
        # Respetamos el límite de la API para no ser bloqueados
        time.sleep(1.5)
            
    return pl.DataFrame(all_season_responses)