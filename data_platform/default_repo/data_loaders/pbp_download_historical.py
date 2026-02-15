import requests
import json
import polars as pl
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

def fetch_single_game(game):
    """Descarga un solo juego (igual que antes)"""
    gid = game['GAME_ID']
    season = game.get('SEASON', 'N/A')
    
    # URL del CDN (más rápido y estable que la API dinámica)
    cdn_url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{gid}.json"
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    
    try:
        # Timeout corto para conectar, largo para leer
        response = session.get(cdn_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=(3, 10))
        if response.status_code == 200:
            return {
                "game_id": gid,
                "season": season,
                "ingested_at": datetime.now().isoformat(),
                "raw_content": json.dumps(response.json()) # Esto ocupa mucha RAM en Python
            }
    except Exception:
        return None
    finally:
        session.close()
    return None

@data_loader
def download_pbp_chunked(df_games, *args, **kwargs):
    game_list = df_games.to_dicts()
    total_games = len(game_list)
    
    # CONFIGURACIÓN
    BATCH_SIZE = 50   
    MAX_WORKERS = 8   
    THREAD_TIMEOUT = 30  # Segundos máximos que permitimos a UN hilo trabajar
    
    dfs_chunks = []   

    def chunked_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    pbar = tqdm(total=total_games, desc="Descargando PBP", unit="games")

    # Usamos el Context Manager del Executor fuera del loop para reutilizar hilos
    # o dentro si queremos limpieza total. Para evitar "hilos colgados", 
    # lo manejaremos con cautela.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for batch in chunked_list(game_list, BATCH_SIZE):
            batch_results = []
            
            # Mapeamos los futuros
            future_to_game = {executor.submit(fetch_single_game, g): g for g in batch}
            
            try:
                # La clave está aquí: as_completed con timeout global del lote
                # Si el lote tarda más de (TIMEOUT * BATCH), algo va mal
                for future in as_completed(future_to_game, timeout=THREAD_TIMEOUT + 10):
                    try:
                        res = future.result() # Aquí podrías poner otro timeout si quisieras
                        if res:
                            batch_results.append(res)
                    except Exception as e:
                        print(f"❌ Error en hilo: {e}")
                    finally:
                        pbar.update(1)
            
            except TimeoutError:
                print(f"\n⚠️ Timeout alcanzado en el lote. Saltando hilos colgados...")
                # Nota: Los hilos colgados seguirán en segundo plano hasta que el proceso muera,
                # pero el flujo de tu programa principal continuará.

            if batch_results:
                dfs_chunks.append(pl.DataFrame(batch_results))
                del batch_results 
            
    pbar.close()

    if not dfs_chunks:
        return pl.DataFrame()

    return pl.concat(dfs_chunks)