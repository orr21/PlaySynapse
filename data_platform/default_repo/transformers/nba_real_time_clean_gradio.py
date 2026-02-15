"""
Transformer: nba_real_time_clean_gradio.

This block transforms real-time Kafka messages for the Gradio App.
It enriches events with AI prompts for commentary generation and formats the data.
"""

from typing import Dict, List, Any, Optional
import requests
import json
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

# Caché para evitar peticiones repetitivas al CDN de la NBA
CACHED_TEAMS = {}

def fetch_team_names(game_id: str) -> Optional[Dict[str, str]]:
    """Obtains team names and tricodes using the NBA boxscore."""
    if not game_id or game_id in CACHED_TEAMS:
        return CACHED_TEAMS.get(game_id)
    
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    try:
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200:
            game_data = res.json().get('game', {})
            h = game_data.get('homeTeam', {})
            a = game_data.get('awayTeam', {})
            info = {
                "home_full": f"{h.get('teamCity')} {h.get('teamName')}".strip() or "Local",
                "away_full": f"{a.get('teamCity')} {a.get('teamName')}".strip() or "Visitante",
                "home_tri": h.get('teamTricode', 'HOME'),
                "away_tri": a.get('teamTricode', 'AWAY')
            }
            CACHED_TEAMS[game_id] = info
            return info
    except Exception as e:
        print(f"⚠️ Error accessing NBA CDN ({game_id}): {e}")
    return None

def generar_prompt_nba(row: Dict[str, Any]) -> str:
    """Generates a structured prompt for the AI commentator based on the event row."""
    # --- 1. Datos Universales ---
    # Manejo de múltiples posibles nombres de columnas para el marcador
    h_score = row.get('scoreHome') or row.get('homeScore', 0)
    a_score = row.get('scoreAway') or row.get('awayScore', 0)
    marcador = f"{h_score}-{a_score}"
    
    reloj = row.get('clock', '00:00')
    periodo = row.get('period', 1)
    jugador = row.get('playerName', 'Jugador')
    equipo = row.get('teamTricode', 'N/A')
    tipo = str(row.get('actionType', '')).lower()
    subtipo = str(row.get('subType', '')).lower() if row.get('subType') else "acción"
    
    # --- 2. Construcción de la Ficha (Estructura Base) ---
    ficha_dinamica = [
        "FICHA TÉCNICA DE ACCIÓN:",
        f"- TIPO: {tipo.upper()} ({subtipo})",
        f"- PROTAGONISTA: {jugador} ({equipo})",
        f"- CONTEXTO: Cuarto {periodo} | Reloj: {reloj} | Marcador: {marcador}"
    ]

    # --- 3. Lógica Segmentada por Tipo de Acción ---
    
    # TIROS (2pt, 3pt, shot)
    if tipo in ['2pt', '3pt', 'shot']:
        resultado = str(row.get('shotResult', '')).upper()
        ficha_dinamica.append(f"- RESULTADO: {resultado}")
        
        if resultado == "MADE":
            pts = 3 if '3pt' in tipo else 2
            ficha_dinamica.append(f"- PUNTOS EN LA ACCIÓN: +{pts}")
            ficha_dinamica.append(f"- PUNTOS TOTALES JUGADOR: {row.get('pointsTotal', 'N/A')}")
            if row.get('shotDistance'):
                ficha_dinamica.append(f"- DISTANCIA: {row.get('shotDistance')} pies")
            if row.get('assistPlayerNameInitial'):
                ficha_dinamica.append(f"- ASISTENCIA DE: {row.get('assistPlayerNameInitial')} (Total: {row.get('assistTotal', 0)})")
    
    # TIROS LIBRES
    elif tipo == 'freethrow':
        resultado = str(row.get('shotResult', '')).upper()
        ficha_dinamica.append(f"- INTENTO: {subtipo}")
        ficha_dinamica.append(f"- RESULTADO: {resultado}")
        if resultado == "MADE":
            ficha_dinamica.append(f"- PUNTOS TOTALES JUGADOR: {row.get('pointsTotal', 'N/A')}")

    # REBOTES
    elif tipo == 'rebound':
        ficha_dinamica.append(f"- CLASE: {subtipo.upper()}")
        ficha_dinamica.append(f"- TOTAL REBOTES JUGADOR: {row.get('reboundTotal', 'N/A')}")

    # SALTO INICIAL
    elif tipo == 'jumpball':
        ficha_dinamica.append(f"- GANADOR: {row.get('jumpBallWonPlayerName', 'N/A')}")
        ficha_dinamica.append(f"- PERDEDOR: {row.get('jumpBallLostPlayerName', 'N/A')}")
        ficha_dinamica.append(f"- RECUPERADO POR: {row.get('jumpBallRecoveredName', 'N/A')}")

    # DEFENSIVOS (Block / Steal)
    elif tipo in ['block', 'steal']:
        actor = row.get('blockPlayerName') if tipo == 'block' else row.get('stealPlayerName')
        ficha_dinamica.append(f"- DEFENSOR: {actor}")
        ficha_dinamica.append(f"- SOBRE: {jugador}")

    # FALTAS
    elif tipo == 'foul':
        ficha_dinamica.append(f"- INFRACTOR: {jugador}")
        ficha_dinamica.append(f"- RECIBE FALTA: {row.get('foulDrawnPlayerName', 'N/A')}")
        ficha_dinamica.append(f"- FALTAS TOTALES: {row.get('foulPersonalTotal', 'N/A')}")

    # PÉRDIDAS (Turnover)
    elif tipo == 'turnover':
        ficha_dinamica.append(f"- CAUSA: {subtipo.upper()}")
        ficha_dinamica.append(f"- PÉRDIDAS TOTALES JUGADOR: {row.get('turnoverTotal', 'N/A')}")

    return "\n".join(ficha_dinamica)

@transformer
def transform(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """
    Transforms a batch of Kafka messages into enriched events for the UI.

    Args:
        messages (List[Dict]): List of Kafka messages.

    Returns:
        List[Dict]: Enriched events with AI prompts.
    """
    transformed_batch = []

    for msg in messages:
        # Mage entrega: {'data': {JSON_KAFKA}, 'metadata': {CLAVES_KAFKA}}
        data = msg.get('data', {})
        meta = msg.get('metadata', {})

        # EXTRAER GAME_ID: Fundamental para que el pipeline no se detenga
        # Buscamos en data, meta o decodificamos la key de Kafka
        raw_key = meta.get('key')
        if isinstance(raw_key, bytes):
            raw_key = raw_key.decode('utf-8')
            
        g_id = data.get('gameId') or data.get('game_id') or raw_key
        
        # Si no hay g_id, usamos el de tu log para evitar el 'continue'
        if not g_id:
            g_id = "0022300650"

        # Obtener nombres reales de equipos
        teams = fetch_team_names(str(g_id))
        h_name = teams['home_full'] if teams else "Equipo Local"
        a_name = teams['away_full'] if teams else "Equipo Visitante"

        # Limpiar el formato del reloj (PT08M24.00S -> 08:24)
        clock_raw = data.get('clock', '00:00')
        clock = clock_raw
        if "PT" in str(clock_raw):
            try:
                clock = clock_raw.replace("PT", "").replace("M", ":").replace("S", "")
                p = clock.split(":")
                clock = f"{p[0].zfill(2)}:{p[1][:2]}"
            except:
                pass
            
        period = data.get('period', "0")


        # Construcción del evento para la capa Gold (Redpanda)
        enriched_event = {
            "game_id": str(g_id),
            "game_name": f"{h_name} vs {a_name}",
            "score_display": f"{h_name} {data.get('scoreHome', 0)} - {data.get('scoreAway', 0)} {a_name}",
            "clock": clock,
            "period": period,
            "description": data.get('description', 'Acción de juego'),
            "ai_input": generar_prompt_nba(data),
            "ingestion_time": datetime.now().isoformat()
        }

        if enriched_event["description"]:
            transformed_batch.append(enriched_event)

    print(f"✅ Transformador completado: {len(transformed_batch)} mensajes procesados.")
    return transformed_batch
