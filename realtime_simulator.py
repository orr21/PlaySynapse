"""
Real-Time NBA Simulator.

This script simulates a live NBA game by fetching Play-by-Play data from the NBA CDN
and replaying it to a Kafka topic ('nba_live') at a controlled speed.
It handles time synchronization to mimic real-time data ingestion.
"""

import json
import requests
import socket
import time
import re
import threading
from confluent_kafka import Producer
import os
from typing import List, Dict, Any, Optional

GAMES_TO_SIMULATE = [
    {"id": "0022300650", "name": "Lakers vs Celtics"},
]

TOPIC_NAME = "nba_live"


SPEED_MULTIPLIER = 1.0


QUARTER_BREAK_SECONDS = 5.0

KAFKA_CONF = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "localhost:19092"),
    "client.id": socket.gethostname(),
}

CDN_URL_TEMPLATE = (
    "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{}.json"
)


def clock_to_seconds(clock_str: str) -> float:
    """
    Converts NBA API clock format (PT12M00.00S) to remaining seconds.

    Args:
        clock_str (str): Clock string from API.

    Returns:
        float: Remaining seconds in the period.
    """
    if not clock_str:
        return 0.0

    match = re.search(r"PT(\d+)M(\d+\.?\d*)S", clock_str)
    if match:
        return (int(match.group(1)) * 60) + float(match.group(2))
    return 0.0


def get_period_duration(period: int) -> int:
    """
    Returns total duration of a period in seconds.

    Args:
        period (int): Period number (1-4 for regular, >4 for OT).

    Returns:
        int: Duration in seconds (720 for regular, 300 for OT).
    """
    return 300 if period > 4 else 720


def run_game_simulation(game_info: Dict[str, str], producer: Producer) -> None:
    """
    Simulates a single game execution in a separate thread.

    Args:
        game_info (Dict): Dictionary with 'id' and 'name'.
        producer (Producer): Kafka Producer instance.
    """
    game_id = game_info["id"]
    game_name = game_info.get("name", game_id)

    try:
        url = CDN_URL_TEMPLATE.format(game_id)
        print(f"📥 [{game_name}] Descargando datos...")
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            print(f"❌ [{game_name}] Error HTTP {response.status_code}")
            return

        actions = response.json()["game"]["actions"]

        actions.sort(
            key=lambda x: (
                x.get("period", 1),
                -clock_to_seconds(x.get("clock", "PT12M00.00S")),
                x.get("actionNumber", 0),
            )
        )

        print(f"🚀 [{game_name}] Iniciando simulación sincronizada...")

        current_period = 0
        quarter_start_real_time = 0

        for action in actions:
            action_period = action.get("period")

            clock_remaining = clock_to_seconds(action.get("clock"))

            if action_period != current_period:
                if current_period != 0:
                    print(
                        f"⏸️  [{game_name}] Fin del Q{current_period}. Pausa de {QUARTER_BREAK_SECONDS}s..."
                    )
                    time.sleep(QUARTER_BREAK_SECONDS)

                current_period = action_period

                quarter_start_real_time = time.time()
                print(f"▶️  [{game_name}] Inicio Q{current_period}")

            period_length = get_period_duration(current_period)
            game_seconds_elapsed = period_length - clock_remaining

            target_real_time = quarter_start_real_time + (
                game_seconds_elapsed / SPEED_MULTIPLIER
            )

            now = time.time()
            wait_time = target_real_time - now

            if wait_time > 0:
                time.sleep(wait_time)

            producer.produce(
                TOPIC_NAME, key=str(game_id), value=json.dumps(action).encode("utf-8")
            )

            producer.poll(0)

            clean_clock = (
                action.get("clock").replace("PT", "").replace("M", ":").replace("S", "")
            )
            print(
                f"🏀 [{game_name}] Q{current_period} {clean_clock} | {action.get('actionType')} - {action.get('description')}"
            )

    except Exception as e:
        print(f"❌ [{game_name}] Error crítico: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    shared_producer = Producer(KAFKA_CONF)
    threads = []

    for game in GAMES_TO_SIMULATE:
        t = threading.Thread(target=run_game_simulation, args=(game, shared_producer))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    shared_producer.flush()
    print("🏁 Fin de la transmisión.")
