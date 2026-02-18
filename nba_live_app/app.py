"""
NBA Live Commentary App (Gradio).

This application consumes real-time game events from Kafka, interactions with the Groq API
to generate "Andrés Montes" style commentary, and displays a live dashboard using Gradio.
"""

import gradio as gr
import json
import threading
import os
from kafka import KafkaConsumer
from groq import Groq
from datetime import datetime
import time
from typing import List, Dict, Any, Tuple, Optional

REDPANDA_TOPIC = "nba_gold_events"
REDPANDA_SERVER = os.getenv("REDPANDA_SERVER", "redpanda:9092")
groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))


class SharedState:
    """Thread-safe state for sharing data between Kafka consumer and UI."""

    def __init__(self):
        self.selected_game_id: Optional[str] = None
        self.game_list: Dict[str, str] = {}
        self.raw_events: List[Dict[str, Any]] = []
        self.visual_logs: List[Tuple[str, str, str, str]] = []
        self.current_score: str = "BUSCANDO SEÑAL..."
        self.lock = threading.Lock()
        self.game_histories: Dict[str, List[str]] = {}


state = SharedState()


def get_montes_commentary(ai_input: str, game_history: List[str] = None) -> str:
    """
    Generates NBA commentary using Groq LLM in the style of Andrés Montes.

    Args:
        ai_input (str): The structured text describing the event.
        game_history (List[str], optional): List of previous commentaries.

    Returns:
        str: Generated commentary.
    """
    messages = []

    messages.append(
        {
            "role": "system",
            "content": """
        Eres un comentarista estrella de la NBA en español estilo narración televisiva.
        Tono: emocionante, fluido y natural, como si estuvieras en directo.

        REGLAS IMPORTANTES:
        - No enumeres jugadas.
        - No leas estadísticas como robot.
        - Conecta lo que está pasando con el MOMENTUM del partido.
        - Si un jugador está en racha, destácalo.
        - Si hay cambio de dinámica, dilo.
        - Si es clutch time, transmite tensión.
        - Sé breve pero impactante (máx 4-5 frases).
        - Evita repetir frases recientes.
        - Usa energía narrativa.
        """,
        }
    )

    if game_history:
        for past_comment in game_history[-6:]:
            messages.append({"role": "assistant", "content": past_comment})

    messages.append({"role": "user", "content": ai_input})

    try:
        res = groq_client.chat.completions.create(
            messages=messages,
            model="llama-3.1-8b-instant",
            temperature=0.9,
            max_completion_tokens=300,
        )
        return res.choices[0].message.content
    except Exception:
        return "¡Daimiel, algo pasa en la centralita!"


def kafka_worker() -> None:
    """Background thread to consume events from Redpanda/Kafka."""
    try:
        consumer = KafkaConsumer(
            REDPANDA_TOPIC,
            bootstrap_servers=[REDPANDA_SERVER],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="latest",
        )
        for message in consumer:
            ev = message.value
            with state.lock:
                g_id = ev.get("game_id")
                g_name = ev.get("game_name")
                if g_id and g_id not in state.game_list:
                    state.game_list[g_id] = g_name
                state.raw_events.append(ev)
    except Exception as e:
        print(f"Error Kafka: {e}")


threading.Thread(target=kafka_worker, daemon=True).start()


def refresh_interface(selected_name: str) -> Tuple[str, str]:
    """
    Updates the UI with the latest events for the selected game.

    Args:
        selected_name (str): Name of the game selected in the dropdown.

    Returns:
        Tuple[str, str]: (Scoreboard Markdown, Feed HTML)
    """
    with state.lock:
        if not selected_name:
            return (
                "## ESPERANDO PARTIDO",
                "<div style='text-align:center;'>Selecciona un partido...</div>",
            )

        target_id = next(
            (gid for gid, name in state.game_list.items() if name == selected_name),
            None,
        )

        if target_id != state.selected_game_id:
            state.selected_game_id = target_id
            state.visual_logs = []
            state.current_score = "CONECTANDO..."

        relevant = [
            e for e in state.raw_events if e.get("game_id") == state.selected_game_id
        ]

        if relevant:
            combined_input = "\n---\n".join([e.get("ai_input") for e in relevant])

            if state.selected_game_id not in state.game_histories:
                state.game_histories[state.selected_game_id] = []

            narration = get_montes_commentary(
                combined_input,
                game_history=state.game_histories[state.selected_game_id],
            )

            state.game_histories[state.selected_game_id].append(narration)
            last_ev = relevant[-1]
            state.current_score = last_ev.get("score_display", "0-0")

            hora_sys = datetime.now().strftime("%H:%M:%S")
            nba_clock = f"Q{last_ev.get('period')} - {last_ev.get('clock')}"

            state.visual_logs.insert(
                0, (hora_sys, nba_clock, state.current_score, narration)
            )

            state.raw_events = [
                e
                for e in state.raw_events
                if e.get("game_id") != state.selected_game_id
            ]

        if not state.visual_logs:
            html = f"<div style='text-align:center; color:#8b949e;'>Esperando eventos de {selected_name}...</div>"
        else:
            html = "<div style='display: flex; flex-direction: column; gap: 15px;'>"
            for h, n, sc, txt in state.visual_logs[:12]:
                html += f"""
                <div class="commentary-card">
                    <div class="header-meta">
                        <span>[{h}] <b style="color:#ef4444; margin-left:10px;">{n}</b></span>
                        <span style="color:#58a6ff; font-weight:bold;">{sc}</span>
                    </div>
                    <div style="color: #f0f6fc; font-size: 1.15em; line-height: 1.5;">{txt}</div>
                </div>
                """
            html += "</div>"

        return f"## {state.current_score}", html


def update_dropdown():
    with state.lock:
        return gr.update(choices=list(state.game_list.values()))


custom_css = """
.container { background-color: #0b0e14; padding: 20px; }
.score-box { 
    background: linear-gradient(135deg, #1e293b 0%, #000000 100%);
    border: 2px solid #3b82f6;
    color: #3b82f6 !important;
    font-family: 'Courier New', monospace;
    font-size: 1.8em !important;
    text-align: center;
}
.commentary-card {
    background: #161b22;
    border-left: 5px solid #ef4444;
    padding: 18px;
    border-radius: 4px;
}
.header-meta { 
    color: #8b949e; 
    font-family: monospace; 
    font-size: 0.9em; 
    border-bottom: 1px solid #30363d;
    margin-bottom: 10px;
    padding-bottom: 5px;
    display: flex;
    justify-content: space-between;
}
"""

with gr.Blocks(css=custom_css) as demo:
    with gr.Column(elem_classes="container"):
        gr.Markdown("# 🏀 **NBA AI LIVE: EL CLUB DE LOS JUGONES**")

        with gr.Row():
            game_selector = gr.Dropdown(choices=[], label="Sintonizar Partido")
            scoreboard = gr.Markdown(
                value="## ESPERANDO PARTIDO", elem_classes="score-box"
            )

        gr.HTML("<hr style='border: 0.5px solid #333; margin: 20px 0;'>")
        feed = gr.HTML()

        dropdown_timer = gr.Timer(3.0)
        auto_refresh = gr.Timer(5.0)

        game_selector.change(
            refresh_interface, inputs=[game_selector], outputs=[scoreboard, feed]
        )

        dropdown_timer.tick(update_dropdown, outputs=[game_selector])
        auto_refresh.tick(
            refresh_interface, inputs=[game_selector], outputs=[scoreboard, feed]
        )

demo.launch(server_name="0.0.0.0", server_port=7860)
