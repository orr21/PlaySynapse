"""
NBA Gold Analytics Dashboard (Streamlit).

This dashboard visualizes data from the Gold layer (Delta Lake/MinIO), allowing users
to filter by season, date, game, and player. It features interactive tables and
radar charts for performance analysis.
"""

import streamlit as st
import polars as pl
import plotly.graph_objects as go
import os
from datetime import date
from typing import Optional, List, Dict, Any

st.set_page_config(page_title="NBA Gold Analytics", layout="wide")

NBA_GOLD = "#FFD700"
st.markdown(
    f"""
    <style>
    .player-title-small {{ font-size: 20px; font-weight: 800; color: {NBA_GOLD}; margin-bottom: 0px; }}
    .stats-table {{ font-size: 12px; width: 100%; color: #ddd; border-collapse: collapse; }}
    .stats-table td {{ padding: 2px; border-bottom: 1px solid #333; }}
    div.stButton > button:first-child {{ background-color: #2b2b2b; color: white; border: 1px solid #FFD700; width: 100%; }}
    </style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=3600)
def get_nba_data() -> pl.DataFrame:
    """
    Loads NBA Play-by-Play Gold data from MinIO (Delta Lake).

    Returns:
        pl.DataFrame: The loaded data, or an empty DataFrame on error.
    """
    storage_options = {
        "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "password123"),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }
    try:
        path = "s3://gold/nba/pbp/"
        df = pl.read_delta(path, storage_options=storage_options)
        return df.with_columns(
            [
                pl.col("game_date").cast(pl.Date),
                pl.col("season").cast(pl.Utf8),
                pl.col("game_id").cast(pl.Utf8).str.strip_chars(),
                pl.col("player_id").cast(pl.Utf8),
            ]
        )
    except Exception as e:
        st.error(f"Error loading Delta Lake: {e}")
        return pl.DataFrame()


df_all = get_nba_data()


st.sidebar.title("🏀 NBA Control Panel")


if st.sidebar.button("🔄 Reload Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()

if not df_all.is_empty():

    seasons = df_all["season"].unique().sort(descending=True).to_list()
    sel_season = st.sidebar.selectbox("📅 Season:", seasons, key="season_key")

    df_season = df_all.filter(pl.col("season") == sel_season)

    use_date = st.sidebar.checkbox("Filter by specific day", value=False)

    if use_date:
        max_d = df_season["game_date"].max()
        sel_date = st.sidebar.date_input("Day:", max_d, key="date_key")
        df_period = df_season.filter(pl.col("game_date") == sel_date)
    else:
        df_period = df_season

    games_meta = df_period.unique(subset=["game_id"]).sort(
        ["game_date", "team"], descending=True
    )

    game_map = {"ALL": "All games"}
    for row in games_meta.iter_rows(named=True):
        gid = row["game_id"]
        t1, t2 = sorted([row["team"], row["opponent"]])
        date_str = row["game_date"].strftime("%d/%m")
        label = f"{t1} vs {t2} ({date_str})"
        game_map[gid] = label

    available_ids = list(game_map.keys())

    if st.session_state.get("sel_game") not in available_ids:
        st.session_state["sel_game"] = "ALL"

    selected_game_id = st.sidebar.selectbox(
        "🏟️ Select Game:",
        options=available_ids,
        format_func=lambda x: game_map[x],
        key="sel_game",
    )

    if selected_game_id != "ALL":
        df_final = df_period.filter(pl.col("game_id") == selected_game_id)
    else:
        df_final = df_period

    search = st.sidebar.text_input("🔍 Search player:").strip().lower()
    if search:
        df_final = df_final.filter(
            pl.col("player_name").str.to_lowercase().str.contains(search)
        )

    df_final = df_final.sort("pts", descending=True)

    st.sidebar.markdown("---")
    with st.sidebar.expander("📚 Glossary & Method", expanded=False):
        st.markdown("""
        **Data Source:**
        
        NBA Stats API
        
        **Metrics:**
        * **PTS**: Points Scored
        * **REB**: Total Rebounds
        * **AST**: Assists
        * **FG%**: Field Goal Percentage (Made/Attempted)
        * **DEF**: Defensive Actions (Steals + Blocks)
        
        **Radar Chart Normalization:**
        Shapes are drawn relative to these 'Elite' benchmarks:
        * 🎯 **PTS**: 40+ = 100%
        * 🏀 **REB**: 15+ = 100%
        * 🤝 **AST**: 12+ = 100%
        * 🛡️ **DEF**: 6+ = 100%
        * 🔥 **EFF**: 100% FG = 100%
        """)

    st.title("🏟️ Game Center")

    if not df_final.is_empty():
        st.write(f"Showing {len(df_final)} records")

        display_cols = [
            "player_name",
            "team",
            "pts",
            "reb",
            "ast",
            "pct_fg",
            "game_date",
        ]

        if selected_game_id == "ALL":
            display_cols.insert(2, "opponent")

        event = st.dataframe(
            df_final.select(display_cols),
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            height=500,
            column_config={
                "player_name": st.column_config.TextColumn(
                    "Player", help="Full name of the player"
                ),
                "team": st.column_config.TextColumn(
                    "Team", help="Player's Team Tricode"
                ),
                "opponent": st.column_config.TextColumn(
                    "Opponent", help="Opponent Team Tricode"
                ),
                "pts": st.column_config.NumberColumn(
                    "PTS", format="%d", help="Points Scored"
                ),
                "reb": st.column_config.NumberColumn(
                    "REB", format="%d", help="Total Rebounds"
                ),
                "ast": st.column_config.NumberColumn(
                    "AST", format="%d", help="Total Assists"
                ),
                "pct_fg": st.column_config.ProgressColumn(
                    "FG%",
                    format="%.2f",
                    min_value=0,
                    max_value=1,
                    help="Field Goal Percentage",
                ),
                "game_date": st.column_config.DateColumn(
                    "Date", format="DD/MM/YY", help="Game Date"
                ),
            },
        )

        selected_indices = event.selection.rows
        if selected_indices:
            st.divider()
            st.subheader("📊 Performance Analysis")

            display_indices = selected_indices[:4]
            cols = st.columns(len(display_indices))

            for i, idx in enumerate(display_indices):
                try:
                    p = df_final.row(idx, named=True)

                    with cols[i]:
                        st.markdown(
                            f"<p class='player-title-small'>{p['player_name']}</p>",
                            unsafe_allow_html=True,
                        )
                        st.caption(f"{p['team']} vs {p['opponent']}")

                        st.image(
                            f"https://cdn.nba.com/headshots/nba/latest/1040x760/{p['player_id']}.png",
                            width=160,
                        )

                        st.markdown(
                            f"""
                        <table class='stats-table'>
                            <tr><td><b>PTS:</b> {p['pts']}</td><td><b>AST:</b> {p['ast']}</td></tr>
                            <tr><td><b>REB:</b> {p['reb']}</td><td><b>FG%:</b> {p['pct_fg']:.2f}</td></tr>
                        </table>
                        """,
                            unsafe_allow_html=True,
                        )

                        r_normalized = [
                            min(p["pts"] / 40, 1),
                            min(p["reb"] / 15, 1),
                            min(p["ast"] / 12, 1),
                            min((p["stl"] + p["blk"]) / 6, 1),
                            p["pct_fg"],
                        ]

                        r_absolute = [
                            p["pts"],
                            p["reb"],
                            p["ast"],
                            (p["stl"] + p["blk"]),
                            f"{p['pct_fg']*100:.1f}%",
                        ]

                        categories = ["PTS", "REB", "AST", "DEF", "EFF"]

                        fig = go.Figure(
                            data=go.Scatterpolar(
                                r=r_normalized,
                                theta=categories,
                                fill="toself",
                                fillcolor=NBA_GOLD,
                                line_color=NBA_GOLD,
                                opacity=0.5,
                                customdata=r_absolute,
                                hovertemplate="<b>%{theta}</b><br>Value: %{customdata}<extra></extra>",
                            )
                        )

                        fig.update_layout(
                            polar=dict(
                                radialaxis=dict(visible=False, range=[0, 1]),
                                angularaxis=dict(
                                    gridcolor="#444",
                                    tickfont=dict(size=10, color="white"),
                                ),
                            ),
                            height=250,
                            margin=dict(l=30, r=30, t=20, b=20),
                            paper_bgcolor="rgba(0,0,0,0)",
                            showlegend=False,
                        )
                        st.plotly_chart(
                            fig,
                            use_container_width=True,
                            key=f"radar_{p['player_id']}_{idx}",
                        )
                except Exception as e:
                    st.error(f"Error rendering player: {e}")
        else:
            st.info(
                "👆 Select players in the table to compare their performance radars."
            )
    else:
        st.warning("No data available with current filters.")
else:
    st.error("Could not load data from Gold layer.")
