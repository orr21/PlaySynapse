"""
Schemas Module.

Defines the Polars Schemas for Silver and Gold layers.
Includes validation logic to enforce data quality and consistency.
"""

import polars as pl

# ==========================================
# 🥈 SILVER LAYER SCHEMAS
# ==========================================

PBP_SILVER_SCHEMA = {
    # Keys & Metadata
    "game_id": pl.Utf8,
    "season": pl.Utf8,
    "actionnumber": pl.Int64,
    "ordernumber": pl.Int64,
    "action_timestamp": pl.Datetime("us", "America/New_York"),
    "year": pl.Int32,
    "month": pl.Int8,
    "day": pl.Int8,
    
    # Time & Period
    "clock": pl.Utf8,
    "timeactual": pl.Utf8,
    "period": pl.Int64,
    "periodtype": pl.Utf8,
    
    # Event Info
    "actiontype": pl.Utf8,
    "subtype": pl.Utf8,
    "description": pl.Utf8,
    "qualifiers": pl.List(pl.Utf8), # Usually a list of strings
    "descriptor": pl.Utf8,
    "edited": pl.Utf8,
    "isfieldgoal": pl.Int64,
    "istargetscorelastperiod": pl.Int64,
    
    # Coordinates & Area
    "x": pl.Float64,
    "y": pl.Float64,
    "xlegacy": pl.Int64,
    "ylegacy": pl.Int64,
    "area": pl.Utf8,
    "areadetail": pl.Utf8,
    "side": pl.Utf8,
    "shotdistance": pl.Float64,
    "shotresult": pl.Utf8,
    
    # Team & Score
    "teamid": pl.Int64,
    "teamtricode": pl.Utf8,
    "possession": pl.Int64,
    "scorehome": pl.Utf8,
    "scoreaway": pl.Utf8,
    
    # Persons involved (Main)
    "personid": pl.Int64,
    "playername": pl.Utf8,
    "playernamei": pl.Utf8, # Initials/Short name
    "personidsfilter": pl.List(pl.Int64),
    
    # Specific Event Details - Jumpball
    "jumpballrecoveredname": pl.Utf8,
    "jumpballrecoverdpersonid": pl.Int64,
    "jumpballwonplayername": pl.Utf8,
    "jumpballwonpersonid": pl.Int64,
    "jumpballlostplayername": pl.Utf8,
    "jumpballlostpersonid": pl.Int64,
    
    # Shot/Rebound
    "shotactionnumber": pl.Int64,
    "reboundtotal": pl.Int64,
    "rebounddefensivetotal": pl.Int64,
    "reboundoffensivetotal": pl.Int64,
    "pointstotal": pl.Int64,
    
    # Assist
    "assistpersonid": pl.Int64,
    "assistplayernameinitial": pl.Utf8,
    "assisttotal": pl.Int64,
    
    # Fouls & Officials
    "officialid": pl.Int64,
    "foulpersonaltotal": pl.Int64,
    "foultechnicaltotal": pl.Int64,
    "fouldrawnpersonid": pl.Int64,
    "fouldrawnplayername": pl.Utf8,
    
    # Turnovers/Steals/Blocks
    "turnovertotal": pl.Int64,
    "stealpersonid": pl.Int64,
    "stealplayername": pl.Utf8,
    "blockpersonid": pl.Int64,
    "blockplayername": pl.Utf8
}

PLAYERS_SILVER_SCHEMA = {
    "season": pl.Utf8,
    "player_id": pl.Int64,
    "player_name": pl.Utf8,
    "team_id": pl.Int64,
    "team_abbreviation": pl.Utf8,
    "age": pl.Float64,
    "player_height": pl.Utf8,
    "player_height_inches": pl.Float64,
    "player_weight": pl.Utf8,
    "college": pl.Utf8,
    "country": pl.Utf8,
    "draft_year": pl.Utf8,
    "draft_round": pl.Utf8,
    "draft_number": pl.Utf8,
    "gp": pl.Int64,
    "pts": pl.Float64,
    "reb": pl.Float64,
    "ast": pl.Float64,
    "net_rating": pl.Float64,
    "oreb_pct": pl.Float64,
    "dreb_pct": pl.Float64,
    "usg_pct": pl.Float64,
    "ts_pct": pl.Float64,
    "ast_pct": pl.Float64,
    "ingested_date": pl.Date
}

TEAMS_SILVER_SCHEMA = {
    # Identity & Meta
    "teamid": pl.Int64,
    "teamcity": pl.Utf8,
    "teamname": pl.Utf8,
    "full_name": pl.Utf8,
    "abbreviation": pl.Utf8, # Usually generic, mapped from 'team_abbreviation' if avail or derived
    "teamslug": pl.Utf8,
    "logo_url": pl.Utf8,
    "season": pl.Utf8,
    "ingested_date": pl.Date,
    
    # Conference / Division Info
    "leagueid": pl.Utf8,
    "conference": pl.Utf8,
    "division": pl.Utf8,
    "leaguerank": pl.Int64,
    "playoffrank": pl.Int64,
    "playoffseeding": pl.Utf8, # Sometimes null or distinct from rank
    "clinchindicator": pl.Utf8, 
    "clinchedconferencetitle": pl.Int64,
    "clincheddivisiontitle": pl.Int64,
    "clinchedplayoffbirth": pl.Int64,
    "clinchedplayin": pl.Int64, 
    "clinchedpostseason": pl.Int64,
    "eliminatedconference": pl.Int64,
    "eliminateddivision": pl.Int64,
    
    # Records & Standings
    "wins": pl.Int64,
    "losses": pl.Int64,
    "winpct": pl.Float64,
    "record": pl.Utf8,
    "conferencerecord": pl.Utf8,
    "divisionrecord": pl.Utf8,
    "conferencegamesback": pl.Float64,
    "divisiongamesback": pl.Float64,
    "leaguegamesback": pl.Float64,
    
    # Streaks
    "home": pl.Utf8, # Record Home
    "road": pl.Utf8, # Record Road
    "l10": pl.Utf8,
    "last10home": pl.Utf8,
    "last10road": pl.Utf8,
    "ot": pl.Utf8, # Overtime Record
    "threeptsorless": pl.Utf8,
    "tenptsormore": pl.Utf8,
    "longhomestreak": pl.Int64,
    "strlonghomestreak": pl.Utf8,
    "longroadstreak": pl.Int64,
    "strlongroadstreak": pl.Utf8,
    "longwinstreak": pl.Int64,
    "longlossstreak": pl.Int64,
    "currenthomestreak": pl.Int64,
    "strcurrenthomestreak": pl.Utf8,
    "currentroadstreak": pl.Int64,
    "strcurrentroadstreak": pl.Utf8,
    "currentstreak": pl.Int64,
    "strcurrentstreak": pl.Utf8,
    
    # Performance Stats (Ahead/Behind)
    "aheadathalf": pl.Utf8,
    "behindathalf": pl.Utf8,
    "tiedathalf": pl.Utf8,
    "aheadatthird": pl.Utf8,
    "behindatthird": pl.Utf8,
    "tiedatthird": pl.Utf8,
    "score100pts": pl.Utf8,
    "oppscore100pts": pl.Utf8,
    "oppover500": pl.Utf8,
    "leadinfgpct": pl.Utf8,
    "leadinreb": pl.Utf8,
    "fewerturnovers": pl.Utf8,
    "neutral": pl.Utf8,
    
    # Monthly Breakdown (Records)
    "jan": pl.Utf8, "feb": pl.Utf8, "mar": pl.Utf8, "apr": pl.Utf8, 
    "may": pl.Utf8, "jun": pl.Utf8, "jul": pl.Utf8, "aug": pl.Utf8, 
    "sep": pl.Utf8, "oct": pl.Utf8, "nov": pl.Utf8, "dec": pl.Utf8,
    
    # Points Stats
    "pointspg": pl.Float64,
    "opppointspg": pl.Float64,
    "diffpointspg": pl.Float64,
    "totalpoints": pl.Int64,
    "opptotalpoints": pl.Int64,
    "difftotalpoints": pl.Int64,
    "score_80_plus": pl.Utf8,
    "opp_score_80_plus": pl.Utf8,
    "score_below_80": pl.Utf8,
    "opp_score_below_80": pl.Utf8,
    
    # VS Opponents
    "vseast": pl.Utf8,
    "vsatlantic": pl.Utf8,
    "vscentral": pl.Utf8,
    "vssoutheast": pl.Utf8,
    "vswest": pl.Utf8,
    "vsnorthwest": pl.Utf8,
    "vspacific": pl.Utf8,
    "vssouthwest": pl.Utf8
}

# ==========================================
# 🥇 GOLD LAYER SCHEMAS
# ==========================================

DASHBOARD_GOLD_SCHEMA = {
    # Dimensiones
    "game_id": pl.Utf8,
    "player_id": pl.Int64,
    "player_name": pl.Utf8,
    "team": pl.Utf8,
    "game_date": pl.Date,
    "season": pl.Utf8,
    "opponent": pl.Utf8,
    
    # Métricas
    "pts": pl.Int64,
    "reb": pl.Int64,
    "ast": pl.Int64,
    "stl": pl.Int64,
    "blk": pl.Int64,
    "tov": pl.Int64,
    
    # Tiro
    "fga": pl.Int64,
    "fgm": pl.Int64,
    "3pa": pl.Int64,
    "3pm": pl.Int64,
    "fta": pl.Int64,
    "ftm": pl.Int64,
    
    # Porcentajes y calculados
    "pct_fg": pl.Float64,
    "pct_3": pl.Float64,
    "pct_ft": pl.Float64,
    "plus_minus": pl.Int32
}



# ==========================================
# 🚨 CRITICAL COLUMNS (NON-NULLABLE)
# ==========================================
PBP_SILVER_CRITICAL = ["game_id", "actionnumber", "period"]
PLAYERS_SILVER_CRITICAL = ["player_id", "season"]
TEAMS_SILVER_CRITICAL = ["teamid", "season"]
DASHBOARD_GOLD_CRITICAL = ["game_id", "player_id", "points_value"] 
# Note: points_value is critical for sum, but in gold 'pts' is the output. 
# Gold criticals:
DASHBOARD_GOLD_OUTPUT_CRITICAL = ["game_id", "player_id", "pts", "season"]

# Helper para validación en tests
def validate_schema(df: pl.DataFrame, schema_dict: dict, critical_columns: list = None) -> None:
    """
    Valida que un DataFrame de Polars cumpla estrictamente con el esquema dado.
    Optionalmente verifica que las 'critical_columns' no tengan nulos.
    Lanza AssertionError si hay discrepancias.
    """
    missing_cols = []
    wrong_types = []
    null_criticals = []

    for col_name, expected_type in schema_dict.items():
        if col_name not in df.columns:
            missing_cols.append(col_name)
            continue
        
        # Obtenemos el tipo real
        actual_type = df.schema[col_name]
        
        # Comparación flexible para Datetimes
        if actual_type != expected_type:
             # Permitir mismatch si ambos son Float (ej Float32 vs Float64) o Int, 
             # pero por ahora somos estrictos como pide el usuario.
             wrong_types.append(f"{col_name}: Expected {expected_type}, got {actual_type}")

    # Validación de cruciales
    if critical_columns:
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].null_count()
                if null_count > 0:
                    null_criticals.append(f"{col} ({null_count} nulls)")

    error_msg = ""
    if missing_cols:
        error_msg += f"Missing Columns: {missing_cols}\n"
    if wrong_types:
        error_msg += f"Type Mismatches: {wrong_types}\n"
    if null_criticals:
        error_msg += f"Critical Nulls Found: {null_criticals}\n"
    
    if error_msg:
        raise AssertionError(f"Schema Validation Failed:\n{error_msg}")
