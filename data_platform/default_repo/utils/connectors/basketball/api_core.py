"""
NBA Stats API Core.

Handles direct HTTP requests to stats.nba.com and builds parameters for various endpoints.
"""

import requests
from typing import Optional, Dict, Any


class StatsAPIClient:
    """
    Client for interacting with the NBA Stats API.
    Handles headers, timeouts, and basic error checking.
    """

    BASE_URL = "https://stats.nba.com/stats"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nba.com/",
            "Origin": "https://www.nba.com",
            "Connection": "keep-alive",
        }

    def make_request(self, endpoint: str, params: dict) -> Optional[dict]:
        """
        Sends a GET request to the specified endpoint.

        Args:
            endpoint (str): API endpoint (e.g., 'leaguedashplayerbiostats').
            params (dict): Query parameters.

        Returns:
            Optional[dict]: JSON response or None if failed.
        """
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = requests.get(
                url, headers=self.headers, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"StatsAPIClient Error: {e}")
            return None


class ParameterBuilder:
    """Helper class to construct parameter dictionaries for API calls."""

    @staticmethod
    def build_team_params(
        season: str,
        league_id: str = "00",
        perMode: str = "Totals",
        date_from: str = "",
        date_to: str = "",
    ) -> Dict[str, str]:
        """Builds parameters for Team stats."""
        return {
            "MeasureType": "Base",
            "PerMode": perMode,
            "LeagueID": league_id,
            "Season": season,
            "SeasonType": "Regular Season",
            "DateFrom": date_from,
            "DateTo": date_to,
            "PlusMinus": "N",
            "PaceAdjust": "N",
            "Rank": "N",
            "PORound": "0",
            "Outcome": "",
            "Location": "",
            "Month": "0",
            "SeasonSegment": "",
            "OpponentTeamID": "0",
            "VsConference": "",
            "VsDivision": "",
            "TeamID": "0",
            "Conference": "",
            "Division": "",
            "GameSegment": "",
            "Period": "0",
            "ShotClockRange": "",
            "LastNGames": "0",
            "GameScope": "",
            "PlayerExperience": "",
            "PlayerPosition": "",
            "StarterBench": "",
        }

    @staticmethod
    def build_player_params(
        season: str,
        league_id: str = "00",
        perMode: str = "Totals",
        date_from: str = "",
        date_to: str = "",
    ) -> Dict[str, str]:
        """Builds parameters for Player stats."""
        return {
            "MeasureType": "Base",
            "PerMode": perMode,
            "LeagueID": league_id,
            "Season": season,
            "SeasonType": "Regular Season",
            "DateFrom": date_from,
            "DateTo": date_to,
            "PlusMinus": "N",
            "PaceAdjust": "N",
            "Rank": "N",
            "PORound": "0",
            "Outcome": "",
            "Location": "",
            "Month": "0",
            "SeasonSegment": "",
            "OpponentTeamID": "0",
            "VsConference": "",
            "VsDivision": "",
            "TeamID": "0",
            "Conference": "",
            "Division": "",
            "GameSegment": "",
            "Period": "0",
            "ShotClockRange": "",
            "LastNGames": "0",
            "GameScope": "",
            "PlayerExperience": "",
            "PlayerPosition": "",
            "StarterBench": "",
        }

    @staticmethod
    def build_game_log_params(season: str, league_id: str = "00") -> Dict[str, str]:
        """Builds parameters for League Game Logs."""
        return {
            "Counter": "1000",
            "DateFrom": "",
            "DateTo": "",
            "Direction": "ASC",
            "LeagueID": league_id,
            "PlayerOrTeam": "T",
            "Season": season,
            "SeasonType": "Regular Season",
            "Sorter": "DATE",
        }

    @staticmethod
    def build_pbp_params(game_id: str) -> Dict[str, str]:
        """Builds parameters for Play-by-Play."""
        safe_id = str(game_id).zfill(10)
        return {
            "GameID": safe_id,
            "StartPeriod": "0",  # 0 = All periods
            "EndPeriod": "14",  # 14 = Covers multiple OTs
        }
