import requests
from typing import Optional, Dict

class StatsAPIClient:
    BASE_URL = "https://stats.nba.com/stats"
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nba.com/',
            'Origin': 'https://www.nba.com',
            'Connection': 'keep-alive',
        }
    
    def make_request(self, endpoint: str, params: dict) -> Optional[dict]:
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return None

class ParameterBuilder:
    @staticmethod
    def build_team_params(season: str, 
            league_id: str = '00', 
            perMode: str = 'Totals', 
            date_from: str = '',  # <--- Añadir esto
            date_to: str = ''    # <--- Añadir esto
        ):    
        return {
            'MeasureType': 'Base', 
            'PerMode': perMode, 
            'LeagueID': league_id,
            'Season': season, 
            'SeasonType': 'Regular Season',
            'DateFrom': date_from,  # Formato: MM/DD/YYYY
            'DateTo': date_to,      # Formato: MM/DD/YYYY
            'PlusMinus': 'N',
            'PaceAdjust': 'N', 
            'Rank': 'N', 
            'PORound': '0', 
            'Outcome': '', 
            'Location': '', 
            'Month': '0',
            'SeasonSegment': '', 
            'OpponentTeamID': '0', 
            'VsConference': '', 
            'VsDivision': '',
            'TeamID': '0', 
            'Conference': '', 
            'Division': '', 
            'GameSegment': '',
            'Period': '0', 
            'ShotClockRange': '', 
            'LastNGames': '0',
            'GameScope': '', 
            'PlayerExperience': '', 
            'PlayerPosition': '',
            'StarterBench': ''
        }
    
    @staticmethod
    def build_player_params(
            season: str, 
            league_id: str = '00', 
            perMode: str = 'Totals', 
            date_from: str = '',  # <--- Añadir esto
            date_to: str = ''    # <--- Añadir esto
        ):    
        return {
            'MeasureType': 'Base', 
            'PerMode': perMode, 
            'LeagueID': league_id,
            'Season': season, 
            'SeasonType': 'Regular Season',
            'DateFrom': date_from,  # Formato: MM/DD/YYYY
            'DateTo': date_to,      # Formato: MM/DD/YYYY
            'PlusMinus': 'N',
            'PaceAdjust': 'N', 
            'Rank': 'N', 
            'PORound': '0', 
            'Outcome': '', 
            'Location': '', 
            'Month': '0',
            'SeasonSegment': '', 
            'OpponentTeamID': '0', 
            'VsConference': '', 
            'VsDivision': '',
            'TeamID': '0', 
            'Conference': '', 
            'Division': '', 
            'GameSegment': '',
            'Period': '0', 
            'ShotClockRange': '', 
            'LastNGames': '0',
            'GameScope': '', 
            'PlayerExperience': '', 
            'PlayerPosition': '',
            'StarterBench': ''
        }

    @staticmethod
    def build_game_log_params(season: str, league_id: str = '00') -> Dict[str, str]:
        return {
            'Counter': '1000',
            'DateFrom': '',
            'DateTo': '',
            'Direction': 'ASC',
            'LeagueID': league_id,
            'PlayerOrTeam': 'T',
            'Season': season,
            'SeasonType': 'Regular Season',
            'Sorter': 'DATE'
        }

    @staticmethod
    def build_pbp_params(game_id: str) -> Dict[str, str]:
        safe_id = str(game_id).zfill(10)
        return {
            'GameID': safe_id,
            'StartPeriod': '0',  # 0 = Todo el partido
            'EndPeriod': '14',   # 14 = Cubre hasta muchas prórrogas
        }