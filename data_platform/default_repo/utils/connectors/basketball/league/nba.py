"""
NBA Logic Module.

Implements the NBA specific logic for the connector.
"""

from typing import List, Optional, Any, Dict
from default_repo.utils.connectors.base import BaseConnector
from default_repo.utils.connectors.basketball.api_core import StatsAPIClient, ParameterBuilder

class NbaConnector(BaseConnector):
    """Connector for the NBA Stats API."""
    league_name = "NBA"

    def __init__(self):
        self.league_id = '00'
        self.client = StatsAPIClient()

    def get_available_metrics(self) -> List[str]:
        return ['season_teams', 'season_teams_stats', 'season_players', 'season_players_stats', 'season_games', 'game_pbp']

    def fetch_data(self, metric_type: str, season: Optional[str] = None, perMode: Optional[str] = None, game_id: Optional[str] = None, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Fetches data from the NBA Stats API.

        Args:
            metric_type (str): Type of data to fetch (e.g., 'season_teams').
            season (str, optional): Season ID (e.g., '2023-24').
            perMode (str, optional): PerMode (e.g., 'Totals').
            game_id (str, optional): Game ID for PBP.
            **kwargs: Additional parameters (date_from, date_to).

        Returns:
            Optional[Dict]: The JSON response from the API.
        """
        if metric_type == 'season_teams':
            if season is None:
                raise ValueError("Season is required for 'season_teams'")
            if perMode is None:
                raise ValueError("perMode is required for 'season_teams'")
            endpoint = 'leaguestandingsv3'
            params = ParameterBuilder.build_team_params(                
                season, 
                self.league_id, 
                perMode,
                date_from=kwargs.get('date_from', ''),
                date_to=kwargs.get('date_to', '')
            )
            
        elif metric_type == 'season_players':
            if season is None:
                raise ValueError("Season is required for 'season_players'")
            if perMode is None:
                raise ValueError("perMode is required for 'season_players'")
            endpoint = 'leaguedashplayerbiostats'
            params = ParameterBuilder.build_player_params(
                season, 
                self.league_id, 
                perMode,
                date_from=kwargs.get('date_from', ''),
                date_to=kwargs.get('date_to', '')
            )
        
        elif metric_type == 'season_games':
            if season is None:
                raise ValueError("Season is required for 'season_games'")
            endpoint = 'leaguegamelog'  
            params = ParameterBuilder.build_game_log_params(season, league_id=self.league_id)
        
        elif metric_type == 'game_pbp':
             # Note: PBP doesn't use Stats API usually, but if we were to support it here:
             # It actually uses a different endpoint structure or CDN usually.
             # But assuming we want to use the Stats API equivalent 'playbyplayv3'
            pass

        json_response = self.client.make_request(endpoint, params)
        
        if not json_response:
            raise RuntimeError(f"Failed to fetch data for {metric_type}")
        
        return json_response