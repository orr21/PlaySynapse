from default_repo.utils.connectors.base import BaseConnector
from default_repo.utils.connectors.basketball.api_core import StatsAPIClient, ParameterBuilder
class NbaConnector(BaseConnector):
    league_name = "NBA"

    def __init__(self):
        self.league_id = '00'
        self.client = StatsAPIClient()

    def get_available_metrics(self):
        return ['season_teams', 'season_teams_stats', 'season_players', 'season_players_stats', 'season_games', 'game_pbp']

    def fetch_data(self, metric_type, season=None, perMode=None, game_id=None, **kwargs):
        if metric_type == 'season_teams':
            if season is None:
                raise ValueError("Season es obligatorio para la métrica 'season_teams'")
            if perMode is None:
                raise ValueError("perMode es obligatorio para la métrica 'season_teams'")
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
                raise ValueError("Season es obligatorio para la métrica 'season_players'")
            if perMode is None:
                raise ValueError("perMode es obligatorio para la métrica 'season_players'")
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
                raise ValueError("Season es obligatorio para la métrica 'season_games'")
            endpoint = 'leaguegamelog'  
            params = ParameterBuilder.build_game_log_params(season, league_id=self.league_id)

        json_response = self.client.make_request(endpoint, params)
        
        if not json_response:
            raise RuntimeError(f"Fallo al obtener datos para {metric_type}")
        
        return json_response