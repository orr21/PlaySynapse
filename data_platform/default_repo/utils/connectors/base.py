from abc import ABC, abstractmethod

class BaseConnector(ABC):
    @property
    @abstractmethod
    def league_name(self):
        pass

    @abstractmethod
    def get_available_metrics(self):
        pass

    @abstractmethod
    def fetch_data(self, metric_type, season, **kwargs):
        pass