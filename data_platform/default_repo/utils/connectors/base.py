"""
Base Connector Module.

Defines the abstract base class for all sports connectors.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class BaseConnector(ABC):
    """
    Abstract base class for sports data connectors.
    Enforces implementation of key properties and methods.
    """

    @property
    @abstractmethod
    def league_name(self) -> str:
        """Returns the name of the league (e.g., 'NBA')."""
        pass

    @abstractmethod
    def get_available_metrics(self) -> List[str]:
        """Returns a list of supported metric types."""
        pass

    @abstractmethod
    def fetch_data(
        self, metric_type: str, season: str, **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Fetches data for a specific metric and season.

        Args:
            metric_type (str): The type of data to fetch.
            season (str): The season identifier.
            **kwargs: Additional arguments.

        Returns:
            Optional[Dict[str, Any]]: The fetched data or None.
        """
        pass
