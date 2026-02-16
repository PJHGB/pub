from abc import ABC, abstractmethod
from typing import Optional
import requests
from models import Market


class ExchangeClient(ABC):
    name: str = ""

    def __init__(self, config: dict):
        self.config = config
        self.session = requests.Session()
        self._session_token: Optional[str] = None
        self._authenticated: bool = False

    # ------------------------------------------------------------------
    # Auth interface (called by AuthManager)
    # ------------------------------------------------------------------

    @abstractmethod
    def authenticate(self) -> bool:
        """
        Authenticate with the exchange.  Must set self._session_token and
        self._authenticated = True on success.  Returns True on success.
        """

    def reauthenticate(self) -> bool:
        """Re-run authentication, clearing existing session state first."""
        self._session_token = None
        self._authenticated = False
        self.session = requests.Session()
        return self.authenticate()

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated and self._session_token is not None

    # ------------------------------------------------------------------
    # Markets interface
    # ------------------------------------------------------------------

    @abstractmethod
    def get_markets(self, event_type_ids: list[str]) -> list[Market]:
        """Fetch available markets for the given event type IDs."""

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, **kwargs) -> dict:
        response = self.session.get(url, **kwargs)
        response.raise_for_status()
        return response.json()

    def _post(self, url: str, **kwargs) -> dict:
        response = self.session.post(url, **kwargs)
        response.raise_for_status()
        return response.json()
