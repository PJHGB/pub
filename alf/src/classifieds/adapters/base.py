from abc import ABC, abstractmethod
from typing import Any

from src.classifieds.models import ClassifiedListing


class BaseClassifiedAdapter(ABC):
    """
    Abstract base for all classified listing adapters.

    Mirrors the auction BaseAdapter contract but produces ClassifiedListing
    instances rather than AuctionRecord instances.
    """

    name: str = ""

    def __init__(self, site_config: dict[str, Any], fetcher: Any) -> None:
        self.config = site_config
        self.fetcher = fetcher

    @abstractmethod
    def fetch(self) -> list[ClassifiedListing]:
        """Fetch all listings from this site and return parsed records."""

    @abstractmethod
    def parse(self, raw_response: Any) -> list[ClassifiedListing]:
        """Transform a raw API response into ClassifiedListing instances."""
