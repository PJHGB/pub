"""
OddsFeed — per-exchange polling loop.

Each exchange gets its own OddsFeed running in a daemon thread.  On
every poll cycle it:
  1. Checks the session is still valid (re-auths via AuthManager if not)
  2. Calls client.get_markets()
  3. Stamps the exchange currency onto every market/outcome
  4. Publishes the result to the event bus on topic  odds.<exchange>

Control topics published:
    feed.started.<exchange>   on first successful poll
    feed.stopped.<exchange>   when stop() is called
    feed.error.<exchange>     on any exception during a poll cycle

Usage
-----
    from feed import OddsFeed, FeedManager

    fm = FeedManager(
        clients=ready_clients,       # from AuthManager.authenticated_clients()
        auth_manager=manager,
        bus=bus,
        event_type_ids=["1"],
        currencies={"betfair": "GBP", "matchbook": "EUR"},
        poll_interval=30,
    )
    fm.start_all()
    # ... bus subscribers receive events as they arrive ...
    fm.stop_all()
"""

import logging
import threading
import time
from typing import Optional

from auth import AuthManager
from clients.base import ExchangeClient
from pubsub import EventBus

log = logging.getLogger(__name__)


class OddsFeed:
    """
    Polling loop for a single exchange.  Runs in a background daemon thread.

    Parameters
    ----------
    name            : exchange name
    client          : authenticated ExchangeClient
    auth_manager    : AuthManager — used to refresh session if expired
    bus             : EventBus to publish onto
    event_type_ids  : sport IDs to query
    currency        : ISO 4217 code for this exchange
    poll_interval   : seconds between polls (default 30)
    """

    def __init__(
        self,
        name: str,
        client: ExchangeClient,
        auth_manager: AuthManager,
        bus: EventBus,
        event_type_ids: list[str],
        currency: str = "USD",
        poll_interval: int = 30,
    ):
        self.name = name
        self._client = client
        self._auth = auth_manager
        self._bus = bus
        self._event_type_ids = event_type_ids
        self._currency = currency
        self._poll_interval = poll_interval

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._poll_count = 0
        self._last_poll: Optional[float] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            log.warning("[feed:%s] already running", self.name)
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"feed-{self.name}",
            daemon=True,
        )
        self._thread.start()
        log.info("[feed:%s] started (interval=%ss)", self.name, self._poll_interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self._poll_interval + 5)
        self._bus.publish(f"feed.stopped.{self.name}", exchange=self.name)
        log.info("[feed:%s] stopped", self.name)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        first_run = True
        while not self._stop_event.is_set():
            try:
                self._poll()
                if first_run:
                    self._bus.publish(f"feed.started.{self.name}", exchange=self.name)
                    first_run = False
            except Exception as exc:
                log.error("[feed:%s] poll error: %s", self.name, exc)
                self._bus.publish(
                    f"feed.error.{self.name}",
                    exchange=self.name,
                    data={"error": str(exc)},
                )

            self._stop_event.wait(self._poll_interval)

    def _poll(self) -> None:
        # Re-authenticate if session has lapsed
        if not self._client.is_authenticated:
            log.info("[feed:%s] session invalid, re-authenticating...", self.name)
            results = self._auth.refresh_expired()
            if not results[self.name].ok:
                raise RuntimeError(f"re-auth failed: {results[self.name].error}")

        markets = self._client.get_markets(self._event_type_ids)

        # Stamp currency
        for market in markets:
            market.currency = self._currency
            for outcome in market.outcomes:
                outcome.currency = self._currency

        self._poll_count += 1
        self._last_poll = time.time()

        log.debug("[feed:%s] poll #%d → %d markets", self.name, self._poll_count, len(markets))
        self._bus.publish(f"odds.{self.name}", exchange=self.name, data=markets)


class FeedManager:
    """
    Manages a collection of OddsFeed instances — one per exchange.

    Parameters
    ----------
    clients         : { exchange_name: ExchangeClient }  (authenticated)
    auth_manager    : shared AuthManager for re-auth
    bus             : shared EventBus
    event_type_ids  : list of sport IDs
    currencies      : { exchange_name: "GBP" }
    poll_interval   : seconds between polls (applied to all feeds)
    """

    def __init__(
        self,
        clients: dict[str, ExchangeClient],
        auth_manager: AuthManager,
        bus: EventBus,
        event_type_ids: list[str],
        currencies: dict[str, str],
        poll_interval: int = 30,
    ):
        self._feeds: dict[str, OddsFeed] = {
            name: OddsFeed(
                name=name,
                client=client,
                auth_manager=auth_manager,
                bus=bus,
                event_type_ids=event_type_ids,
                currency=currencies.get(name, "USD"),
                poll_interval=poll_interval,
            )
            for name, client in clients.items()
        }

    def start_all(self) -> None:
        for feed in self._feeds.values():
            feed.start()

    def stop_all(self) -> None:
        for feed in self._feeds.values():
            feed.stop()

    def status(self) -> dict[str, bool]:
        return {name: feed.is_running for name, feed in self._feeds.items()}
