import logging
import signal
import time

from config import (
    EXCHANGES, EVENT_TYPE_IDS,
    FRACTION_TOLERANCE, MAX_DENOMINATOR,
    POLL_INTERVAL, MIN_EXCHANGES,
)
from clients.betfair import BetfairClient
from clients.matchbook import MatchbookClient
from exchange_rates import ExchangeRates
from auth import AuthManager
from pubsub import EventBus
from feed import FeedManager
from comparator import OddsListener, print_comparisons

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)

CLIENT_MAP = {
    "betfair": BetfairClient,
    "matchbook": MatchbookClient,
}


def build_clients() -> dict:
    return {
        name: CLIENT_MAP[name](cfg)
        for name, cfg in EXCHANGES.items()
        if cfg.get("enabled") and name in CLIENT_MAP
    }


def run():
    # --- Exchange rates ---------------------------------------------------
    print("[fx] Loading exchange rates...")
    fx = ExchangeRates()
    fx._ensure_fresh()
    print(f"[fx] Rates loaded. Updated: {fx.last_updated()}")

    # --- Auth all exchanges concurrently ----------------------------------
    clients = build_clients()
    manager = AuthManager(clients, token_ttl=3600)
    manager.authenticate_all()
    manager.print_status()

    ready = manager.authenticated_clients()
    if len(ready) < MIN_EXCHANGES:
        print(f"[main] Only {len(ready)} exchange(s) authenticated — need at least {MIN_EXCHANGES}.")
        return

    # --- Event bus --------------------------------------------------------
    bus = EventBus(async_dispatch=True)

    # Log every feed control event to stdout
    def _on_feed_event(event):
        print(f"[bus] {event.topic}")

    bus.subscribe("feed.*", _on_feed_event)

    # --- Odds listener (subscriber) ---------------------------------------
    listener = OddsListener(
        bus=bus,
        fx=fx,
        max_denominator=MAX_DENOMINATOR,
        fraction_tolerance=FRACTION_TOLERANCE,
        on_comparison=print_comparisons,
        min_exchanges=MIN_EXCHANGES,
    )
    listener.start()

    # --- Feed (publishers) ------------------------------------------------
    currencies = {name: EXCHANGES[name].get("currency", "USD") for name in ready}
    feeds = FeedManager(
        clients=ready,
        auth_manager=manager,
        bus=bus,
        event_type_ids=EVENT_TYPE_IDS,
        currencies=currencies,
        poll_interval=POLL_INTERVAL,
    )
    feeds.start_all()

    print(
        f"\n[main] Running — polling every {POLL_INTERVAL}s. "
        f"Press Ctrl+C to stop.\n"
    )

    # --- Graceful shutdown on SIGINT/SIGTERM ------------------------------
    def _shutdown(signum, frame):
        print("\n[main] Shutting down...")
        feeds.stop_all()
        listener.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Block main thread; all work happens in feed/bus daemon threads
    while any(feeds.status().values()):
        time.sleep(1)


if __name__ == "__main__":
    run()
