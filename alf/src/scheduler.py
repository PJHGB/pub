import logging
import signal
import time
from typing import Any, Optional, Type

log = logging.getLogger(__name__)


class Scheduler:
    """
    Micro-batch scheduler. Runs client.run() on a configured interval,
    indefinitely, with graceful SIGINT/SIGTERM shutdown.

    The client_class parameter accepts any class whose constructor takes
    (config_dir, data_dir) and whose run() returns a stats dict with
    "records_fetched", "records_written", and "sites_failed" keys.
    Defaults to HarvestClient (auction module).
    """

    def __init__(
        self,
        config_dir: str,
        data_dir: Optional[str] = None,
        batch_interval_secs: Optional[int] = None,
        client_class: Optional[Type] = None,
    ) -> None:
        if client_class is None:
            from src.client import HarvestClient
            client_class = HarvestClient
        self._client      = client_class(config_dir=config_dir, data_dir=data_dir)
        self._interval    = batch_interval_secs or self._client._settings.get(
            "batch_interval_seconds", 300
        )
        self._stop        = False
        self._batch_count = 0

    def run_once(self) -> dict[str, Any]:
        """Execute exactly one batch and return the stats dict."""
        batch_num = self._batch_count + 1
        log.info("=== Batch %d starting ===", batch_num)
        start = time.monotonic()
        stats = self._client.run()
        elapsed = time.monotonic() - start
        self._batch_count += 1
        log.info(
            "=== Batch %d complete in %.1fs | fetched=%d written=%d failed_sites=%d ===",
            self._batch_count,
            elapsed,
            stats["records_fetched"],
            stats["records_written"],
            stats["sites_failed"],
        )
        return stats

    def run_forever(self) -> None:
        """
        Run batches on the configured interval until SIGINT or SIGTERM.

        The signal handler sets self._stop = True. The scheduler checks
        this flag once per second during the inter-batch sleep so that
        shutdown happens within ~1 second of receiving the signal.
        """
        self._register_signals()
        log.info(
            "Scheduler starting — batch interval: %ds. Press Ctrl+C to stop.",
            self._interval,
        )

        while not self._stop:
            self.run_once()

            if self._stop:
                break

            log.info("Sleeping %ds until next batch...", self._interval)
            for _ in range(self._interval):
                if self._stop:
                    break
                time.sleep(1)

        log.info("Scheduler stopped after %d batch(es).", self._batch_count)

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _register_signals(self) -> None:
        signal.signal(signal.SIGINT,  self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        log.info("Signal %d received — stopping after current batch.", signum)
        self._stop = True
