import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from src.adapters import ADAPTER_REGISTRY
from src.fetcher import Fetcher
from src.models import AuctionRecord
from src.storage import AuctionStorage

log = logging.getLogger(__name__)


class HarvestClient:
    """
    Orchestrator for a single micro-batch harvest run.

    Loads site configs, instantiates adapters and fetchers, runs all
    enabled sites concurrently via ThreadPoolExecutor, collects results,
    and writes to storage.
    """

    def __init__(self, config_dir: str, data_dir: Optional[str] = None) -> None:
        load_dotenv()
        self._settings    = self._load_json(Path(config_dir) / "settings.json")
        self._sites       = self._load_json(Path(config_dir) / "sites.json").get("sites", [])
        self._data_dir    = data_dir or self._settings.get("data_dir", "data")
        self._max_workers = self._settings.get("max_workers", 4)
        self._storage     = AuctionStorage(self._data_dir)

    def run(self) -> dict[str, Any]:
        """
        Execute one harvest batch.

        Returns a stats dict:
        {
            "sites_attempted": int,
            "sites_succeeded": int,
            "sites_failed":    int,
            "records_fetched": int,
            "records_written": int,
            "site_stats":      { site_name: {"fetched": int, "written": int} }
        }
        """
        enabled = [s for s in self._sites if s.get("enabled", False)]
        log.info("Starting batch: %d/%d sites enabled", len(enabled), len(self._sites))

        if not enabled:
            log.warning("No enabled sites configured.")
            return _empty_stats()

        global_retry = self._settings.get("retry", {})
        results:  dict[str, list[AuctionRecord]] = {}
        failures: dict[str, str]                 = {}

        with ThreadPoolExecutor(
            max_workers=min(self._max_workers, len(enabled)),
            thread_name_prefix="alf-site",
        ) as executor:
            future_to_name = {
                executor.submit(self._fetch_site, site, global_retry): site["name"]
                for site in enabled
            }
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    records = future.result()
                    results[name] = records
                    log.info("[%s] fetched %d records", name, len(records))
                except Exception as exc:
                    failures[name] = str(exc)
                    log.error("[%s] site fetch failed: %s", name, exc)
                    results[name] = []

        site_stats: dict[str, dict[str, int]] = {}
        total_fetched = 0
        total_written = 0

        for name, records in results.items():
            fetched = len(records)
            written = self._storage.save(records)
            total_fetched += fetched
            total_written += written
            site_stats[name] = {"fetched": fetched, "written": written}

        log.info(
            "Batch complete: %d fetched, %d written, %d site(s) failed",
            total_fetched, total_written, len(failures),
        )

        return {
            "sites_attempted": len(enabled),
            "sites_succeeded": len(enabled) - len(failures),
            "sites_failed":    len(failures),
            "records_fetched": total_fetched,
            "records_written": total_written,
            "site_stats":      site_stats,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_site(
        self,
        site_cfg: dict[str, Any],
        global_retry: dict[str, Any],
    ) -> list[AuctionRecord]:
        """Build a Fetcher and Adapter for one site, then fetch. Runs in a worker thread."""
        adapter_name = site_cfg.get("adapter", "rest")
        adapter_cls  = ADAPTER_REGISTRY.get(adapter_name)
        if adapter_cls is None:
            raise ValueError(
                f"Unknown adapter {adapter_name!r} for site {site_cfg['name']!r}. "
                f"Available: {list(ADAPTER_REGISTRY)}"
            )
        fetcher = Fetcher(site_cfg, global_retry)
        adapter = adapter_cls(site_cfg, fetcher)
        return adapter.fetch()

    @staticmethod
    def _load_json(path: Path) -> dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


def _empty_stats() -> dict[str, Any]:
    return {
        "sites_attempted": 0,
        "sites_succeeded": 0,
        "sites_failed":    0,
        "records_fetched": 0,
        "records_written": 0,
        "site_stats":      {},
    }
