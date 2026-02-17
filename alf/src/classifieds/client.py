import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from src.classifieds.adapters import CLASSIFIED_ADAPTER_REGISTRY
from src.classifieds.models import ClassifiedListing
from src.classifieds.storage import ClassifiedStorage
from src.fetcher import Fetcher

log = logging.getLogger(__name__)


class ClassifiedHarvestClient:
    """
    Orchestrator for a single classified listings micro-batch harvest run.

    Mirrors HarvestClient but uses ClassifiedStorage and CLASSIFIED_ADAPTER_REGISTRY.
    Reads config from config/classifieds/ by default.
    """

    def __init__(self, config_dir: str, data_dir: Optional[str] = None) -> None:
        load_dotenv()
        self._settings    = self._load_json(Path(config_dir) / "settings.json")
        self._sites       = self._load_json(Path(config_dir) / "sites.json").get("sites", [])
        self._data_dir    = data_dir or self._settings.get("data_dir", "data")
        self._max_workers = self._settings.get("max_workers", 4)
        self._storage     = ClassifiedStorage(self._data_dir)

    def run(self) -> dict[str, Any]:
        """
        Execute one classified listings harvest batch.

        Returns a stats dict matching the auction HarvestClient.run() schema
        so Scheduler can consume it without modification.
        """
        enabled = [s for s in self._sites if s.get("enabled", False)]
        log.info("Classifieds batch: %d/%d sites enabled", len(enabled), len(self._sites))

        if not enabled:
            log.warning("No enabled classifieds sites configured.")
            return _empty_stats()

        global_retry = self._settings.get("retry", {})
        results:  dict[str, list[ClassifiedListing]] = {}
        failures: dict[str, str]                     = {}

        with ThreadPoolExecutor(
            max_workers=min(self._max_workers, len(enabled)),
            thread_name_prefix="alf-classified",
        ) as executor:
            future_to_name = {
                executor.submit(self._fetch_site, site, global_retry): site["name"]
                for site in enabled
            }
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    listings = future.result()
                    results[name] = listings
                    log.info("[%s] fetched %d listings", name, len(listings))
                except Exception as exc:
                    failures[name] = str(exc)
                    log.error("[%s] site fetch failed: %s", name, exc)
                    results[name] = []

        site_stats: dict[str, dict[str, int]] = {}
        total_fetched = 0
        total_written = 0

        for name, listings in results.items():
            fetched = len(listings)
            written = self._storage.save(listings)
            total_fetched += fetched
            total_written += written
            site_stats[name] = {"fetched": fetched, "written": written}

        log.info(
            "Classifieds batch complete: %d fetched, %d written, %d site(s) failed",
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

    def _fetch_site(
        self,
        site_cfg: dict[str, Any],
        global_retry: dict[str, Any],
    ) -> list[ClassifiedListing]:
        adapter_name = site_cfg.get("adapter", "rest")
        adapter_cls  = CLASSIFIED_ADAPTER_REGISTRY.get(adapter_name)
        if adapter_cls is None:
            raise ValueError(
                f"Unknown adapter {adapter_name!r} for site {site_cfg['name']!r}. "
                f"Available: {list(CLASSIFIED_ADAPTER_REGISTRY)}"
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
