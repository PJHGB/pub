import json
import logging
from pathlib import Path

import filelock

from src.classifieds.models import ClassifiedListing

log = logging.getLogger(__name__)

_LOCK_TIMEOUT = 10


class ClassifiedStorage:
    """
    Thread-safe flat-file storage for ClassifiedListing instances.

    Writes to:
        {data_dir}/classifieds/{manufacturer}/{model}/{YYYY-MM-DD}/listings.json

    Each listings.json is a JSON array. New records are appended without
    duplicating by `id`. Uses per-file filelock for thread safety.
    """

    def __init__(self, data_dir: str) -> None:
        self._data_dir = Path(data_dir)

    def save(self, listings: list[ClassifiedListing]) -> int:
        """
        Persist a list of listings to disk.

        Groups by storage path, writes each group under a single lock.
        Returns total number of new listings written (existing IDs skipped).
        """
        if not listings:
            return 0

        groups: dict[Path, list[ClassifiedListing]] = {}
        for listing in listings:
            path = self._resolve_path(listing)
            groups.setdefault(path, []).append(listing)

        total_written = 0
        for path, group in groups.items():
            total_written += self._write_group(path, group)

        return total_written

    def _resolve_path(self, listing: ClassifiedListing) -> Path:
        mfr, mdl, date = listing.storage_path_parts
        return self._data_dir / "classifieds" / mfr / mdl / date / "listings.json"

    def _write_group(self, path: Path, listings: list[ClassifiedListing]) -> int:
        lock_path = path.with_suffix(".lock")
        try:
            with filelock.FileLock(str(lock_path), timeout=_LOCK_TIMEOUT):
                return self._merge_and_write(path, listings)
        except filelock.Timeout:
            log.error("Lock timeout for %s — skipping %d listings", path, len(listings))
            return 0
        except Exception as exc:
            log.error("Write error for %s: %s", path, exc)
            return 0

    def _merge_and_write(self, path: Path, listings: list[ClassifiedListing]) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)

        existing: list[dict] = []
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("Could not read %s: %s — overwriting", path, exc)
                existing = []

        existing_ids = {r.get("id") for r in existing}
        new_dicts    = [l.to_dict() for l in listings if l.id not in existing_ids]

        if not new_dicts:
            log.debug("All %d listings already stored at %s", len(listings), path)
            return 0

        merged = existing + new_dicts
        with open(path, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)

        log.debug("Wrote %d new listings to %s (total %d)", len(new_dicts), path, len(merged))
        return len(new_dicts)
