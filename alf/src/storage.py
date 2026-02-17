import json
import logging
from pathlib import Path
from typing import Optional

import filelock

from src.models import AuctionRecord

log = logging.getLogger(__name__)

# How long to wait for a file lock before giving up (seconds)
_LOCK_TIMEOUT = 10


class AuctionStorage:
    """
    Thread-safe flat-file storage for AuctionRecord instances.

    Writes to:
        {data_dir}/{manufacturer}/{model}/{YYYY-MM-DD}/auctions.json

    Each auctions.json is a JSON array. New records are appended to the
    existing array without duplicating by `id`.

    Thread safety is provided by filelock — one .lock file per auctions.json.
    Multiple threads writing to different manufacturer/model/date paths
    proceed in parallel. Only threads targeting the same path serialise.
    """

    def __init__(self, data_dir: str) -> None:
        self._data_dir = Path(data_dir)

    def save(self, records: list[AuctionRecord]) -> int:
        """
        Persist a list of records to disk.

        Groups records by their storage path, then writes each group
        under a single lock acquisition. Returns the total number of
        new records written (existing IDs are skipped).
        """
        if not records:
            return 0

        groups: dict[Path, list[AuctionRecord]] = {}
        for record in records:
            path = self._resolve_path(record)
            groups.setdefault(path, []).append(record)

        total_written = 0
        for path, group_records in groups.items():
            total_written += self._write_group(path, group_records)

        return total_written

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_path(self, record: AuctionRecord) -> Path:
        mfr, mdl, date = record.storage_path_parts
        return self._data_dir / mfr / mdl / date / "auctions.json"

    def _write_group(self, path: Path, records: list[AuctionRecord]) -> int:
        lock_path = path.with_suffix(".lock")
        try:
            with filelock.FileLock(str(lock_path), timeout=_LOCK_TIMEOUT):
                return self._merge_and_write(path, records)
        except filelock.Timeout:
            log.error("Lock timeout for %s — skipping %d records", path, len(records))
            return 0
        except Exception as exc:
            log.error("Write error for %s: %s", path, exc)
            return 0

    def _merge_and_write(self, path: Path, records: list[AuctionRecord]) -> int:
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
        new_dicts    = [r.to_dict() for r in records if r.id not in existing_ids]

        if not new_dicts:
            log.debug("All %d records already stored at %s", len(records), path)
            return 0

        merged = existing + new_dicts
        with open(path, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)

        log.debug("Wrote %d new records to %s (total %d)", len(new_dicts), path, len(merged))
        return len(new_dicts)
