import logging
from datetime import datetime
from typing import Any, Optional

from src.adapters.base import BaseAdapter
from src.models import AuctionRecord

log = logging.getLogger(__name__)

# Wrapper keys to try when the response is a dict rather than a bare list
_WRAPPER_KEYS = ("itemSummaries", "vehicles", "listings", "results", "data", "lots", "auctions", "items", "records")


class RestAdapter(BaseAdapter):
    """
    Generic REST adapter for sites that return JSON arrays of auction objects.

    Uses field_mapping from site config to normalise any REST API response
    into AuctionRecord instances. Unknown fields are collected into raw.

    Pagination is config-driven:
      - type "none" (or omitted): single fetch
      - type "offset": increments a page param until empty/short page
      - type "cursor": follows a cursor field in the response until null/absent
    """

    name = "rest"

    def fetch(self) -> list[AuctionRecord]:
        """
        Fetch all pages from the auctions endpoint and return a flat record list.
        Returns an empty list on HTTP errors (logged, not raised).
        """
        endpoint = self.config["endpoints"]["auctions"]
        url      = self.config["base_url"].rstrip("/") + endpoint
        params   = dict(self.config.get("default_params", {}))
        pagination = self.config.get("pagination", {})
        pag_type   = pagination.get("type", "none")

        try:
            if pag_type == "offset":
                return self._fetch_offset(url, params, pagination)
            elif pag_type == "cursor":
                return self._fetch_cursor(url, params, pagination)
            else:
                raw = self.fetcher.get(url, params=params)
                return self.parse(raw)
        except Exception as exc:
            log.error("[%s] fetch failed: %s", self.config["name"], exc)
            return []

    def parse(self, raw_response: Any) -> list[AuctionRecord]:
        """
        Map raw API response to AuctionRecord list using field_mapping.

        raw_response may be a list of dicts or a dict wrapping a list.
        """
        mapping = self.config.get("field_mapping", {})
        source  = self.config["name"]
        records = []

        items = self._unwrap(raw_response)
        for item in items:
            try:
                records.append(self._map_item(item, mapping, source))
            except Exception as exc:
                log.warning("[%s] skipping malformed record: %s — %r", source, exc, item)

        log.debug("[%s] parsed %d records", source, len(records))
        return records

    # ------------------------------------------------------------------
    # Pagination helpers
    # ------------------------------------------------------------------

    def _fetch_offset(
        self,
        url: str,
        base_params: dict[str, Any],
        pagination: dict[str, Any],
    ) -> list[AuctionRecord]:
        page_param      = pagination.get("page_param", "page")
        page_size_param = pagination.get("page_size_param", "page_size")
        offset          = pagination.get("start_page", 1)
        page_size       = base_params.get(page_size_param, 100)
        # offset_step=1  → traditional page-number APIs (page 1, 2, 3…)
        # offset_step=N  → direct item-offset APIs like eBay (offset 0, 200, 400…)
        step            = pagination.get("offset_step", 1)
        all_records: list[AuctionRecord] = []
        page_count = 0

        while True:
            params = {**base_params, page_param: offset}
            raw    = self.fetcher.get(url, params=params)
            batch  = self.parse(raw)
            all_records.extend(batch)
            page_count += 1

            if len(batch) < page_size:
                break
            offset += step

        log.debug("[%s] offset pagination: %d total records across %d pages",
                  self.config["name"], len(all_records), page_count)
        return all_records

    def _fetch_cursor(
        self,
        url: str,
        base_params: dict[str, Any],
        pagination: dict[str, Any],
    ) -> list[AuctionRecord]:
        cursor_param          = pagination.get("cursor_param", "cursor")
        cursor_response_field = pagination.get("cursor_response_field", "next_cursor")
        params                = dict(base_params)
        all_records: list[AuctionRecord] = []
        page_count = 0

        while True:
            raw    = self.fetcher.get(url, params=params)
            batch  = self.parse(raw)
            all_records.extend(batch)
            page_count += 1

            # Extract next cursor from the response dict
            next_cursor = None
            if isinstance(raw, dict):
                next_cursor = raw.get(cursor_response_field)

            if not next_cursor:
                break
            params = {**base_params, cursor_param: next_cursor}

        log.debug("[%s] cursor pagination: %d total records across %d pages",
                  self.config["name"], len(all_records), page_count)
        return all_records

    # ------------------------------------------------------------------
    # Parse helpers
    # ------------------------------------------------------------------

    def _unwrap(self, raw: Any) -> list[dict]:
        """Extract the list of auction dicts from the raw API response."""
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for key in _WRAPPER_KEYS:
                if key in raw and isinstance(raw[key], list):
                    return raw[key]
        log.warning("[%s] unexpected response shape: %r", self.config["name"], type(raw))
        return []

    def _map_item(
        self,
        item: dict[str, Any],
        mapping: dict[str, str],
        source: str,
    ) -> AuctionRecord:
        """Apply field_mapping to a single auction dict, returning an AuctionRecord.

        field_mapping values support dot-notation for nested fields,
        e.g. "currentBidPrice.value" extracts item["currentBidPrice"]["value"].
        The top-level key of any mapped path is excluded from raw{}.
        """
        # Top-level keys covered by the mapping (may be "a.b.c" → top key "a")
        mapped_top_keys = {v.split(".")[0] for v in mapping.values() if v}

        def _get(canonical: str) -> Any:
            path = mapping.get(canonical)
            if not path:
                return None
            val: Any = item
            for part in path.split("."):
                if not isinstance(val, dict):
                    return None
                val = val.get(part)
            return val

        def _to_float(v: Any) -> Optional[float]:
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        def _to_date(v: Any) -> Optional[str]:
            if v is None:
                return None
            s = str(v)
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                return s[:10]
            for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y%m%d", "%d-%m-%Y"):
                try:
                    return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
            log.debug("[%s] could not parse date %r — storing as-is", source, s)
            return s

        raw = {k: v for k, v in item.items() if k not in mapped_top_keys}

        return AuctionRecord(
            id            = str(_get("id") or ""),
            source        = source,
            lot_id        = str(_get("lot_id")) if _get("lot_id") is not None else None,
            url           = _get("url"),
            manufacturer  = str(_get("manufacturer") or "").strip().title(),
            model         = str(_get("model") or "").strip().title(),
            sold_price    = _to_float(_get("sold_price")),
            reserve_price = _to_float(_get("reserve_price")),
            start_price   = _to_float(_get("start_price")),
            currency      = str(_get("currency") or "GBP").upper(),
            auction_date  = _to_date(_get("auction_date")),
            raw           = raw,
        )
