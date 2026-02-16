"""
Exchange rates module.

Fetches live FX rates from the Open Exchange Rates (free tier) or
frankfurter.app (no key required) and converts any supported currency
to USD.  All rates are cached for the duration of the process.

Usage:
    from exchange_rates import ExchangeRates
    fx = ExchangeRates()
    usd_value = fx.to_usd(150.0, "GBP")   # -> ~190.xx
    rate      = fx.rate("GBP")             # -> USD per 1 GBP
"""

import time
from typing import Optional
import requests

# Frankfurter is a free, open-source ECB-backed FX API — no key required.
_FRANKFURTER_URL = "https://api.frankfurter.app/latest"

# Optional: set OPEN_EXCHANGE_RATES_APP_ID in .env to use OXR instead
import os
_OXR_APP_ID = os.getenv("OPEN_EXCHANGE_RATES_APP_ID", "")
_OXR_URL = "https://openexchangerates.org/api/latest.json"

_CACHE_TTL_SECONDS = 300  # 5-minute cache


class ExchangeRates:
    """Live FX rates, all normalised to USD as base currency."""

    def __init__(self):
        self._rates: dict[str, float] = {}   # { "GBP": 1.27, "EUR": 1.09, ... }  (USD per 1 unit)
        self._fetched_at: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def rate(self, currency: str) -> float:
        """Return USD value of 1 unit of *currency*. Returns 1.0 for USD."""
        currency = currency.upper()
        if currency == "USD":
            return 1.0
        self._ensure_fresh()
        r = self._rates.get(currency)
        if r is None:
            raise ValueError(f"Unsupported currency: {currency!r}. Available: {sorted(self._rates)}")
        return r

    def to_usd(self, amount: float, currency: str) -> float:
        """Convert *amount* in *currency* to USD."""
        return round(amount * self.rate(currency), 6)

    def from_usd(self, usd_amount: float, currency: str) -> float:
        """Convert a USD amount to *currency*."""
        r = self.rate(currency)
        return round(usd_amount / r, 6)

    def available_currencies(self) -> list[str]:
        self._ensure_fresh()
        return sorted(["USD"] + list(self._rates.keys()))

    def last_updated(self) -> Optional[str]:
        if not self._fetched_at:
            return None
        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(self._fetched_at))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_fresh(self) -> None:
        age = time.time() - self._fetched_at
        if age < _CACHE_TTL_SECONDS and self._rates:
            return
        self._fetch()

    def _fetch(self) -> None:
        if _OXR_APP_ID:
            self._fetch_oxr()
        else:
            self._fetch_frankfurter()

    def _fetch_frankfurter(self) -> None:
        """
        Frankfurter returns rates relative to EUR by default.
        We request base=USD so all rates are already USD-based.
        """
        try:
            resp = requests.get(
                _FRANKFURTER_URL,
                params={"base": "USD"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            raw: dict[str, float] = data["rates"]  # e.g. {"GBP": 0.79, "EUR": 0.91}
            # frankfurter gives "units of foreign per 1 USD"
            # we want "USD per 1 foreign", so invert
            self._rates = {ccy: round(1.0 / r, 8) for ccy, r in raw.items()}
            self._fetched_at = time.time()
            print(f"[fx] Rates refreshed via frankfurter.app ({len(self._rates)} currencies)")
        except Exception as e:
            print(f"[fx] frankfurter fetch failed: {e}")
            if not self._rates:
                raise

    def _fetch_oxr(self) -> None:
        """
        Open Exchange Rates free tier returns rates relative to USD directly.
        rates[ccy] = units of ccy per 1 USD  → invert for USD-per-1-ccy.
        """
        try:
            resp = requests.get(
                _OXR_URL,
                params={"app_id": _OXR_APP_ID, "base": "USD"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            raw: dict[str, float] = data["rates"]
            self._rates = {ccy: round(1.0 / r, 8) for ccy, r in raw.items() if r > 0}
            self._rates.pop("USD", None)
            self._fetched_at = time.time()
            print(f"[fx] Rates refreshed via Open Exchange Rates ({len(self._rates)} currencies)")
        except Exception as e:
            print(f"[fx] OXR fetch failed: {e}")
            if not self._rates:
                raise
