"""
Authentication manager.

Authenticates to all configured exchanges concurrently and tracks session
state for each one.  Supports re-authentication on token expiry.

Usage
-----
    from auth import AuthManager
    from clients.betfair import BetfairClient
    from clients.matchbook import MatchbookClient

    manager = AuthManager(
        clients={
            "betfair":   BetfairClient(cfg_betfair),
            "matchbook": MatchbookClient(cfg_matchbook),
        }
    )
    results = manager.authenticate_all()
    # results -> {"betfair": AuthResult, "matchbook": AuthResult}

    ready = manager.authenticated_clients()
    # ready -> {"betfair": <BetfairClient>, ...}  (only those that succeeded)
"""

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional

from clients.base import ExchangeClient


class AuthStatus(Enum):
    PENDING      = auto()
    SUCCESS      = auto()
    FAILED       = auto()
    EXPIRED      = auto()   # authenticated before, but token has since expired


@dataclass
class AuthResult:
    exchange: str
    status: AuthStatus
    authenticated_at: Optional[float] = None   # unix timestamp
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.status == AuthStatus.SUCCESS

    @property
    def age_seconds(self) -> Optional[float]:
        if self.authenticated_at is None:
            return None
        return round(time.time() - self.authenticated_at, 1)

    def __str__(self) -> str:
        if self.ok:
            return f"[{self.exchange}] OK  (age {self.age_seconds}s)"
        if self.status == AuthStatus.EXPIRED:
            return f"[{self.exchange}] EXPIRED  (age {self.age_seconds}s)"
        return f"[{self.exchange}] {self.status.name}  error={self.error!r}"


class AuthManager:
    """
    Manages concurrent authentication across multiple exchange clients.

    Parameters
    ----------
    clients         : mapping of exchange name → ExchangeClient instance
    token_ttl       : seconds before a session is considered expired
                      and must be refreshed (default 3600 = 1 hour)
    max_workers     : thread-pool size for concurrent auth calls
    """

    def __init__(
        self,
        clients: dict[str, ExchangeClient],
        token_ttl: int = 3600,
        max_workers: int = 8,
    ):
        self._clients  = clients
        self._token_ttl  = token_ttl
        self._max_workers = max_workers
        self._results: dict[str, AuthResult] = {
            name: AuthResult(exchange=name, status=AuthStatus.PENDING)
            for name in clients
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def authenticate_all(self, force: bool = False) -> dict[str, AuthResult]:
        """
        Authenticate all clients concurrently.

        Parameters
        ----------
        force : re-authenticate even if the session is still valid

        Returns a dict of AuthResult keyed by exchange name.
        """
        targets = {
            name: client
            for name, client in self._clients.items()
            if force or self._needs_auth(name)
        }

        if not targets:
            print("[auth] All sessions already valid, nothing to do.")
            return self._results

        print(f"[auth] Authenticating {list(targets)} concurrently...")

        with ThreadPoolExecutor(max_workers=min(self._max_workers, len(targets))) as pool:
            futures = {
                pool.submit(self._auth_one, name, client): name
                for name, client in targets.items()
            }
            for future in as_completed(futures):
                name = futures[future]
                result = future.result()   # _auth_one never raises
                self._results[name] = result
                print(f"[auth] {result}")

        return self._results

    def refresh_expired(self) -> dict[str, AuthResult]:
        """Re-authenticate any sessions that have expired or failed."""
        stale = {
            name
            for name, result in self._results.items()
            if result.status in (AuthStatus.EXPIRED, AuthStatus.FAILED, AuthStatus.PENDING)
               or self._is_expired(result)
        }
        if not stale:
            return self._results
        print(f"[auth] Refreshing stale sessions: {list(stale)}")
        return self.authenticate_all(force=False)

    def authenticated_clients(self) -> dict[str, ExchangeClient]:
        """Return only the clients whose sessions are currently valid."""
        self._mark_expired()
        return {
            name: self._clients[name]
            for name, result in self._results.items()
            if result.status == AuthStatus.SUCCESS
        }

    def status(self) -> dict[str, AuthResult]:
        self._mark_expired()
        return dict(self._results)

    def print_status(self) -> None:
        self._mark_expired()
        print("\n[auth] Session status:")
        for result in self._results.values():
            print(f"  {result}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _auth_one(self, name: str, client: ExchangeClient) -> AuthResult:
        """Run a single authenticate() call; catches all exceptions."""
        try:
            ok = client.authenticate()
            if ok:
                return AuthResult(
                    exchange=name,
                    status=AuthStatus.SUCCESS,
                    authenticated_at=time.time(),
                )
            return AuthResult(
                exchange=name,
                status=AuthStatus.FAILED,
                error="authenticate() returned False",
            )
        except Exception as exc:
            return AuthResult(
                exchange=name,
                status=AuthStatus.FAILED,
                error=traceback.format_exception_only(type(exc), exc)[-1].strip(),
            )

    def _needs_auth(self, name: str) -> bool:
        result = self._results[name]
        if result.status == AuthStatus.PENDING:
            return True
        if result.status in (AuthStatus.FAILED, AuthStatus.EXPIRED):
            return True
        if result.status == AuthStatus.SUCCESS and self._is_expired(result):
            return True
        return False

    def _is_expired(self, result: AuthResult) -> bool:
        if result.authenticated_at is None:
            return False
        return (time.time() - result.authenticated_at) >= self._token_ttl

    def _mark_expired(self) -> None:
        for name, result in self._results.items():
            if result.status == AuthStatus.SUCCESS and self._is_expired(result):
                self._results[name] = AuthResult(
                    exchange=name,
                    status=AuthStatus.EXPIRED,
                    authenticated_at=result.authenticated_at,
                )
