"""
Odds comparison engine.

Matching strategy
-----------------
Markets from different exchanges are matched by normalised
(event_name, market_name).  For each matched pair, outcomes are
matched by normalised name.

Fractional-unit filter
----------------------
Rather than a fixed decimal threshold, the comparator identifies odds
that are *exactly* (or approximately) 1/N of a unit apart, for any
integer N in [1, max_denominator].  The tolerance controls how closely
the raw diff must sit to a unit fraction before it is accepted.

Currency normalisation
----------------------
Every odds value is converted to its USD equivalent using ExchangeRates
before comparison so that GBP-priced and USD-priced markets are
directly comparable.

Event-driven usage
------------------
OddsListener subscribes to the pub/sub bus and runs compare_markets
automatically whenever any exchange publishes a new odds snapshot.
"""

import logging
import threading
import time
from fractions import Fraction
from typing import Callable, Optional

from exchange_rates import ExchangeRates
from models import Market, Outcome, OddsDiff, MarketComparison
from pubsub import EventBus, Event, Subscription

log = logging.getLogger(__name__)


def _normalize(name: str) -> str:
    return name.lower().strip()


def _nearest_unit_fraction(diff: float, max_denominator: int = 100) -> tuple[Fraction, float]:
    """
    Return (1/N, error) where N in [1, max_denominator] minimises |diff - 1/N|.
    Returns (Fraction(0), 0.0) when diff is zero.
    """
    if diff == 0:
        return Fraction(0), 0.0
    best_n = min(range(1, max_denominator + 1), key=lambda n: abs(diff - 1 / n))
    frac = Fraction(1, best_n)
    return frac, abs(diff - float(frac))


def _odds_to_usd(outcome: Outcome, fx: ExchangeRates) -> float:
    """
    Convert decimal odds to their USD-normalised equivalent.

    Decimal odds represent a *return ratio*, not an absolute price, so they
    are dimensionless.  However, when exchanges operate in different
    currencies the stake implied by "odds 2.0" differs by the FX rate.
    We scale each odds value by (exchange_currency / USD) so that all
    values reflect a USD-denominated stake.

    Example:
        Betfair (GBP):  odds 2.50, GBP/USD = 1.27  → 2.50 * 1.27 = 3.175 USD-equivalent
        Matchbook (EUR): odds 2.40, EUR/USD = 1.09  → 2.40 * 1.09 = 2.616 USD-equivalent
    """
    return round(outcome.odds * fx.rate(outcome.currency), 6)


def compare_markets(
    markets_by_exchange: dict[str, list[Market]],
    fx: ExchangeRates,
    max_denominator: int = 20,
    fraction_tolerance: float = 0.01,
) -> list[MarketComparison]:
    """
    Compare odds across exchanges, keeping only diffs that are within
    *fraction_tolerance* of a unit fraction 1/N (N ≤ max_denominator).

    Parameters
    ----------
    markets_by_exchange  : { exchange_name: [Market, ...] }
    fx                   : ExchangeRates instance (rates already loaded)
    max_denominator      : largest N to consider (1/1 … 1/N)
    fraction_tolerance   : max allowed |diff - 1/N| to accept the match

    Returns
    -------
    List of MarketComparison sorted by max_diff descending.
    """
    # Index markets by (event_name, market_name) per exchange
    indexed: dict[tuple, dict[str, Market]] = {}
    for exchange, markets in markets_by_exchange.items():
        for market in markets:
            key = (_normalize(market.event_name), _normalize(market.market_name))
            indexed.setdefault(key, {})[exchange] = market

    comparisons: list[MarketComparison] = []

    for (event_key, market_key), exchange_markets in indexed.items():
        if len(exchange_markets) < 2:
            continue

        diffs: list[OddsDiff] = []
        exchange_list = list(exchange_markets.items())

        for i in range(len(exchange_list)):
            for j in range(i + 1, len(exchange_list)):
                ex_a, market_a = exchange_list[i]
                ex_b, market_b = exchange_list[j]

                outcomes_a = {_normalize(o.name): o for o in market_a.outcomes}
                outcomes_b = {_normalize(o.name): o for o in market_b.outcomes}

                common = set(outcomes_a.keys()) & set(outcomes_b.keys())
                for outcome_key in common:
                    oa = outcomes_a[outcome_key]
                    ob = outcomes_b[outcome_key]

                    usd_a = _odds_to_usd(oa, fx)
                    usd_b = _odds_to_usd(ob, fx)
                    raw_diff = abs(usd_a - usd_b)

                    frac, err = _nearest_unit_fraction(raw_diff, max_denominator)

                    if frac == 0 or err > fraction_tolerance:
                        continue  # not close enough to any 1/N

                    diffs.append(OddsDiff(
                        outcome_name=oa.name,
                        exchange_a=ex_a,
                        odds_a_usd=usd_a,
                        exchange_b=ex_b,
                        odds_b_usd=usd_b,
                    ))

        if diffs:
            comparisons.append(MarketComparison(
                event_name=market_a.event_name,
                market_name=market_a.market_name,
                diffs=diffs,
            ))

    comparisons.sort(key=lambda c: c.max_diff, reverse=True)
    return comparisons


def print_comparisons(comparisons: list[MarketComparison]) -> None:
    if not comparisons:
        print("No fractional-unit odds differences found.")
        return

    for comp in comparisons:
        print(f"\n{'='*70}")
        print(f"  {comp.event_name}  |  {comp.market_name}")
        print(f"  max diff: {comp.max_diff:.6f}  |  tightest fraction: {comp.tightest_fraction}")
        print(f"{'='*70}")
        for diff in sorted(comp.diffs, key=lambda d: d.abs_diff, reverse=True):
            print(f"  {diff}")


# ---------------------------------------------------------------------------
# Event-driven listener
# ---------------------------------------------------------------------------

ComparisonCallback = Callable[[list[MarketComparison]], None]


class OddsListener:
    """
    Subscribes to ``odds.*`` topics on an EventBus and runs
    compare_markets whenever any exchange publishes a new snapshot.

    The latest markets from every exchange are held in memory.  A
    comparison is triggered as soon as at least two exchanges have
    contributed data.

    Parameters
    ----------
    bus              : EventBus to subscribe on
    fx               : ExchangeRates instance
    max_denominator  : passed through to compare_markets
    fraction_tolerance : passed through to compare_markets
    on_comparison    : optional callback(list[MarketComparison]) invoked
                       after each comparison run — defaults to
                       print_comparisons
    min_exchanges    : wait until this many exchanges have data before
                       running the first comparison (default 2)
    """

    def __init__(
        self,
        bus: EventBus,
        fx: ExchangeRates,
        max_denominator: int = 20,
        fraction_tolerance: float = 0.01,
        on_comparison: Optional[ComparisonCallback] = None,
        min_exchanges: int = 2,
    ):
        self._bus = bus
        self._fx = fx
        self._max_denominator = max_denominator
        self._fraction_tolerance = fraction_tolerance
        self._on_comparison: ComparisonCallback = on_comparison or print_comparisons
        self._min_exchanges = min_exchanges

        # Latest markets snapshot per exchange  { exchange: list[Market] }
        self._snapshots: dict[str, list[Market]] = {}
        self._lock = threading.Lock()

        self._sub: Optional[Subscription] = None
        self._comparison_count = 0
        self._last_run: Optional[float] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Subscribe to odds topics on the bus."""
        self._sub = self._bus.subscribe("odds.*", self._handle_odds_event)
        log.info("[listener] subscribed to odds.* — waiting for data from %d exchanges",
                 self._min_exchanges)

    def stop(self) -> None:
        """Unsubscribe from the bus."""
        if self._sub:
            self._bus.unsubscribe(self._sub)
            self._sub = None
        log.info("[listener] unsubscribed")

    # ------------------------------------------------------------------
    # Handler
    # ------------------------------------------------------------------

    def _handle_odds_event(self, event: Event) -> None:
        markets: list[Market] = event.data or []
        exchange = event.exchange

        log.debug(
            "[listener] received odds.%s  markets=%d  ts=%.3f",
            exchange, len(markets), event.timestamp,
        )

        with self._lock:
            self._snapshots[exchange] = markets
            snapshot_copy = dict(self._snapshots)

        if len(snapshot_copy) < self._min_exchanges:
            log.debug(
                "[listener] waiting for more exchanges (%d/%d)",
                len(snapshot_copy), self._min_exchanges,
            )
            return

        # Run comparison off the lock
        self._run_comparison(snapshot_copy, trigger_exchange=exchange)

    def _run_comparison(
        self,
        markets_by_exchange: dict[str, list[Market]],
        trigger_exchange: str,
    ) -> None:
        try:
            comparisons = compare_markets(
                markets_by_exchange,
                fx=self._fx,
                max_denominator=self._max_denominator,
                fraction_tolerance=self._fraction_tolerance,
            )
            self._comparison_count += 1
            self._last_run = time.time()

            log.info(
                "[listener] comparison #%d triggered by %s → %d market(s) with diffs",
                self._comparison_count, trigger_exchange, len(comparisons),
            )
            self._on_comparison(comparisons)

        except Exception:
            log.exception("[listener] comparison failed")
