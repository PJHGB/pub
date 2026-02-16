from dataclasses import dataclass, field
from fractions import Fraction
from typing import Optional


@dataclass
class Outcome:
    name: str           # e.g. "Man Utd", "Draw", "Chelsea"
    odds: float         # decimal odds (native exchange currency)
    exchange: str       # source exchange name
    currency: str = "USD"  # ISO 4217 currency code


@dataclass
class Market:
    market_id: str
    market_name: str    # e.g. "Match Odds"
    event_name: str     # e.g. "Man Utd vs Chelsea"
    exchange: str
    currency: str = "USD"
    outcomes: list[Outcome] = field(default_factory=list)


@dataclass
class OddsDiff:
    outcome_name: str
    exchange_a: str
    odds_a_usd: float   # odds already normalised to USD
    exchange_b: str
    odds_b_usd: float

    # Fractional representation: how many 1/N units apart are the two odds?
    # e.g. diff=0.25 → 1/4 of a unit apart
    @property
    def abs_diff(self) -> float:
        """Raw absolute difference in USD-normalised decimal odds."""
        return round(abs(self.odds_a_usd - self.odds_b_usd), 6)

    @property
    def fraction(self) -> Fraction:
        """
        Express the difference as the nearest unit fraction 1/N where N is a
        positive integer.  We find the N that minimises |diff - 1/N|.

        E.g. diff=0.2  → 1/5
             diff=0.25 → 1/4
             diff=0.34 → 1/3
        """
        d = self.abs_diff
        if d == 0:
            return Fraction(0)
        # Search N in [1, 100]; pick the one whose 1/N is closest to d
        best_n = min(range(1, 101), key=lambda n: abs(d - 1 / n))
        return Fraction(1, best_n)

    @property
    def fraction_error(self) -> float:
        """How far the real diff is from the nearest unit fraction."""
        f = self.fraction
        if f == 0:
            return 0.0
        return round(abs(self.abs_diff - float(f)), 6)

    @property
    def best_exchange(self) -> str:
        return self.exchange_a if self.odds_a_usd >= self.odds_b_usd else self.exchange_b

    @property
    def best_odds_usd(self) -> float:
        return max(self.odds_a_usd, self.odds_b_usd)

    def __str__(self) -> str:
        frac = self.fraction
        frac_str = f"1/{frac.denominator}" if frac != 0 else "0"
        return (
            f"[{self.outcome_name}] "
            f"{self.exchange_a}={self.odds_a_usd:.4f} vs "
            f"{self.exchange_b}={self.odds_b_usd:.4f} (USD) "
            f"| diff={self.abs_diff} ≈ {frac_str} unit "
            f"| err={self.fraction_error} "
            f"| best={self.best_exchange}@{self.best_odds_usd:.4f}"
        )


@dataclass
class MarketComparison:
    event_name: str
    market_name: str
    diffs: list[OddsDiff] = field(default_factory=list)

    @property
    def max_diff(self) -> float:
        return max((d.abs_diff for d in self.diffs), default=0.0)

    @property
    def tightest_fraction(self) -> Optional[str]:
        """The smallest unit fraction observed across all outcome diffs."""
        if not self.diffs:
            return None
        best = min(self.diffs, key=lambda d: float(d.fraction) if d.fraction != 0 else float("inf"))
        f = best.fraction
        return f"1/{f.denominator}" if f != 0 else "0"
