# Ellis - Exchange Odds Comparator

Queries multiple sportsbook exchange APIs, compares odds per market, and identifies differences (arbitrage opportunities, value gaps).

## Structure

```
ellis/
├── config.py               # API keys, exchange configs
├── models.py               # Data models (Market, Outcome, OddsSnapshot)
├── clients/
│   ├── base.py             # Abstract base client
│   ├── betfair.py          # Betfair Exchange
│   └── matchbook.py        # Matchbook Exchange
├── comparator.py           # Odds comparison and diff logic
├── main.py                 # Entry point / runner
└── requirements.txt
```

## Usage

```bash
pip install -r requirements.txt
python main.py
```

Configure your API keys in `config.py` before running.
