import os
from dotenv import load_dotenv

load_dotenv()

EXCHANGES = {
    "betfair": {
        "enabled": True,
        "app_key": os.getenv("BETFAIR_APP_KEY", ""),
        "username": os.getenv("BETFAIR_USERNAME", ""),
        "password": os.getenv("BETFAIR_PASSWORD", ""),
        "base_url": "https://api.betfair.com/exchange/betting/json-rpc/v1",
        "login_url": "https://identitysso-cert.betfair.com/api/certlogin",
        "currency": "GBP",  # Betfair UK operates in GBP
    },
    "matchbook": {
        "enabled": True,
        "username": os.getenv("MATCHBOOK_USERNAME", ""),
        "password": os.getenv("MATCHBOOK_PASSWORD", ""),
        "base_url": "https://api.matchbook.com/edge/rest",
        "currency": "EUR",  # Matchbook default is EUR
    },
}

# Fractional-unit comparison settings
# Diffs must be within FRACTION_TOLERANCE of a unit fraction 1/N to be reported
FRACTION_TOLERANCE = 0.01   # e.g. diff=0.251 is close enough to 1/4 (0.25)
MAX_DENOMINATOR = 20        # consider fractions 1/1, 1/2, ... 1/20

# Sports / event type IDs to query (Betfair event type IDs)
EVENT_TYPE_IDS = ["1"]  # 1 = Soccer

# Feed settings
POLL_INTERVAL = 30      # seconds between odds polls per exchange
MIN_EXCHANGES = 2       # wait for this many exchanges before comparing
