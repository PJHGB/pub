from clients.base import ExchangeClient
from models import Market, Outcome

# Matchbook sport IDs: 15 = Soccer
SPORT_IDS = {"1": 15}


class MatchbookClient(ExchangeClient):
    name = "matchbook"

    def authenticate(self) -> bool:
        try:
            resp = self._post(
                f"{self.config['base_url']}/security/session",
                json={
                    "username": self.config["username"],
                    "password": self.config["password"],
                },
            )
            self._session_token = resp.get("session-token")
            if self._session_token:
                self._authenticated = True
                self.session.headers.update({
                    "session-token": self._session_token,
                    "Content-Type": "application/json",
                })
                return True
            print(f"[matchbook] Auth failed: no session token")
            return False
        except Exception as e:
            print(f"[matchbook] Auth error: {e}")
            return False

    def get_markets(self, event_type_ids: list[str]) -> list[Market]:
        markets = []

        for eid in event_type_ids:
            sport_id = SPORT_IDS.get(eid)
            if not sport_id:
                continue
            try:
                resp = self._get(
                    f"{self.config['base_url']}/events",
                    params={
                        "sport-ids": sport_id,
                        "states": "open",
                        "include-prices": True,
                        "price-depth": 1,
                        "per-page": 50,
                    },
                )
            except Exception as e:
                print(f"[matchbook] get_markets error: {e}")
                continue

            for event in resp.get("events", []):
                event_name = event.get("name", "Unknown Event")
                for mb_market in event.get("markets", []):
                    if mb_market.get("market-type") != "one_x_two":
                        continue
                    market = Market(
                        market_id=str(mb_market["id"]),
                        market_name=mb_market.get("name", "Match Odds"),
                        event_name=event_name,
                        exchange=self.name,
                    )
                    for runner in mb_market.get("runners", []):
                        prices = runner.get("prices", [])
                        back_prices = [p for p in prices if p.get("side") == "back"]
                        if back_prices:
                            best_odds = max(p["decimal-odds"] for p in back_prices)
                            market.outcomes.append(Outcome(
                                name=runner.get("name", "Unknown"),
                                odds=best_odds,
                                exchange=self.name,
                            ))
                    if market.outcomes:
                        markets.append(market)

        return markets
