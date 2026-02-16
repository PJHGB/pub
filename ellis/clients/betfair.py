import json
from clients.base import ExchangeClient
from models import Market, Outcome


class BetfairClient(ExchangeClient):
    name = "betfair"

    def authenticate(self) -> bool:
        try:
            resp = self.session.post(
                self.config["login_url"],
                data={
                    "username": self.config["username"],
                    "password": self.config["password"],
                },
                headers={"X-Application": self.config["app_key"]},
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("loginStatus") == "SUCCESS":
                self._session_token = data["sessionToken"]
                self._authenticated = True
                self.session.headers.update({
                    "X-Authentication": self._session_token,
                    "X-Application": self.config["app_key"],
                    "Content-Type": "application/json",
                })
                return True
            print(f"[betfair] Auth failed: {data.get('loginStatus')}")
            return False
        except Exception as e:
            print(f"[betfair] Auth error: {e}")
            return False

    def get_markets(self, event_type_ids: list[str]) -> list[Market]:
        markets = []

        # Step 1: list markets
        list_payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": {
                    "eventTypeIds": event_type_ids,
                    "marketTypeCodes": ["MATCH_ODDS"],
                    "inPlayOnly": False,
                },
                "marketProjection": ["EVENT", "RUNNER_DESCRIPTION"],
                "maxResults": 50,
            },
            "id": 1,
        }
        try:
            resp = self._post(self.config["base_url"], json=list_payload)
            catalogue = resp.get("result", [])
        except Exception as e:
            print(f"[betfair] listMarketCatalogue error: {e}")
            return markets

        market_ids = [m["marketId"] for m in catalogue]
        if not market_ids:
            return markets

        # Step 2: fetch best available prices
        book_payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketBook",
            "params": {
                "marketIds": market_ids,
                "priceProjection": {"priceData": ["EX_BEST_OFFERS"]},
            },
            "id": 2,
        }
        try:
            resp = self._post(self.config["base_url"], json=book_payload)
            books = {b["marketId"]: b for b in resp.get("result", [])}
        except Exception as e:
            print(f"[betfair] listMarketBook error: {e}")
            return markets

        runner_map = {
            m["marketId"]: {r["selectionId"]: r["runnerName"] for r in m.get("runners", [])}
            for m in catalogue
        }
        market_meta = {m["marketId"]: m for m in catalogue}

        for market_id, book in books.items():
            meta = market_meta.get(market_id, {})
            event_name = meta.get("event", {}).get("name", "Unknown Event")
            market_name = meta.get("marketName", "Unknown Market")
            runners_names = runner_map.get(market_id, {})

            market = Market(
                market_id=market_id,
                market_name=market_name,
                event_name=event_name,
                exchange=self.name,
            )

            for runner in book.get("runners", []):
                selection_id = runner["selectionId"]
                best_back = runner.get("ex", {}).get("availableToBack", [])
                if best_back:
                    odds = best_back[0]["price"]
                    market.outcomes.append(Outcome(
                        name=runners_names.get(selection_id, str(selection_id)),
                        odds=odds,
                        exchange=self.name,
                    ))

            if market.outcomes:
                markets.append(market)

        return markets
