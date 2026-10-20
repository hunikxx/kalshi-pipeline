from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

import config
from kalshi_auth import auth_headers
from rich.console import Console

_console = Console()


@dataclass
class Market:
    condition_id: str  # Kalshi ticker (e.g. "KXINFLATION-25JUN")
    question: str      # market title / yes_sub_title
    category: str
    yes_price: float   # last yes price in dollars (0.0 - 1.0)
    no_price: float
    volume: float      # 24h volume in contracts
    end_date: str      # close_time ISO string
    active: bool
    tokens: list[dict]  # kept for compatibility — [{token_id, outcome, price}]
    event_ticker: str = ""
    liquidity: float = 0.0
    open_interest: float = 0.0

    @property
    def implied_probability(self) -> float:
        return self.yes_price


def _api_path(endpoint: str) -> str:
    """Extract the path portion for signing from a full URL."""
    parsed = urlparse(f"{config.KALSHI_HOST}{endpoint}")
    return parsed.path


def fetch_active_markets(limit: int = 500) -> list[Market]:
    """Fetch active markets from Kalshi API via the events endpoint."""
    import time as _time
    markets = []
    cursor = None
    max_pages = 20

    for page in range(max_pages):
        endpoint = "/events"
        path = _api_path(endpoint)
        headers = auth_headers("GET", path)

        params: dict = {
            "status": "open",
            "limit": 50,
            "with_nested_markets": "true",
        }
        if cursor:
            params["cursor"] = cursor

        for attempt in range(3):
            try:
                resp = httpx.get(
                    f"{config.KALSHI_HOST}{endpoint}",
                    params=params,
                    headers=headers,
                    timeout=30,
                )
                if resp.status_code == 429:
                    _console.print(f"  [yellow][markets][/yellow] Rate limited, waiting 10s...")
                    _time.sleep(10)
                    continue
                resp.raise_for_status()
                data = resp.json()
                events = data.get("events", [])
                _console.print(f"  [dim][markets][/dim] Page {page+1}: {len(events)} events (status {resp.status_code})")
                break
            except Exception as e:
                _console.print(f"  [red][markets][/red] API error (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    _time.sleep(3)
                else:
                    _finish_log(markets)
                    return markets

        # Pause between paginated requests
        if page > 0:
            _time.sleep(0.5)

        # Extract nested markets from each event
        for event in events:
            event_category = event.get("category", "")
            for m in event.get("markets", []):
                try:
                    ticker = m.get("ticker", "")
                    status = m.get("status", "")

                    if status not in ("open", "active"):
                        continue

                    # Skip multi-leg combo markets
                    if any(prefix in ticker for prefix in ("KXMVE", "KXMULTI", "CROSSCATEGORY")):
                        continue

                    last_price = float(m.get("last_price_dollars", 0) or 0)
                    yes_bid = float(m.get("yes_bid_dollars", 0) or 0)
                    yes_ask = float(m.get("yes_ask_dollars", 0) or 0)
                    yes_price = last_price if last_price > 0 else (yes_bid + yes_ask) / 2 if (yes_bid + yes_ask) > 0 else 0.5
                    no_price = 1.0 - yes_price

                    vol_24h = float(m.get("volume_24h_fp", 0) or 0)
                    vol_total = float(m.get("volume_fp", 0) or 0)
                    liquidity = float(m.get("liquidity_dollars", 0) or 0)
                    open_interest = float(m.get("open_interest", 0) or 0)

                    title = m.get("title", m.get("yes_sub_title", ticker))

                    token_list = [
                        {"token_id": ticker, "outcome": "Yes", "price": yes_price},
                        {"token_id": ticker, "outcome": "No", "price": no_price},
                    ]

                    markets.append(Market(
                        condition_id=ticker,
                        question=title,
                        category=_infer_category(title, event_category),
                        yes_price=yes_price,
                        no_price=no_price,
                        volume=vol_24h if vol_24h > 0 else vol_total,
                        end_date=m.get("close_time", m.get("latest_expiration_time", "")),
                        active=True,
                        tokens=token_list,
                        event_ticker=m.get("event_ticker", event.get("event_ticker", "")),
                        liquidity=liquidity,
                        open_interest=open_interest,
                    ))
                except (KeyError, ValueError, TypeError):
                    continue

        cursor = data.get("cursor")
        if not cursor or len(markets) >= limit:
            break

    markets.sort(key=lambda x: x.volume, reverse=True)
    _finish_log(markets)
    return markets[:limit]


def _finish_log(markets: list[Market]):
    categories = set(m.category for m in markets)
    _console.print(f"  [bright_green][markets][/bright_green] Total parsed: {len(markets)} | Categories: {categories}")


def fetch_market_by_ticker(ticker: str) -> Market | None:
    """Fetch a single market by ticker."""
    endpoint = f"/markets/{ticker}"
    path = _api_path(endpoint)
    headers = auth_headers("GET", path)

    try:
        resp = httpx.get(
            f"{config.KALSHI_HOST}{endpoint}",
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        m = resp.json().get("market", {})
    except Exception:
        return None

    if not m:
        return None

    last_price = float(m.get("last_price_dollars", 0) or 0)
    yes_price = last_price if last_price > 0 else 0.5

    return Market(
        condition_id=m.get("ticker", ticker),
        question=m.get("title", ""),
        category=_infer_category(m.get("title", ""), m.get("category", "")),
        yes_price=yes_price,
        no_price=1.0 - yes_price,
        volume=float(m.get("volume_24h_fp", 0) or 0),
        end_date=m.get("close_time", ""),
        active=m.get("status") == "open",
        tokens=[
            {"token_id": ticker, "outcome": "Yes", "price": yes_price},
            {"token_id": ticker, "outcome": "No", "price": 1.0 - yes_price},
        ],
        event_ticker=m.get("event_ticker", ""),
        liquidity=float(m.get("liquidity_dollars", 0) or 0),
        open_interest=float(m.get("open_interest", 0) or 0),
    )


def fetch_orderbook(ticker: str) -> dict:
    """Fetch order book for a market. Returns {yes: [[price, size], ...], no: [...]}."""
    endpoint = f"/markets/{ticker}/orderbook"
    path = _api_path(endpoint)
    headers = auth_headers("GET", path)

    try:
        resp = httpx.get(
            f"{config.KALSHI_HOST}{endpoint}",
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        # Kalshi returns orderbook_fp with yes_dollars / no_dollars
        book = data.get("orderbook_fp") or data.get("orderbook", {})
        return {
            "yes": book.get("yes_dollars", book.get("yes", [])),
            "no": book.get("no_dollars", book.get("no", [])),
        }
    except Exception:
        return {}


def _infer_category(title: str, kalshi_category: str) -> str:
    """Infer category from title and Kalshi's own category field."""
    combined = f"{title} {kalshi_category}".lower()

    if any(kw in combined for kw in ["ai", "artificial intelligence", "openai", "chatgpt", "llm", "anthropic"]):
        return "ai"
    if any(kw in combined for kw in ["bitcoin", "ethereum", "crypto", "blockchain", "defi"]):
        return "crypto"
    if any(kw in combined for kw in ["election", "president", "congress", "senate", "trump", "biden", "political", "politics"]):
        return "politics"
    if any(kw in combined for kw in ["spacex", "nasa", "climate", "research", "discovery"]):
        return "science"
    if any(kw in combined for kw in ["tech", "apple", "google", "microsoft", "software", "startup"]):
        return "technology"
    if any(kw in combined for kw in ["econ", "gdp", "inflation", "fed", "interest rate", "cpi", "jobs", "unemployment"]):
        return "economy"
    if any(kw in combined for kw in ["weather", "hurricane", "temperature", "storm", "tornado", "flood"]):
        return "weather"
    if any(kw in combined for kw in ["nba", "nfl", "nhl", "mlb", "soccer", "football", "basketball",
                                      "baseball", "hockey", "tennis", "golf", "ufc", "mma", "boxing",
                                      "olympics", "fifa", "pga", "cricket", "ipl", "f1", "nascar",
                                      "playoff", "championship", "match", "game", "win on 202",
                                      "marseille", "olympique", "ligue", "laliga", "bundesliga",
                                      "premier league", "champions league", "serie a"]):
        return "sports"
    if any(kw in combined for kw in ["health", "covid", "vaccine", "fda", "drug", "medical", "disease", "pandemic"]):
        return "health"
    if any(kw in combined for kw in ["stock", "s&p", "nasdaq", "dow", "market cap", "ipo", "earnings"]):
        return "financials"
    if any(kw in combined for kw in ["airline", "flight", "shipping", "transportation", "rail", "ev"]):
        return "transportation"
    if any(kw in combined for kw in ["company", "ceo", "merger", "acquisition", "layoff", "revenue"]):
        return "companies"
    if any(kw in combined for kw in ["social media", "twitter", "tiktok", "instagram", "youtube", "influencer"]):
        return "social"
    if any(kw in combined for kw in ["war", "conflict", "nato", "united nations", "geopolitical", "china", "russia", "ukraine", "middle east"]):
        return "world"
    # Normalize Kalshi's raw category to lowercase
    return kalshi_category.lower() if kalshi_category else "other"


def filter_by_categories(markets: list[Market], categories: list[str] | None = None) -> list[Market]:
    """Filter markets to only target categories."""
    cats = {c.lower() for c in (categories or config.MARKET_CATEGORIES)}
    return [m for m in markets if m.category.lower() in cats]


def get_token_id(market: Market, side: str) -> str | None:
    """Get the ticker for a given side. On Kalshi the ticker is the same for both sides."""
    return market.condition_id


if __name__ == "__main__":
    all_markets = fetch_active_markets(limit=20)
    filtered = filter_by_categories(all_markets)
    print(f"\n--- {len(filtered)} markets in target categories (of {len(all_markets)} total) ---\n")
    for m in filtered[:15]:
        print(f"  [{m.category}] {m.question}")
        print(f"    YES: {m.yes_price:.2f} | NO: {m.no_price:.2f} | Vol: {m.volume:,.0f} contracts")
        print()
