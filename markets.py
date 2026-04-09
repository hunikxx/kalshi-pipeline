from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

import config
from kalshi_auth import auth_headers


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

    @property
    def implied_probability(self) -> float:
        return self.yes_price


def _api_path(endpoint: str) -> str:
    """Extract the path portion for signing from a full URL."""
    parsed = urlparse(f"{config.KALSHI_HOST}{endpoint}")
    return parsed.path


def fetch_active_markets(limit: int = 50) -> list[Market]:
    """Fetch active markets from Kalshi API."""
    markets = []
    cursor = None

    while len(markets) < limit:
        params: dict = {
            "status": "open",
            "limit": min(limit - len(markets), 200),
        }
        if cursor:
            params["cursor"] = cursor

        endpoint = "/markets"
        path = _api_path(endpoint)
        headers = auth_headers("GET", path)

        try:
            resp = httpx.get(
                f"{config.KALSHI_HOST}{endpoint}",
                params=params,
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[markets] Kalshi API error: {e}")
            return markets

        for m in data.get("markets", []):
            try:
                yes_bid = float(m.get("yes_bid_dollars", 0) or 0)
                yes_ask = float(m.get("yes_ask_dollars", 0) or 0)
                no_bid = float(m.get("no_bid_dollars", 0) or 0)
                no_ask = float(m.get("no_ask_dollars", 0) or 0)

                # Mid price, or last trade price
                last_price = float(m.get("last_price_dollars", 0) or 0)
                yes_price = last_price if last_price > 0 else (yes_bid + yes_ask) / 2 if (yes_bid + yes_ask) > 0 else 0.5
                no_price = 1.0 - yes_price

                vol_24h = float(m.get("volume_24h_fp", 0) or 0)
                vol_total = float(m.get("volume_fp", 0) or 0)
                liquidity = float(m.get("liquidity_dollars", 0) or 0)

                ticker = m.get("ticker", "")
                title = m.get("title", m.get("yes_sub_title", ticker))
                status = m.get("status", "")

                if status != "open":
                    continue

                # Build token-like structure for compatibility
                token_list = [
                    {"token_id": ticker, "outcome": "Yes", "price": yes_price},
                    {"token_id": ticker, "outcome": "No", "price": no_price},
                ]

                markets.append(Market(
                    condition_id=ticker,
                    question=title,
                    category=_infer_category(title, m.get("category", "")),
                    yes_price=yes_price,
                    no_price=no_price,
                    volume=vol_24h if vol_24h > 0 else vol_total,
                    end_date=m.get("close_time", m.get("latest_expiration_time", "")),
                    active=True,
                    tokens=token_list,
                    event_ticker=m.get("event_ticker", ""),
                    liquidity=liquidity,
                ))
            except (KeyError, ValueError, TypeError):
                continue

        cursor = data.get("cursor")
        if not cursor:
            break

    markets.sort(key=lambda x: x.volume, reverse=True)
    return markets


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
    )


def fetch_orderbook(ticker: str) -> dict:
    """Fetch order book for a market. Returns {yes_bids, yes_asks, no_bids, no_asks}."""
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
        return resp.json().get("orderbook", {})
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
    if any(kw in combined for kw in ["weather", "hurricane", "temperature", "storm"]):
        return "weather"
    return kalshi_category if kalshi_category else "other"


def filter_by_categories(markets: list[Market], categories: list[str] | None = None) -> list[Market]:
    """Filter markets to only target categories."""
    cats = categories or config.MARKET_CATEGORIES
    return [m for m in markets if m.category in cats]


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
