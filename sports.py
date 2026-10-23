"""
Sports detection shared by every path that alerts or trades.

Kalshi: the exchange's own series categories (GET /series), checked for each leg of a combo
market. Polymarket: the event's tags from the Gamma API. Both fail closed: a market whose category
can't be determined counts as sports, so a lookup failure can only cost a trade, never place a
sports bet. (Titles are no substitute: Kalshi game markets read like "Cleveland wins".)
"""
from __future__ import annotations

import logging
import time

import httpx

import config

log = logging.getLogger(__name__)

GAMMA_EVENT_URL = "https://gamma-api.polymarket.com/events/slug/{}"
GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets/slug/{}"
SERIES_REFRESH_SECONDS = 6 * 3600

_series_category: dict[str, str] = {}  # Kalshi series ticker -> category, e.g. "KXNBAGAME" -> "Sports"
_series_loaded_at = 0.0
_poly_event_sports: dict[str, bool] = {}  # Polymarket event slug -> tagged sports
_poly_market_event: dict[str, str] = {}  # Polymarket market slug -> event slug


def _series_of(ticker: str) -> str:
    # Some series tickers contain "-" (e.g. KXNEWOUTBREAK-P), so take the longest known prefix
    parts = ticker.split("-")
    for n in range(len(parts) - 1, 1, -1):
        prefix = "-".join(parts[:n])
        if prefix in _series_category:
            return prefix
    return parts[0]


def note_kalshi_series(series: str, category: str):
    """Record a series category seen elsewhere (e.g. on an event) if /series hasn't given one;
    an event's category can differ from its series' (e.g. KXMLBCBA: Entertainment vs Sports)."""
    if series and category:
        _series_category.setdefault(series, category)


def refresh_kalshi_series():
    """Load every Kalshi series category. Cheap to call often: refetches every few hours."""
    global _series_loaded_at
    if time.time() - _series_loaded_at < SERIES_REFRESH_SECONDS:
        return
    try:
        resp = httpx.get(f"{config.KALSHI_HOST}/series", timeout=30)
        resp.raise_for_status()
        series = resp.json().get("series") or []
    except Exception as e:
        log.warning(f"Kalshi series load failed: {e}")
        return
    _series_category.update({s["ticker"]: s.get("category") or "" for s in series if s.get("ticker")})
    _series_loaded_at = time.time()


def ensure_kalshi_series(tickers: list[str]):
    """Fetch the category of any series not seen yet (new series appear between full loads)."""
    for series in {_series_of(t) for t in tickers if t} - _series_category.keys():
        try:
            resp = httpx.get(f"{config.KALSHI_HOST}/series/{series}", timeout=5)
            resp.raise_for_status()
            note_kalshi_series(series, (resp.json().get("series") or {}).get("category") or "")
        except Exception as e:
            log.warning(f"Kalshi series lookup failed for {series}: {e}")


def kalshi_is_sports(ticker: str, legs: list[str] = ()) -> bool:
    """A Kalshi market is sports if its series is; a combo is sports if any of its legs is."""
    cats = [_series_category.get(_series_of(t)) for t in (legs or [ticker]) if t]
    if "Sports" in cats:
        return True
    # Combo series are "Exotics" whatever their legs are, so without legs they say nothing
    return not cats or not all(c and c != "Exotics" for c in cats)


async def _gamma(client: httpx.AsyncClient, url: str) -> dict:
    resp = await client.get(url, timeout=3)
    resp.raise_for_status()
    return resp.json()


async def polymarket_is_sports(event_slug: str, market_slug: str = "") -> bool:
    """A Polymarket trade is sports if its event is tagged sports. Cached per event."""
    # The trade feed fills in event_slug a few seconds after a trade first appears; the market
    # slug is there from the start
    event_slug = event_slug or _poly_market_event.get(market_slug, "")
    if event_slug in _poly_event_sports:
        return _poly_event_sports[event_slug]
    if not event_slug and not market_slug:
        return True  # combo trades carry neither (and have no Yes/No outcome to copy anyway)
    try:
        async with httpx.AsyncClient() as client:
            if not event_slug:
                market = await _gamma(client, GAMMA_MARKET_URL.format(market_slug))
                event_slug = ((market.get("events") or [{}])[0]).get("slug") or ""
                if not event_slug:
                    return True
                _poly_market_event[market_slug] = event_slug
                if event_slug in _poly_event_sports:
                    return _poly_event_sports[event_slug]
            event = await _gamma(client, GAMMA_EVENT_URL.format(event_slug))
        tags = {t.get("slug") for t in event.get("tags") or []}
    except Exception as e:
        log.warning(f"Polymarket tag lookup failed for {event_slug or market_slug!r}: {e}")
        return True
    if not tags:
        return True
    _poly_event_sports[event_slug] = "sports" in tags
    return _poly_event_sports[event_slug]
