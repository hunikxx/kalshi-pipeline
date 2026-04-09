"""
Kalshi WebSocket subscriber — live price feed + niche market filtering.
Maintains a live snapshot of tracked markets and detects momentum shifts.
"""
from __future__ import annotations

import asyncio
import json
import time
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field

import config
from markets import Market, fetch_active_markets, filter_by_categories
from kalshi_auth import auth_headers

log = logging.getLogger(__name__)


@dataclass
class MarketSnapshot:
    market: Market
    last_price: float
    prev_price: float
    last_update: datetime
    momentum: float = 0.0  # price change per minute

    @property
    def price_change(self) -> float:
        return self.last_price - self.prev_price


class MarketWatcher:
    """Watches niche Kalshi markets via WebSocket + periodic REST refresh."""

    def __init__(self):
        self.snapshots: dict[str, MarketSnapshot] = {}
        self.tracked_markets: list[Market] = []
        self._refresh_interval = 300  # refresh market list every 5 min
        self._ws_connected = False
        self.stats = {
            "ws_messages": 0,
            "price_updates": 0,
            "market_refreshes": 0,
        }

    def get_niche_markets(self, markets: list[Market]) -> list[Market]:
        """Filter to niche markets within volume bounds."""
        return [
            m for m in markets
            if config.MIN_VOLUME_USD <= m.volume <= config.MAX_VOLUME_USD
            and m.active
        ]

    async def refresh_markets(self):
        """Fetch and filter markets from Kalshi API."""
        try:
            all_markets = await asyncio.get_event_loop().run_in_executor(
                None, lambda: fetch_active_markets(limit=200)
            )
            categorized = filter_by_categories(all_markets)
            self.tracked_markets = self.get_niche_markets(categorized)

            # Update snapshots
            now = datetime.now(timezone.utc)
            existing_ids = set(self.snapshots.keys())
            new_ids = set()

            for m in self.tracked_markets:
                new_ids.add(m.condition_id)
                if m.condition_id not in self.snapshots:
                    self.snapshots[m.condition_id] = MarketSnapshot(
                        market=m,
                        last_price=m.yes_price,
                        prev_price=m.yes_price,
                        last_update=now,
                    )
                else:
                    snap = self.snapshots[m.condition_id]
                    snap.market = m

            for stale_id in existing_ids - new_ids:
                del self.snapshots[stale_id]

            self.stats["market_refreshes"] += 1
            log.info(f"[watcher] Tracking {len(self.tracked_markets)} niche markets")

        except Exception as e:
            log.warning(f"[watcher] Market refresh error: {e}")

    async def _connect_websocket(self):
        """Connect to Kalshi WebSocket for live price updates."""
        try:
            import websockets
        except ImportError:
            log.warning("[watcher] websockets not installed — using polling fallback")
            return

        while True:
            try:
                # Kalshi WS requires auth headers
                ws_path = "/trade-api/ws/v2"
                headers = auth_headers("GET", ws_path)

                async with websockets.connect(
                    config.KALSHI_WS_HOST,
                    additional_headers=headers,
                ) as ws:
                    self._ws_connected = True
                    log.info("[watcher] Kalshi WebSocket connected")

                    # Subscribe to ticker channel for tracked markets
                    tickers = [m.condition_id for m in self.tracked_markets]
                    if tickers:
                        sub = {
                            "id": 1,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["ticker"],
                                "market_tickers": tickers[:100],  # cap subscriptions
                            },
                        }
                        await ws.send(json.dumps(sub))

                    # Listen for updates
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=15)
                            self.stats["ws_messages"] += 1
                            data = json.loads(msg)
                            self._handle_ws_message(data)
                        except asyncio.TimeoutError:
                            await ws.ping()

            except Exception as e:
                self._ws_connected = False
                log.warning(f"[watcher] WebSocket error: {e}, reconnecting in 5s")
                await asyncio.sleep(5)

    def _handle_ws_message(self, data: dict):
        """Process a Kalshi WebSocket ticker update."""
        msg_type = data.get("type", "")
        if msg_type != "ticker":
            return

        msg = data.get("msg", {})
        ticker = msg.get("market_ticker", "")
        price = msg.get("yes_price")
        if price is None:
            price = msg.get("last_price_dollars")

        if not ticker or price is None:
            return

        snap = self.snapshots.get(ticker)
        if not snap:
            return

        now = datetime.now(timezone.utc)
        elapsed = (now - snap.last_update).total_seconds()
        snap.prev_price = snap.last_price
        snap.last_price = float(price)
        snap.last_update = now
        if elapsed > 0:
            snap.momentum = (snap.last_price - snap.prev_price) / (elapsed / 60)
        self.stats["price_updates"] += 1

    async def _polling_fallback(self):
        """Poll Kalshi API for price updates when WebSocket unavailable."""
        while True:
            await asyncio.sleep(30)
            if self._ws_connected:
                continue
            await self.refresh_markets()

    async def run(self):
        """Start the market watcher — refresh + WebSocket + polling fallback."""
        await self.refresh_markets()

        async def refresh_loop():
            while True:
                await asyncio.sleep(self._refresh_interval)
                await self.refresh_markets()

        await asyncio.gather(
            refresh_loop(),
            self._connect_websocket(),
            self._polling_fallback(),
            return_exceptions=True,
        )

    def get_market_by_question(self, question_fragment: str) -> Market | None:
        """Find a tracked market by partial question match."""
        frag = question_fragment.lower()
        for m in self.tracked_markets:
            if frag in m.question.lower():
                return m
        return None

    def get_snapshot(self, condition_id: str) -> MarketSnapshot | None:
        return self.snapshots.get(condition_id)


if __name__ == "__main__":
    async def _test():
        watcher = MarketWatcher()
        await watcher.refresh_markets()
        print(f"Tracking {len(watcher.tracked_markets)} niche markets:")
        for m in watcher.tracked_markets[:10]:
            print(f"  [{m.category}] vol:{m.volume:,.0f} | YES:{m.yes_price:.2f} | {m.question[:60]}")

    asyncio.run(_test())
