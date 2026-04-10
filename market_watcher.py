"""
Kalshi Market Watcher — loads all markets + real-time trade feed.
Uses WebSocket `trade` channel for instant whale detection (like polymaster).
Falls back to HTTP polling of /markets/trades when WebSocket is down.
"""
from __future__ import annotations

import asyncio
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from urllib.parse import urlparse

import httpx

import config
from markets import Market, fetch_active_markets, fetch_orderbook, fetch_market_by_ticker
from kalshi_auth import auth_headers
from rich.console import Console
import logger

_console = Console()
log = logging.getLogger(__name__)

WHALE_THRESHOLD_USD = 5000  # minimum trade value to display (dim)
WHALE_ALERT_USD = 10000  # threshold for whale alert + auto-trade


@dataclass
class KalshiTrade:
    """A single executed trade on Kalshi."""
    trade_id: str
    ticker: str
    count: int  # number of contracts
    yes_price: float  # 0-100 cents
    no_price: float
    taker_side: str  # "yes" or "no"
    created_time: str
    market_title: str = ""

    @property
    def price_dollars(self) -> float:
        """Price in dollars (0-1 range)."""
        return self.yes_price

    @property
    def value_usd(self) -> float:
        """Trade value in USD = price × count."""
        price = self.price_dollars
        if self.taker_side == "no":
            price = 1.0 - price
        return price * self.count

    @property
    def side_label(self) -> str:
        return "YES" if self.taker_side == "yes" else "NO"


class MarketWatcher:
    """Loads Kalshi markets + streams trades via WebSocket."""

    def __init__(self):
        self.all_markets: list[Market] = []
        self.tracked_markets: list[Market] = []  # alias for compatibility
        self._market_lookup: dict[str, Market] = {}  # ticker → Market
        self._refresh_interval = 300
        self._ws_connected = False
        self._seen_trade_ids: set[str] = set()
        self._http_poll_interval = 10  # seconds between HTTP polls
        self.flow_alerts: list[dict] = []
        self._whale_cluster: dict[str, list[float]] = {}  # ticker → [timestamps]
        self.signal_queue: asyncio.Queue | None = None
        self.stats = {
            "ws_messages": 0,
            "ws_trades": 0,
            "http_trades": 0,
            "market_refreshes": 0,
            "whales_detected": 0,
        }

    async def refresh_markets(self):
        """Fetch all markets from Kalshi API."""
        try:
            all_markets = await asyncio.get_event_loop().run_in_executor(
                None, lambda: fetch_active_markets(limit=1000)
            )
            self.all_markets = all_markets
            self.tracked_markets = all_markets

            # Build lookup by ticker
            self._market_lookup = {m.condition_id: m for m in all_markets}

            self.stats["market_refreshes"] += 1
            _console.print(
                f"  [bright_green][kalshi][/bright_green] "
                f"Loaded {len(self.all_markets)} markets"
            )

        except Exception as e:
            _console.print(f"  [red][kalshi][/red] Market refresh error: {e}")

    async def _process_trade(self, trade: KalshiTrade, source: str = "ws"):
        """Process a single trade — detect whales and emit alerts."""
        if trade.trade_id in self._seen_trade_ids:
            return
        self._seen_trade_ids.add(trade.trade_id)

        # Trim seen cache
        if len(self._seen_trade_ids) > 20_000:
            self._seen_trade_ids = set(list(self._seen_trade_ids)[-10_000:])

        value = trade.value_usd

        if source == "ws":
            self.stats["ws_trades"] += 1
        else:
            self.stats["http_trades"] += 1

        # Look up market title — fetch on-the-fly if unknown
        market = self._market_lookup.get(trade.ticker)
        if not market and trade.ticker:
            try:
                market = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: fetch_market_by_ticker(trade.ticker)
                )
                if market:
                    self._market_lookup[trade.ticker] = market
            except Exception:
                pass
        if market:
            trade.market_title = market.question
        else:
            trade.market_title = trade.ticker

        # Skip small trades
        if value < WHALE_THRESHOLD_USD:
            return

        # Skip contracts with no upside — too cheap or too expensive
        if trade.price_dollars > 0.85 or trade.price_dollars < 0.05:
            return

        # Display all trades above threshold
        if value >= WHALE_ALERT_USD:
            self.stats["whales_detected"] += 1
            color = "bright_red" if value >= 25_000 else ("bright_yellow" if value >= 10_000 else "bright_cyan")
            tier = "WHALE" if value >= 25_000 else ("BIG" if value >= 10_000 else "LARGE")
            oi_str = f" [OI: ${market.open_interest:,.0f}]" if market and market.open_interest > 0 else ""
            _console.print(
                f"  [{color}][{tier}][/{color}] "
                f"${value:,.0f} {trade.side_label} "
                f"({trade.count} contracts @${trade.price_dollars:.2f}) "
                f"\"{trade.market_title[:45]}\"{oi_str} "
                f"[{source}]"
            )

            # Log to DB
            logger.log_flow_alert(
                ticker=trade.ticker,
                market_question=trade.market_title[:100],
                side=trade.side_label,
                size=value,
                price=trade.price_dollars,
                avg_size=0,
                multiple=value / WHALE_ALERT_USD,
                market_price=market.yes_price if market else trade.price_dollars,
            )

            alert = {
                "ticker": trade.ticker,
                "question": trade.market_title,
                "side": trade.side_label,
                "size": value,
                "price": trade.price_dollars,
                "count": trade.count,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.flow_alerts.append(alert)
            self.flow_alerts = self.flow_alerts[-50:]

            # Track whale clustering — how many whales on this ticker in last 10 min
            now = time.time()
            if trade.ticker not in self._whale_cluster:
                self._whale_cluster[trade.ticker] = []
            self._whale_cluster[trade.ticker].append(now)
            # Trim to last 10 min
            self._whale_cluster[trade.ticker] = [
                t for t in self._whale_cluster[trade.ticker] if now - t < 600
            ]
            cluster_count = len(self._whale_cluster[trade.ticker])

            if cluster_count > 1:
                _console.print(
                    f"  [bright_yellow][CLUSTER x{cluster_count}][/bright_yellow] "
                    f"{trade.ticker} — {cluster_count} whales in 10 min"
                )

            # Emit trade signal for auto-trading
            if market and self.signal_queue:
                self.signal_queue.put_nowait({
                    "type": "flow_signal",
                    "market": market,
                    "side": trade.side_label,
                    "size": value,
                    "price": trade.price_dollars,
                    "multiple": value / WHALE_ALERT_USD,
                    "count": trade.count,
                    "cluster_count": cluster_count,
                })

        else:
            # Log smaller notable trades dimly
            _console.print(
                f"  [dim][trade] ${value:,.0f} {trade.side_label} "
                f"({trade.count}@${trade.price_dollars:.2f}) "
                f"\"{trade.market_title[:40]}\" [{source}][/dim]"
            )

    async def _connect_trade_websocket(self):
        """Connect to Kalshi WebSocket trade channel — real-time trade feed."""
        try:
            import websockets
        except ImportError:
            _console.print("[yellow][kalshi] websockets not installed — using HTTP polling only[/yellow]")
            return

        backoff = 2
        while True:
            try:
                _console.print(f"  [bright_cyan][kalshi][/bright_cyan] Connecting WebSocket trade feed...")

                # Kalshi WS requires auth
                ws_path = "/trade-api/ws/v2"
                headers = auth_headers("GET", ws_path)
                async with websockets.connect(
                    config.KALSHI_WS_HOST,
                    additional_headers=headers,
                ) as ws:
                    self._ws_connected = True
                    backoff = 2

                    # Subscribe to trade channel (all trades, no ticker filter)
                    sub = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["trade"],
                        },
                    }
                    await ws.send(json.dumps(sub))
                    _console.print(f"  [bright_green][kalshi][/bright_green] WebSocket connected — streaming all trades")

                    # Ping every 10s to keep alive (polymaster approach)
                    async def ping_loop():
                        while True:
                            await asyncio.sleep(10)
                            try:
                                await ws.ping()
                            except Exception:
                                break

                    ping_task = asyncio.create_task(ping_loop())

                    try:
                        while True:
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=30)
                                self.stats["ws_messages"] += 1
                                data = json.loads(msg)
                                await self._handle_trade_message(data)
                            except asyncio.TimeoutError:
                                continue  # ping_loop handles keepalive
                    finally:
                        ping_task.cancel()

            except Exception as e:
                self._ws_connected = False
                _console.print(f"  [yellow][kalshi][/yellow] WebSocket error: {e}, reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_trade_message(self, data: dict):
        """Process a WebSocket trade message."""
        msg_type = data.get("type", "")
        if msg_type != "trade":
            return

        msg = data.get("msg", {})

        # Primary format: msg.trades is an array of trade objects
        trades_list = msg.get("trades", [])
        if trades_list:
            for t in trades_list:
                trade = self._parse_trade(t)
                if trade and trade.trade_id and trade.count > 0:
                    await self._process_trade(trade, source="ws")
        else:
            # Fallback: msg itself is a single trade object
            trade = self._parse_trade(msg)
            if trade and trade.trade_id and trade.count > 0:
                await self._process_trade(trade, source="ws")

    def _parse_trade(self, t: dict) -> KalshiTrade | None:
        """Parse a trade dict into a KalshiTrade."""
        try:
            # Kalshi WS uses: market_ticker, yes_price_dollars, no_price_dollars, count_fp, ts
            yes_p = float(t.get("yes_price_dollars", 0) or 0)
            no_p = float(t.get("no_price_dollars", 0) or 0)
            count = float(t.get("count_fp", t.get("count", 1)) or 1)

            return KalshiTrade(
                trade_id=t.get("trade_id", ""),
                ticker=t.get("market_ticker", t.get("ticker", "")),
                count=int(count),
                yes_price=yes_p,  # already in dollars (0-1)
                no_price=no_p,
                taker_side=t.get("taker_side", "yes"),
                created_time=t.get("ts", t.get("created_time", "")),
            )
        except (ValueError, TypeError):
            return None

    async def _http_trade_polling(self):
        """Poll /markets/trades as fallback when WebSocket is down."""
        # Wait a bit before starting to let WS connect first
        await asyncio.sleep(15)

        while True:
            await asyncio.sleep(self._http_poll_interval)
            if self._ws_connected:
                continue  # WebSocket is handling it

            try:
                endpoint = "/markets/trades"
                path = urlparse(f"{config.KALSHI_HOST}{endpoint}").path
                headers = auth_headers("GET", path)

                resp = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: httpx.get(
                        f"{config.KALSHI_HOST}{endpoint}",
                        params={"limit": 100},
                        headers=headers,
                        timeout=15,
                    ),
                )

                if resp.status_code == 429:
                    _console.print(f"  [yellow][kalshi][/yellow] HTTP rate limited, waiting 10s")
                    await asyncio.sleep(10)
                    continue

                resp.raise_for_status()
                data = resp.json()

                for t in data.get("trades", []):
                    trade = KalshiTrade(
                        trade_id=t.get("trade_id", ""),
                        ticker=t.get("ticker", ""),
                        count=int(t.get("count", 0)),
                        yes_price=float(t.get("yes_price", 0)),
                        no_price=float(t.get("no_price", 0)),
                        taker_side=t.get("taker_side", ""),
                        created_time=t.get("created_time", ""),
                    )
                    if trade.trade_id and trade.count > 0:
                        await self._process_trade(trade, source="http")

            except Exception as e:
                _console.print(f"  [red][kalshi][/red] HTTP trade poll error: {e}")

    async def run(self):
        """Start the market watcher — load markets + stream trades."""
        # Load markets first
        for attempt in range(10):
            await self.refresh_markets()
            if self.all_markets:
                break
            wait = min(5 * (attempt + 1), 30)
            _console.print(f"  [yellow][kalshi][/yellow] No markets loaded, retrying in {wait}s (attempt {attempt + 1}/10)")
            await asyncio.sleep(wait)

        async def refresh_loop():
            while True:
                await asyncio.sleep(self._refresh_interval)
                await self.refresh_markets()

        await asyncio.gather(
            refresh_loop(),
            self._connect_trade_websocket(),
            self._http_trade_polling(),
            return_exceptions=True,
        )

    def get_market_by_question(self, question_fragment: str) -> Market | None:
        """Find a tracked market by partial question match."""
        frag = question_fragment.lower()
        for m in self.all_markets:
            if frag in m.question.lower():
                return m
        return None
