"""
Polymarket Whale Tracker — monitors large trades on Polymarket,
matches to Kalshi markets, and emits trade signals.

Data source: Polymarket Data API (public, no auth required)
Whale threshold: $25,000+ trades (server-side filtered)
"""
from __future__ import annotations

import asyncio
import hashlib
import time
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from difflib import SequenceMatcher

import httpx

import config
import logger as db_logger
from markets import Market
from rich.console import Console

log = logging.getLogger(__name__)
_console = Console()

TRADES_URL = "https://data-api.polymarket.com/trades"
POLL_INTERVAL = 5  # seconds between polls (speed premium)

# Only track whales in these sectors
TRACKED_SECTORS = {"politics", "crypto"}
SECTOR_KEYWORDS = {
    "politics": [
        "president", "election", "trump", "biden", "congress", "senate", "governor",
        "democrat", "republican", "vote", "primary", "nominee", "impeach", "cabinet",
        "speaker", "mayor", "prime minister", "parliament", "poll", "inaugur",
        "tariff", "executive order", "veto", "legislation", "party",
    ],
    "crypto": [
        "bitcoin", "btc", "ethereum", "eth", "crypto", "blockchain", "solana",
        "defi", "token", "coin", "nft", "binance", "coinbase", "altcoin",
        "stablecoin", "mining", "halving", "sec crypto", "etf bitcoin",
    ],
}


@dataclass
class WhaleTrade:
    wallet: str
    wallet_hash: str
    name: str
    market_title: str
    condition_id: str
    outcome: str  # "Yes", "No", or custom
    side: str  # "BUY" or "SELL"
    size: float  # USD value
    price: float
    timestamp: int
    tx_hash: str = ""

    @property
    def direction(self) -> str:
        """Effective direction: BUY YES = bullish, SELL YES = bearish, etc."""
        if self.outcome.lower() in ("yes", "0"):
            return "bullish" if self.side == "BUY" else "bearish"
        elif self.outcome.lower() in ("no", "1"):
            return "bearish" if self.side == "BUY" else "bullish"
        return "unknown"

    @property
    def age_seconds(self) -> float:
        return time.time() - self.timestamp


@dataclass
class WalletProfile:
    wallet_hash: str
    name: str
    trades: list[WhaleTrade] = field(default_factory=list)
    first_seen: float = 0.0
    total_volume: float = 0.0

    @property
    def trade_count(self) -> int:
        return len(self.trades)

    @property
    def is_repeat(self) -> bool:
        """2+ trades in tracking window."""
        return self.trade_count >= 2

    @property
    def is_heavy(self) -> bool:
        """5+ trades or $100k+ volume."""
        return self.trade_count >= 5 or self.total_volume >= 100_000

    def add_trade(self, trade: WhaleTrade):
        self.trades.append(trade)
        self.total_volume += trade.size
        if not self.first_seen:
            self.first_seen = trade.timestamp
        # Keep last 50 trades
        self.trades = self.trades[-50:]


@dataclass
class OpenPosition:
    """Tracks an open position for exit strategy."""
    kalshi_ticker: str
    kalshi_market: Market
    poly_condition_id: str
    side: str  # "YES" or "NO"
    entry_price: float  # Kalshi price at entry
    bet_amount: float
    entry_time: float  # timestamp
    entry_wallet: str  # wallet hash that triggered entry
    entry_score: int
    poly_title: str

    @property
    def age_seconds(self) -> float:
        return time.time() - self.entry_time


class PolymarketTracker:
    """Polls Polymarket for whale trades and emits signals."""

    def __init__(self, min_trade_usd: int = 25_000):
        self.min_trade_usd = min_trade_usd
        self._seen_tx: set[str] = set()  # tx hashes already processed
        self._wallets: dict[str, WalletProfile] = {}  # hash → profile
        self._market_cache: dict[str, str] = {}  # poly condition_id → kalshi ticker
        self.signal_queue: asyncio.Queue | None = None
        self.kalshi_markets: list[Market] = []  # set by pipeline
        self.recent_alerts: list[dict] = []  # for dashboard
        self._recent_signals: dict[str, float] = {}  # kalshi ticker → last signal timestamp
        self._signal_cooldown = 300  # 5 min cooldown per market
        # Exit strategy
        self.open_positions: dict[str, OpenPosition] = {}  # kalshi ticker → position
        self._price_target_cents = 0.08  # exit when price moves 8 cents in our favor
        self.stats = {
            "polls": 0,
            "trades_seen": 0,
            "whales_detected": 0,
            "repeat_whales": 0,
            "kalshi_matches": 0,
            "signals_emitted": 0,
            "exits_reversal": 0,
            "exits_price_target": 0,
            "exits_contradicting": 0,
        }

    @staticmethod
    def _hash_wallet(wallet: str) -> str:
        return hashlib.sha256(wallet.encode()).hexdigest()[:16]

    @staticmethod
    def _detect_sector(title: str) -> str | None:
        """Check if a market title belongs to a tracked sector."""
        lower = title.lower()
        for sector, keywords in SECTOR_KEYWORDS.items():
            if any(kw in lower for kw in keywords):
                return sector
        return None

    async def _fetch_trades(self) -> list[WhaleTrade]:
        """Fetch recent large trades from Polymarket."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    TRADES_URL,
                    params={
                        "limit": "100",
                        "filterType": "CASH",
                        "filterAmount": str(self.min_trade_usd),
                        "takerOnly": "true",
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                raw = resp.json()

            trades = []
            for t in raw:
                tx = t.get("transactionHash", "")
                if tx in self._seen_tx:
                    continue

                wallet = t.get("proxyWallet", "")
                if not wallet:
                    continue

                trade_price = float(t.get("price", 0))

                # Skip contracts with no upside ($0.95+ or $0.05-)
                if trade_price > 0.95 or trade_price < 0.05:
                    continue

                trade = WhaleTrade(
                    wallet=wallet,
                    wallet_hash=self._hash_wallet(wallet),
                    name=t.get("name", "") or t.get("pseudonym", "") or wallet[:10],
                    market_title=t.get("title", ""),
                    condition_id=t.get("conditionId", ""),
                    outcome=t.get("outcome", ""),
                    side=t.get("side", ""),
                    size=float(t.get("size", 0)),
                    price=trade_price,
                    timestamp=int(t.get("timestamp", time.time())),
                    tx_hash=tx,
                )
                trades.append(trade)
                self._seen_tx.add(tx)

            # Trim seen cache
            if len(self._seen_tx) > 10_000:
                self._seen_tx = set(list(self._seen_tx)[-5_000:])

            return trades

        except Exception as e:
            _console.print(f"  [red][poly][/red] Fetch error: {e}")
            return []

    def _track_wallet(self, trade: WhaleTrade) -> WalletProfile:
        """Update wallet profile with new trade."""
        h = trade.wallet_hash
        if h not in self._wallets:
            self._wallets[h] = WalletProfile(
                wallet_hash=h,
                name=trade.name,
            )
        profile = self._wallets[h]
        profile.add_trade(trade)
        return profile

    def _detect_pattern(self, trade: WhaleTrade, profile: WalletProfile) -> str | None:
        """Detect whale behavior pattern."""
        if profile.trade_count < 2:
            return None

        prev_trades = [t for t in profile.trades[:-1]
                       if t.condition_id == trade.condition_id]

        if prev_trades:
            last = prev_trades[-1]
            if last.outcome == trade.outcome and last.side == trade.side:
                return "doubling_down"
            elif last.outcome == trade.outcome and last.side != trade.side:
                return "flip"
            elif last.outcome != trade.outcome:
                return "hedge"

        if profile.is_heavy:
            return "heavy_actor"
        if profile.is_repeat:
            return "repeat_whale"

        return None

    _STOPWORDS = {
        "will", "the", "a", "an", "be", "by", "in", "on", "at", "to",
        "of", "for", "is", "it", "this", "that", "and", "or", "not",
        "before", "after", "end", "yes", "no", "any", "has", "have",
        "does", "do", "than", "more", "less", "over", "under", "above",
        "below", "through", "during", "between", "reach", "exceed",
        "next", "first", "last", "new", "become", "win", "get",
    }

    @staticmethod
    def _extract_keywords(title: str) -> set[str]:
        """Extract meaningful keywords, ignoring boilerplate."""
        words = title.lower().split()
        return {
            w.strip("?.,!\"'()[]")
            for w in words
            if w.strip("?.,!\"'()[]") not in PolymarketTracker._STOPWORDS
            and len(w.strip("?.,!\"'()[]")) > 2
        }

    def _match_to_kalshi(self, poly_title: str) -> Market | None:
        """Find best matching Kalshi market by keyword overlap."""
        if not self.kalshi_markets:
            return None

        poly_kw = self._extract_keywords(poly_title)
        if not poly_kw:
            return None

        best_match = None
        best_score = 0.0

        for market in self.kalshi_markets:
            kalshi_kw = self._extract_keywords(market.question)
            if not kalshi_kw:
                continue

            overlap = len(poly_kw & kalshi_kw)
            if overlap == 0:
                continue

            # Jaccard similarity — overlap / union
            union = len(poly_kw | kalshi_kw)
            score = overlap / union

            if score > best_score:
                best_score = score
                best_match = market

        # Need at least 45% keyword overlap to match cross-platform
        if best_score >= 0.45 and best_match:
            return best_match
        return None

    def _score_alert(self, trade: WhaleTrade, profile: WalletProfile, pattern: str | None) -> int:
        """Score the alert 0-100. Higher = more actionable."""
        score = 0

        # Trade size
        if trade.size >= 250_000:
            score += 25
        elif trade.size >= 100_000:
            score += 20
        elif trade.size >= 50_000:
            score += 15
        elif trade.size >= 25_000:
            score += 10

        # Wallet history
        if profile.is_heavy:
            score += 20
        elif profile.is_repeat:
            score += 10

        # Pattern
        if pattern == "doubling_down":
            score += 20
        elif pattern == "flip":
            score += 15
        elif pattern == "heavy_actor":
            score += 10

        # Extreme price (high conviction)
        if trade.price > 0.90 or trade.price < 0.10:
            score += 10

        # Contrarian (buying NO side or selling high-probability)
        if trade.side == "BUY" and trade.outcome.lower() == "no":
            score += 10

        return min(score, 100)

    def _check_exit_reversal(self, trade: WhaleTrade) -> OpenPosition | None:
        """Exit 1: The same wallet that triggered our entry now bets the opposite way."""
        for ticker, pos in self.open_positions.items():
            if pos.poly_condition_id != trade.condition_id:
                continue
            if trade.wallet_hash != pos.entry_wallet:
                continue
            # Same wallet, same market — check if direction flipped
            if trade.direction == "bullish" and pos.side == "NO":
                return pos
            if trade.direction == "bearish" and pos.side == "YES":
                return pos
        return None

    def _check_exit_contradicting(self, trade: WhaleTrade) -> OpenPosition | None:
        """Exit 4: A BIGGER whale bets the opposite way on the same market."""
        for ticker, pos in self.open_positions.items():
            if pos.poly_condition_id != trade.condition_id:
                continue
            # Must be opposite direction
            if trade.direction == "bullish" and pos.side == "YES":
                continue
            if trade.direction == "bearish" and pos.side == "NO":
                continue
            # Must be a bigger trade than what triggered our entry
            if trade.size >= pos.bet_amount * 5:  # 5x our entry size in USD
                return pos
        return None

    def _check_exit_price_target(self) -> list[OpenPosition]:
        """Exit 2: Kalshi price moved enough in our favor."""
        exits = []
        for ticker, pos in self.open_positions.items():
            # Refresh current Kalshi price
            current_market = None
            for m in self.kalshi_markets:
                if m.condition_id == pos.kalshi_ticker:
                    current_market = m
                    break
            if not current_market:
                continue

            current_price = current_market.yes_price
            if pos.side == "YES":
                profit = current_price - pos.entry_price
            else:
                profit = pos.entry_price - current_price

            if profit >= self._price_target_cents:
                exits.append(pos)
        return exits

    def _emit_exit_signal(self, pos: OpenPosition, reason: str):
        """Send exit signal to pipeline."""
        if not self.signal_queue:
            return

        exit_side = "NO" if pos.side == "YES" else "YES"

        self.signal_queue.put_nowait({
            "type": "poly_exit",
            "market": pos.kalshi_market,
            "side": exit_side,
            "size": pos.bet_amount,
            "score": pos.entry_score,
            "pattern": reason,
            "wallet_name": f"EXIT:{reason}",
            "poly_price": 0,
        })

        _console.print(
            f"  [bright_red]EXIT {reason.upper()}[/bright_red] "
            f"{exit_side} ${pos.bet_amount:.2f} "
            f"on \"{pos.kalshi_market.question[:40]}...\" "
            f"(was {pos.side} @{pos.entry_price:.2f})"
        )

        # Remove position
        if pos.kalshi_ticker in self.open_positions:
            del self.open_positions[pos.kalshi_ticker]

    async def run(self):
        """Main polling loop."""
        sectors = ", ".join(s.upper() for s in TRACKED_SECTORS)
        _console.print(
            f"  [bright_magenta][poly][/bright_magenta] "
            f"Polymarket whale tracker started | "
            f"sectors: {sectors} | "
            f"threshold: ${self.min_trade_usd:,} | "
            f"poll: {POLL_INTERVAL}s"
        )

        while True:
            trades = await self._fetch_trades()
            self.stats["polls"] += 1

            for trade in trades:
                self.stats["trades_seen"] += 1

                # Filter to tracked sectors only
                sector = self._detect_sector(trade.market_title)
                if not sector:
                    continue

                self.stats["whales_detected"] += 1

                # --- EXIT CHECK 1: Whale reversal (same wallet flips) ---
                rev_pos = self._check_exit_reversal(trade)
                if rev_pos:
                    self.stats["exits_reversal"] += 1
                    self._emit_exit_signal(rev_pos, "reversal")

                # Track wallet
                profile = self._track_wallet(trade)
                if profile.is_repeat:
                    self.stats["repeat_whales"] += 1

                # Detect pattern
                pattern = self._detect_pattern(trade, profile)

                # Score
                score = self._score_alert(trade, profile, pattern)

                # Display
                tier_color = "bright_red" if score >= 60 else ("bright_yellow" if score >= 35 else "dim")
                tier = "HIGH" if score >= 60 else ("MED" if score >= 35 else "LOW")
                pattern_str = f" [{pattern}]" if pattern else ""

                sector_color = "bright_cyan" if sector == "crypto" else "bright_blue"
                _console.print(
                    f"  [{tier_color}][WHALE {tier}][/{tier_color}] "
                    f"[{sector_color}]{sector.upper()}[/{sector_color}] "
                    f"${trade.size:,.0f} {trade.side} {trade.outcome} "
                    f"@{trade.price:.2f}{pattern_str} "
                    f"by {trade.name[:15]} "
                    f"(score:{score}) "
                    f"\"{trade.market_title[:40]}...\""
                )

                # Store alert
                alert = {
                    "wallet_hash": trade.wallet_hash,
                    "name": trade.name,
                    "market_title": trade.market_title,
                    "outcome": trade.outcome,
                    "side": trade.side,
                    "size": trade.size,
                    "price": trade.price,
                    "pattern": pattern or "",
                    "score": score,
                    "direction": trade.direction,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                self.recent_alerts.append(alert)
                self.recent_alerts = self.recent_alerts[-100:]

                # Log to DB
                db_logger.log_flow_alert(
                    ticker=f"POLY:{trade.condition_id[:20]}",
                    market_question=trade.market_title[:100],
                    side=f"{trade.side}_{trade.outcome}",
                    size=trade.size,
                    price=trade.price,
                    avg_size=profile.total_volume / max(profile.trade_count, 1),
                    multiple=score / 10.0,
                    market_price=trade.price,
                )

                # Match to Kalshi and emit signal
                if score >= 35:
                    kalshi_market = self._match_to_kalshi(trade.market_title)
                    if kalshi_market:
                        self.stats["kalshi_matches"] += 1
                        _console.print(
                            f"    [bright_green]→ KALSHI MATCH:[/bright_green] "
                            f"\"{kalshi_market.question[:50]}\" "
                            f"(YES:{kalshi_market.yes_price:.2f})"
                        )

                        if self.signal_queue and score >= 35:
                            # Cooldown: one signal per market per 5 min
                            now = time.time()
                            last = self._recent_signals.get(kalshi_market.condition_id, 0)
                            if now - last < self._signal_cooldown:
                                _console.print(
                                    f"    [dim]→ skipped (cooldown {int(self._signal_cooldown - (now - last))}s remaining)[/dim]"
                                )
                                continue
                            self._recent_signals[kalshi_market.condition_id] = now

                            side = "YES" if trade.direction == "bullish" else "NO"
                            self.stats["signals_emitted"] += 1
                            self.signal_queue.put_nowait({
                                "type": "poly_whale",
                                "market": kalshi_market,
                                "side": side,
                                "size": trade.size,
                                "score": score,
                                "pattern": pattern,
                                "wallet_name": trade.name,
                                "poly_price": trade.price,
                            })
                            # Track open position for exit strategy
                            bet_frac = min((score - 30) / 70.0, 1.0)
                            bet_amt = round(max(1.0, config.MAX_BET_USD * bet_frac), 2)
                            self.open_positions[kalshi_market.condition_id] = OpenPosition(
                                kalshi_ticker=kalshi_market.condition_id,
                                kalshi_market=kalshi_market,
                                poly_condition_id=trade.condition_id,
                                side=side,
                                entry_price=kalshi_market.yes_price,
                                bet_amount=bet_amt,
                                entry_time=time.time(),
                                entry_wallet=trade.wallet_hash,
                                entry_score=score,
                                poly_title=trade.market_title,
                            )

                            _console.print(
                                f"    [bright_magenta]→ SIGNAL {side}[/bright_magenta] "
                                f"score:{score} ${trade.size:,.0f} whale "
                                f"→ Kalshi \"{kalshi_market.question[:40]}...\""
                            )

            # --- EXIT CHECK 2: Price target reached ---
            price_exits = self._check_exit_price_target()
            for pos in price_exits:
                self.stats["exits_price_target"] += 1
                self._emit_exit_signal(pos, "price_target")

            # Show open positions count
            if self.open_positions:
                _console.print(
                    f"  [dim][poly] {len(self.open_positions)} open positions[/dim]"
                )

            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    async def _test():
        tracker = PolymarketTracker(min_trade_usd=10_000)
        await tracker.run()
    asyncio.run(_test())
