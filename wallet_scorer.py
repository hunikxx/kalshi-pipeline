"""
Wallet Win Rate Scorer — fetches Polymarket position history for whale wallets
and calculates win rate + ROI to inform bet sizing and fade strategies.

Uses Polymarket Data API v2: GET /v2/positions?user={wallet}&status=CLOSED|REDEEMABLE

Results are persisted to wallet_stats.json and refreshed every 12 hours.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

POSITIONS_URL = "https://data-api.polymarket.com/v2/positions"
# Resolved positions only: CLOSED = exited (sold or redeemed); REDEEMABLE = resolved but still
# held, used here for its winners only, because the API lists a page's winners before its losers;
# REDEEMABLE_LOST = the still-held losers, newest first. Newest first throughout, so a capped page
# is a recent sample rather than the default biggest-wins-first ordering.
RESOLVED_STATUSES = ("CLOSED", "REDEEMABLE", "REDEEMABLE_LOST")
POSITIONS_LIMIT = 200
STATS_METHOD = 2  # bump when the win-rate calculation changes so stored stats are refetched
REFRESH_INTERVAL = 43200  # Refresh from API every 12 hours
FAILURE_RETRY_SECONDS = 600  # a failed fetch is retried after 10 min, not cached as "unknown" for 12h
MIN_POSITIONS = 5  # Need at least 5 resolved positions for a meaningful win rate
STORE_PATH = Path(__file__).parent / "wallet_stats.json"


@dataclass
class WalletStats:
    wallet: str
    name: str
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    total_invested: float = 0.0
    positions_count: int = 0
    fetched_at: float = 0.0
    method: int = 1

    @property
    def win_rate(self) -> float:
        """Win rate as a decimal (0.0 - 1.0)."""
        total = self.wins + self.losses
        if total == 0:
            return 0.5  # Unknown — assume neutral
        return self.wins / total

    @property
    def win_rate_pct(self) -> float:
        return self.win_rate * 100

    @property
    def roi(self) -> float:
        """Return on investment as a decimal."""
        if self.total_invested <= 0:
            return 0.0
        return self.total_pnl / self.total_invested

    @property
    def roi_pct(self) -> float:
        return self.roi * 100

    @property
    def is_reliable(self) -> bool:
        """Enough data to trust the win rate."""
        return (self.wins + self.losses) >= MIN_POSITIONS

    @property
    def tier(self) -> str:
        """Classify the wallet's skill level."""
        if not self.is_reliable:
            return "unknown"
        if self.win_rate >= 0.65:
            return "sharp"
        if self.win_rate >= 0.55:
            return "good"
        if self.win_rate >= 0.45:
            return "average"
        if self.win_rate >= 0.35:
            return "weak"
        return "fade"  # < 35% — bet against them

    @property
    def bet_multiplier(self) -> float:
        """
        Bet sizing multiplier based on wallet quality.
        - sharp (65%+): 2.0x
        - good (55-65%): 1.5x
        - average (45-55%): 1.0x
        - weak (35-45%): 0.5x (reduce bet)
        - fade (<35%): -1.0 (bet opposite side)
        - unknown: 1.0x (default)
        """
        if not self.is_reliable:
            return 1.0
        if self.win_rate >= 0.65:
            return 2.0
        if self.win_rate >= 0.55:
            return 1.5
        if self.win_rate >= 0.45:
            return 1.0
        if self.win_rate >= 0.35:
            return 0.5
        return -1.0  # Fade signal

    def to_dict(self) -> dict:
        return {
            "wallet": self.wallet,
            "name": self.name,
            "wins": self.wins,
            "losses": self.losses,
            "total_pnl": round(self.total_pnl, 2),
            "total_invested": round(self.total_invested, 2),
            "positions_count": self.positions_count,
            "fetched_at": self.fetched_at,
            "method": self.method,
        }

    @classmethod
    def from_dict(cls, d: dict) -> WalletStats:
        return cls(
            wallet=d["wallet"],
            name=d.get("name", ""),
            wins=d.get("wins", 0),
            losses=d.get("losses", 0),
            total_pnl=d.get("total_pnl", 0.0),
            total_invested=d.get("total_invested", 0.0),
            positions_count=d.get("positions_count", 0),
            fetched_at=d.get("fetched_at", 0.0),
            method=d.get("method", 1),
        )


# In-memory store: wallet_address -> WalletStats
_store: dict[str, WalletStats] = {}
_loaded = False


def _load_store():
    """Load wallet stats from disk on first access."""
    global _store, _loaded
    if _loaded:
        return
    _loaded = True
    if STORE_PATH.exists():
        try:
            with open(STORE_PATH, "r") as f:
                data = json.load(f)
            for wallet, entry in data.items():
                _store[wallet] = WalletStats.from_dict(entry)
            log.info(f"Loaded {len(_store)} wallet profiles from {STORE_PATH.name}")
        except Exception as e:
            log.warning(f"Failed to load wallet store: {e}")


def _save_store():
    """Persist wallet stats to disk."""
    try:
        data = {wallet: stats.to_dict() for wallet, stats in _store.items()}
        with open(STORE_PATH, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.warning(f"Failed to save wallet store: {e}")


async def get_wallet_stats(wallet: str, name: str = "") -> WalletStats:
    """
    Fetch wallet position history from Polymarket and calculate win rate.
    Returns cached version if refreshed within REFRESH_INTERVAL.
    Persists to wallet_stats.json after each new fetch.
    """
    _load_store()

    # Return stored data if fresh enough and computed by the current method
    cached = _store.get(wallet)
    if cached and cached.method != STATS_METHOD:
        cached = None
    if cached:
        # Update name if we have a better one
        if name and not cached.name:
            cached.name = name
        if time.time() - cached.fetched_at < REFRESH_INTERVAL:
            return cached

    # Fetch from API
    stats = WalletStats(wallet=wallet, name=name, fetched_at=time.time(), method=STATS_METHOD)

    try:
        async with httpx.AsyncClient() as client:
            resps = await asyncio.gather(*(
                client.get(
                    POSITIONS_URL,
                    params={
                        "user": wallet,
                        "status": status,
                        "sort_by": "TIMESTAMP",
                        "sort_direction": "DESC",
                        "limit": str(POSITIONS_LIMIT),
                    },
                    timeout=15,
                )
                for status in RESOLVED_STATUSES
            ))
        # Each set is capped separately, so each covers its own time span. Losers tend to sit
        # unredeemed while winners move to CLOSED, so mixing unequal spans skews the rate; keep
        # only the span that every capped set covers.
        cutoff = 0
        sets = []
        for status, resp in zip(RESOLVED_STATUSES, resps):
            resp.raise_for_status()
            page = resp.json()
            rows = page.get("data") or []
            capped = bool((page.get("pagination") or {}).get("has_more"))
            if status == "REDEEMABLE":
                # Winners come first; they are only truncated if the page never reached a loser
                capped = capped and all((r.get("current_price") or 0) > 0 for r in rows)
                rows = [r for r in rows if (r.get("current_price") or 0) > 0]
            times = [r["last_event_at"] for r in rows if r.get("last_event_at")]
            if capped and times:
                cutoff = max(cutoff, min(times))
            sets.append(rows)

        # Many positions can share one timestamp (e.g. a batch redemption) and a capped page can end
        # partway through them, so the boundary second itself is dropped. A position sold down to a
        # dust residual before resolution is listed both as CLOSED and as still held; count it once.
        positions = {}
        for rows in sets:
            for r in rows:
                if not cutoff or r.get("last_event_at", 0) > cutoff:
                    positions.setdefault(r.get("token_id") or id(r), r)
        positions = list(positions.values())

        stats.positions_count = len(positions)

        for p in positions:
            # Resolved rows have no open mark left, so total_pnl is the position's final result.
            # entry_cost_usdc is ~0 on CLOSED rows; lifetime cost is total_size x avg_price.
            pnl = float(p.get("total_pnl", 0) or 0)
            invested = float(p.get("total_size", 0) or 0) * float(p.get("avg_price", 0) or 0)

            stats.total_pnl += pnl
            stats.total_invested += invested

            if pnl > 0:
                stats.wins += 1
            elif pnl < 0:
                stats.losses += 1
            # pnl == 0 means breakeven — don't count

    except Exception as e:
        log.warning(f"Wallet stats fetch failed for {wallet[:10]}: {e}")
        # Keep current-method data if we have it (else neutral stats), and hold off retrying for
        # FAILURE_RETRY_SECONDS rather than refetching on every trade during an outage
        stats = cached or WalletStats(wallet=wallet, name=name, method=STATS_METHOD)
        stats.fetched_at = time.time() - REFRESH_INTERVAL + FAILURE_RETRY_SECONDS

    _store[wallet] = stats
    _save_store()
    return stats


def get_cached_stats(wallet: str) -> WalletStats | None:
    """Get stored stats without fetching. Returns None if not stored."""
    _load_store()
    return _store.get(wallet)


def get_all_stats() -> dict[str, WalletStats]:
    """Return all stored wallet stats."""
    _load_store()
    return dict(_store)


def cache_size() -> int:
    _load_store()
    return len(_store)
