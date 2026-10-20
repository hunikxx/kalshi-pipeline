"""
Followed Wallets — curated list of top Polymarket performers.
Any trade from these wallets auto-triggers a Kalshi buy (non-sports only),
bypassing the normal size/score/sector filters.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

STORE_PATH = Path(__file__).parent / "followed_wallets.json"

_addresses: set[str] = set()       # normalized (lowercase) wallet addresses
_names: set[str] = set()           # normalized (lowercase, stripped) names
_loaded = False


def _normalize_name(s: str) -> str:
    return (s or "").strip().lower()


def _load():
    global _loaded, _addresses, _names
    if _loaded:
        return
    _loaded = True
    if not STORE_PATH.exists():
        log.warning(f"No followed wallets file at {STORE_PATH}")
        return
    try:
        with open(STORE_PATH) as f:
            data = json.load(f)
        for entry in data.get("wallets", []):
            addr = entry.get("address")
            name = entry.get("name")
            if addr:
                _addresses.add(addr.lower())
            if name:
                _names.add(_normalize_name(name))
        log.info(f"Loaded {len(_addresses)} followed addresses, {len(_names)} followed names")
    except Exception as e:
        log.warning(f"Failed to load followed wallets: {e}")


def is_followed(wallet: str | None, name: str | None) -> bool:
    """True if this trade is from one of our followed whales (by address or name)."""
    _load()
    if wallet and wallet.lower() in _addresses:
        return True
    if name and _normalize_name(name) in _names:
        return True
    return False


def followed_count() -> tuple[int, int]:
    """Return (num_addresses, num_names) loaded."""
    _load()
    return len(_addresses), len(_names)
