from __future__ import annotations

import asyncio
import uuid
from urllib.parse import urlparse

import httpx

import config
import logger
from edge import Signal
from kalshi_auth import sign_request


def execute_trade(signal: Signal) -> dict:
    """Execute a trade on Kalshi or log a dry-run. Synchronous."""
    daily_spent = abs(logger.get_daily_pnl())
    if daily_spent + signal.bet_amount > config.DAILY_LOSS_LIMIT_USD:
        return _log_and_return(signal, status="rejected_daily_limit", order_id=None)

    if config.DRY_RUN:
        return _log_and_return(signal, status="dry_run", order_id=None)

    return _execute_live(signal)


async def execute_trade_async(signal: Signal) -> dict:
    """Async wrapper around execute_trade."""
    return await asyncio.get_event_loop().run_in_executor(None, execute_trade, signal)


def _execute_live(signal: Signal) -> dict:
    """Place a real order via Kalshi API."""
    try:
        ticker = signal.market.condition_id
        side = "yes" if signal.side == "YES" else "no"
        price = signal.market.yes_price if side == "yes" else signal.market.no_price

        # Kalshi uses contract counts — at the given price, how many contracts
        # can we buy with our dollar amount?
        # Each contract pays $1 on resolution, costs `price` to buy
        count = int(signal.bet_amount / price) if price > 0 else 0
        if count < 1:
            return _log_and_return(signal, status="error_zero_contracts", order_id=None)

        endpoint = "/portfolio/orders"
        path = urlparse(f"{config.KALSHI_HOST}{endpoint}").path
        headers = sign_request("POST", path)
        headers["Content-Type"] = "application/json"

        order_body = {
            "ticker": ticker,
            "side": side,
            "action": "buy",
            "count": count,
            "type": "limit",
            "yes_price": int(price * 100),  # Kalshi uses cents
            "client_order_id": str(uuid.uuid4()),
        }

        resp = httpx.post(
            f"{config.KALSHI_HOST}{endpoint}",
            headers=headers,
            json=order_body,
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()

        order_id = result.get("order", {}).get("order_id", "unknown")
        return _log_and_return(signal, status="executed", order_id=order_id)

    except httpx.HTTPStatusError as e:
        return _log_and_return(signal, status=f"error_http_{e.response.status_code}", order_id=None)
    except Exception as e:
        return _log_and_return(signal, status=f"error_{type(e).__name__}", order_id=None)


def _log_and_return(signal: Signal, status: str, order_id: str | None) -> dict:
    """Log trade to SQLite and return result dict."""
    trade_id = logger.log_trade(
        market_id=signal.market.condition_id,
        market_question=signal.market.question,
        claude_score=signal.claude_score,
        market_price=signal.market_price,
        edge=signal.edge,
        side=signal.side,
        amount_usd=signal.bet_amount,
        order_id=order_id,
        status=status,
        reasoning=signal.reasoning,
        headlines=signal.headlines,
        news_source=signal.news_source,
        classification=signal.classification,
        materiality=signal.materiality,
        news_latency_ms=signal.news_latency_ms,
        classification_latency_ms=signal.classification_latency_ms,
        total_latency_ms=signal.total_latency_ms,
    )

    return {
        "trade_id": trade_id,
        "market": signal.market.question,
        "side": signal.side,
        "amount": signal.bet_amount,
        "edge": signal.edge,
        "status": status,
        "order_id": order_id,
        "classification": signal.classification,
        "materiality": signal.materiality,
        "latency_ms": signal.total_latency_ms,
    }
