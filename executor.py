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
    """Place a real order via Kalshi API. Try at whale price, bump +1¢ up to +3¢ if not filled."""
    import time
    from rich.console import Console
    _console = Console()

    try:
        ticker = signal.market.condition_id
        side = "yes" if signal.side == "YES" else "no"

        # Start at whale's price if available, otherwise market price
        if signal.limit_price > 0:
            base_price = signal.limit_price
        else:
            base_price = signal.market.yes_price if side == "yes" else signal.market.no_price

        # Try +1¢, +2¢, +3¢ above whale price
        for bump in range(1, 4):
            price = min(base_price + (bump * 0.01), 0.99)

            count = int(signal.bet_amount / price) if price > 0 else 0
            if count < 1:
                return _log_and_return(signal, status="error_zero_contracts", order_id=None)

            endpoint = "/portfolio/orders"
            path = urlparse(f"{config.KALSHI_HOST}{endpoint}").path
            headers = sign_request("POST", path)
            headers["Content-Type"] = "application/json"

            # Detect if this is an exit/sell signal
            is_exit = "EXIT" in (signal.reasoning or "")
            action = "sell" if is_exit else "buy"

            # For sells, use the original side (not the flipped one)
            if is_exit:
                side = "no" if side == "yes" else "yes"

            order_body = {
                "ticker": ticker,
                "side": side,
                "action": action,
                "count": count,
                "type": "limit",
                "yes_price": int(price * 100),
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

            order = result.get("order", {})
            order_id = order.get("order_id", "unknown")
            order_status = order.get("status", "unknown")

            # Filled immediately
            if order_status == "executed":
                if bump > 0:
                    _console.print(f"    [dim]filled at +{bump}¢ (${price:.2f})[/dim]")
                return _log_and_return(signal, status="filled", order_id=order_id)

            # Resting — wait 10s for fill
            if order_status in ("resting", "pending"):
                cancel_endpoint = f"/portfolio/orders/{order_id}"
                for _ in range(2):
                    time.sleep(5)
                    try:
                        check_path = urlparse(f"{config.KALSHI_HOST}{cancel_endpoint}").path
                        check_headers = sign_request("GET", check_path)
                        check_resp = httpx.get(
                            f"{config.KALSHI_HOST}{cancel_endpoint}",
                            headers=check_headers,
                            timeout=10,
                        )
                        check_resp.raise_for_status()
                        current_status = check_resp.json().get("order", {}).get("status", "")
                        if current_status == "executed":
                            if bump > 0:
                                _console.print(f"    [dim]filled at +{bump}¢ (${price:.2f})[/dim]")
                            return _log_and_return(signal, status="filled", order_id=order_id)
                    except Exception:
                        pass

                # Not filled after 10s — cancel and try next price
                try:
                    cancel_path = urlparse(f"{config.KALSHI_HOST}{cancel_endpoint}").path
                    cancel_headers = sign_request("DELETE", cancel_path)
                    httpx.delete(
                        f"{config.KALSHI_HOST}{cancel_endpoint}",
                        headers=cancel_headers,
                        timeout=10,
                    )
                    if bump < 3:
                        _console.print(f"    [dim]not filled at +{bump}¢ (${price:.2f}), bumping to +{bump+1}¢...[/dim]")
                except Exception:
                    pass

                continue

        # All 4 attempts failed
        return _log_and_return(signal, status="cancelled", order_id=None)

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
