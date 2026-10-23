from __future__ import annotations

import asyncio
import time
import uuid
from urllib.parse import urlparse

import httpx
from rich.console import Console
from rich.markup import escape

import config
import logger
from edge import Signal
from kalshi_auth import sign_request

_console = Console()

# Kalshi's V2 order endpoint quotes every order on the YES leg: buying YES is a bid at p and
# buying NO at q is an ask at 1 - q; selling a position is the reverse of buying it.
ORDERS_PATH = "/portfolio/events/orders"
ENTRY_SLIPPAGE_CENTS = 3  # pay up to the whale's price + 3¢ (the old +1/+2/+3¢ ladder's ceiling)
EXIT_SLIPPAGE_CENTS = 3   # sell down to the best bid - 3¢


class OrderUnknown(Exception):
    """An order was sent but whether it executed couldn't be confirmed."""


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
    """Place one immediate-or-cancel order: it fills what it can at once and cancels the rest."""
    try:
        return _exit(signal) if signal.is_exit else _enter(signal)
    except OrderUnknown as e:
        # Counted toward the daily limit as if it filled, until checked by hand
        _console.print(f"    [red]Order outcome unknown: {escape(str(e))}[/red]")
        return _log_and_return(signal, status="unknown", order_id=None, detail=str(e)[:500])
    except httpx.HTTPStatusError as e:
        body = e.response.text[:500]
        _console.print(f"    [red]HTTP {e.response.status_code}: {escape(body)}[/red]")
        return _log_and_return(signal, status=f"error_http_{e.response.status_code}", order_id=None, detail=body)
    except Exception as e:
        _console.print(f"    [red]{type(e).__name__}: {escape(str(e))}[/red]")
        return _log_and_return(signal, status=f"error_{type(e).__name__}", order_id=None, detail=str(e)[:500])


def _enter(signal: Signal) -> dict:
    buy_yes = signal.side == "YES"
    price = signal.limit_price or (signal.market.yes_price if buy_yes else signal.market.no_price)
    if not 0 < price < 1:
        return _log_and_return(signal, status="error_bad_price", order_id=None, detail=f"price {price}")

    # Whole cents are valid on every market's price grid
    limit = min(99, round(price * 100) + ENTRY_SLIPPAGE_CENTS)
    count = int(signal.bet_amount * 100 // limit)
    if count < 1:
        return _log_and_return(signal, status="error_zero_contracts", order_id=None)

    order = _place({
        "ticker": signal.market.condition_id,
        "side": "bid" if buy_yes else "ask",
        "count": str(count),
        "price": _dollars(limit if buy_yes else 100 - limit),
    })
    return _log_fill(signal, order, count, yes_side=buy_yes)


def _exit(signal: Signal) -> dict:
    """Sell the whole position held in the market, whichever side it is on."""
    ticker = signal.market.condition_id
    held = _position(ticker)  # contracts: positive = YES, negative = NO
    if not held:
        return _log_and_return(signal, status="no_position", order_id=None)
    sell_yes = held > 0

    bids = _orderbook(ticker).get("yes_dollars" if sell_yes else "no_dollars") or []
    if not bids:
        return _log_and_return(signal, status="no_bids", order_id=None)
    best_bid = round(float(bids[-1][0]) * 100)  # levels are ascending; the best is last
    limit = max(1, best_bid - EXIT_SLIPPAGE_CENTS)

    count = abs(held)
    order = _place({
        "ticker": ticker,
        "side": "ask" if sell_yes else "bid",
        "count": f"{count:.2f}",
        "price": _dollars(limit if sell_yes else 100 - limit),
        "reduce_only": True,
    })
    return _log_fill(signal, order, count, yes_side=sell_yes)


def _log_fill(signal: Signal, order: dict, count: float, yes_side: bool) -> dict:
    filled = float(order.get("fill_count") or 0)
    if not filled:
        return _log_and_return(signal, status="cancelled", order_id=order.get("order_id"),
                               detail=f"0/{count:g} filled at limit")
    # average_fill_price is on the YES leg; a NO contract's price is 1 minus it
    avg = float(order.get("average_fill_price") or 0)
    price = avg if yes_side else 1 - avg
    fees = filled * float(order.get("average_fee_paid") or 0)
    # Cash paid for an entry, cash received for an exit
    amount = filled * price - fees if signal.is_exit else filled * price + fees
    return _log_and_return(
        signal,
        status="filled" if filled >= count else "partial",
        order_id=order.get("order_id"),
        amount=round(amount, 2),
        detail=f"{filled:g}/{count:g} @ {price:.4f}, fees ${fees:.2f}{order.get('_note', '')}",
    )


def _place(fields: dict) -> dict:
    """POST one immediate-or-cancel order and return the V2 create-order response."""
    body = {
        **fields,
        "client_order_id": str(uuid.uuid4()),
        "time_in_force": "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
    }
    sent = time.time()
    try:
        resp = httpx.post(f"{config.KALSHI_HOST}{ORDERS_PATH}", headers=_headers("POST", ORDERS_PATH),
                          json=body, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
        raise  # the request never reached Kalshi
    except (httpx.TransportError, httpx.HTTPStatusError) as e:
        if isinstance(e, httpx.HTTPStatusError) and e.response.status_code < 500:
            raise  # Kalshi rejected the order
        # The order may have executed even though no usable response came back
        found = _find_order(body["ticker"], body["client_order_id"], body["side"], since=sent)
        if found is None:
            raise OrderUnknown(f"client_order_id {body['client_order_id']} on {body['ticker']} after "
                               f"{type(e).__name__}: {e}") from e
        return found


def _find_order(ticker: str, client_order_id: str, side: str, since: float) -> dict | None:
    """Look an order up by client_order_id, shaped like a create-order response."""
    endpoint = "/portfolio/orders"
    for attempt in range(3):
        time.sleep(1 + attempt)  # orders show up here after a short delay
        try:
            resp = httpx.get(f"{config.KALSHI_HOST}{endpoint}", headers=_headers("GET", endpoint),
                             params={"ticker": ticker, "min_ts": int(since) - 60}, timeout=10)
            resp.raise_for_status()
        except Exception:
            continue
        for o in resp.json().get("orders") or []:
            if o.get("client_order_id") != client_order_id:
                continue
            filled = float(o.get("fill_count_fp") or 0)
            cost = float(o.get("taker_fill_cost_dollars") or 0) + float(o.get("maker_fill_cost_dollars") or 0)
            fees = float(o.get("taker_fees_dollars") or 0) + float(o.get("maker_fees_dollars") or 0)
            # Fill cost is on the outcome side the order positions for (YES for a bid, NO for an
            # ask); the create-order response this stands in for quotes the YES leg
            paid = cost / filled if filled else 0
            return {
                "order_id": o.get("order_id"),
                "fill_count": str(filled),
                "average_fill_price": str(paid if side == "bid" else 1 - paid),
                "average_fee_paid": str(fees / filled) if filled else "0",
                "_note": " (recovered after a lost response)",
            }
    return None


def shard_balances() -> dict[int, float]:
    """Cash on each exchange shard. An API order can only use the shard its market trades on."""
    endpoint = "/portfolio/balance"
    resp = httpx.get(f"{config.KALSHI_HOST}{endpoint}", headers=_headers("GET", endpoint), timeout=10)
    resp.raise_for_status()
    return {b["exchange_index"]: float(b.get("balance") or 0) for b in resp.json().get("balance_breakdown") or []}


def _position(ticker: str) -> float:
    endpoint = "/portfolio/positions"
    resp = httpx.get(f"{config.KALSHI_HOST}{endpoint}", headers=_headers("GET", endpoint),
                     params={"ticker": ticker}, timeout=10)
    resp.raise_for_status()
    return sum(float(p.get("position_fp") or 0)
               for p in resp.json().get("market_positions") or [] if p.get("ticker") == ticker)


def _orderbook(ticker: str) -> dict:
    resp = httpx.get(f"{config.KALSHI_HOST}/markets/{ticker}/orderbook", params={"depth": 1}, timeout=10)
    resp.raise_for_status()
    return resp.json().get("orderbook_fp") or {}


def _headers(method: str, endpoint: str) -> dict:
    headers = sign_request(method, urlparse(f"{config.KALSHI_HOST}{endpoint}").path)
    headers["Content-Type"] = "application/json"
    return headers


def _dollars(cents: int) -> str:
    return f"{cents / 100:.2f}"


def _log_and_return(signal: Signal, status: str, order_id: str | None,
                    amount: float | None = None, detail: str = "") -> dict:
    """Log trade to SQLite and return result dict."""
    amount = signal.bet_amount if amount is None else amount
    trade_id = logger.log_trade(
        market_id=signal.market.condition_id,
        market_question=signal.market.question,
        claude_score=signal.claude_score,
        market_price=signal.market_price,
        edge=signal.edge,
        side=signal.side,
        amount_usd=amount,
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
        detail=detail,
    )

    return {
        "trade_id": trade_id,
        "market": signal.market.question,
        "side": signal.side,
        "amount": amount,
        "edge": signal.edge,
        "status": status,
        "order_id": order_id,
        "detail": detail,
        "classification": signal.classification,
        "materiality": signal.materiality,
        "latency_ms": signal.total_latency_ms,
    }
