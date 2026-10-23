"""
Telegram notifier for whale trade alerts.
Sends whale trades to Telegram: $10K+ non-sports, $30K+ sports.
"""

import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import aiohttp

import config

log = logging.getLogger(__name__)

TELEGRAM_THRESHOLD_USD = 10_000
TELEGRAM_SPORTS_THRESHOLD_USD = 30_000

async def send_whale_alert(
    source: str,
    title: str,
    side: str,
    value: float,
    price: float,
    count: int = 0,
    username: str = "",
    extra: str = "",
    force: bool = False,
    sports: bool = False,
):
    """
    Send a whale trade alert to Telegram.
    $10K+ for non-sports, $30K+ for sports. Pass force=True to bypass threshold (e.g., followed wallets).
    """
    token = config.TELEGRAM_BOT_TOKEN
    chat_ids = config.TELEGRAM_CHANNEL_IDS

    if not token or not chat_ids:
        return

    if not force:
        # Sports gets higher threshold
        threshold = TELEGRAM_SPORTS_THRESHOLD_USD if sports else TELEGRAM_THRESHOLD_USD
        if value < threshold:
            return

    ts = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%H:%M:%S PT")
    tier = "FOLLOWED" if force else ("WHALE" if value >= 25_000 else "BIG")

    msg = (
        f"🐋 <b>[{tier}] {source.upper()}</b>\n"
        f"<b>${value:,.0f}</b> {side} @ ${price:.2f}"
    )
    if count:
        msg += f" ({count:,} contracts)"
    msg += f"\n📌 {title}"
    if username:
        msg += f"\n👤 {username}"
    if extra:
        msg += f"\n{extra}"
    msg += f"\n🕐 {ts}"

    await send_text(msg)


async def send_text(msg: str):
    """Send an HTML message to every configured Telegram chat."""
    token = config.TELEGRAM_BOT_TOKEN
    chat_ids = config.TELEGRAM_CHANNEL_IDS

    if not token or not chat_ids:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    try:
        async with aiohttp.ClientSession() as session:
            for chat_id in chat_ids:
                payload = {
                    "chat_id": chat_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                }
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        log.warning(f"Telegram send failed ({resp.status}): {body}")
    except Exception as e:
        log.warning(f"Telegram notification error: {e}")
