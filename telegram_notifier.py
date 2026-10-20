"""
Telegram notifier for whale trade alerts.
Sends whale trades to Telegram: $15K+ non-sports, $50K+ sports.
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

SPORTS_KEYWORDS = {
    "game", "nba", "nfl", "mlb", "nhl", "pga", "ufc", "mma", "soccer", "epl",
    "fifa", "football", "basketball", "baseball", "hockey", "tennis", "golf",
    "boxing", "olympics", "world cup", "super bowl", "playoff", "championship",
    "match", "touchdown", "goal scorer", "mvp", "draft pick", "ligue",
    "laliga", "bundesliga", "serie a", "premier league", "champions league",
    "olympique", "marseille", "arsenal", "chelsea", "liverpool", "barcelona",
    "real madrid", "manchester", "juventus", "bayern", "cricket", "ipl",
    "winner", "f1", "nascar",
    # Stat-line / parlay-style keywords (NBA props etc.)
    "points scored", "rebounds", "assists", "steals", "blocks",
    "three-pointer", "3-pointer", " pts ", " reb ", " ast ", " stl ", " blk ",
    "field goal", "free throw", "double-double", "triple-double",
    # World Cup / tournament advancement keywords
    "advances", "advance", "knockouts", "group stage", "round of 16",
    "quarterfinal", "semifinal", "wins the group", "top scorer",
    "clean sheet", "own goal", "penalty shootout",
}

# Common pro team names — unambiguous sports indicators
SPORTS_TEAMS = {
    # NHL
    "hurricanes", "senators", "maple leafs", "canadiens", "bruins", "rangers",
    "islanders", "devils", "penguins", "capitals", "flyers", "lightning",
    "panthers", "red wings", "blue jackets", "sabres", "blackhawks",
    "predators", "avalanche", "stars", "wild", "jets", "blues", "oilers",
    "flames", "canucks", "kings", "ducks", "sharks", "coyotes",
    "golden knights", "kraken",
    # NBA
    "lakers", "celtics", "warriors", "bulls", "bucks", "heat", "knicks",
    "76ers", "nets", "raptors", "hawks", "hornets", "magic", "pistons",
    "pacers", "cavaliers", "wizards", "thunder", "rockets", "mavericks",
    "spurs", "grizzlies", "pelicans", "nuggets", "jazz", "trail blazers",
    "suns", "clippers", "timberwolves",
    # NFL
    "cowboys", "patriots", "49ers", "eagles", "chiefs", "ravens", "bills",
    "steelers", "packers", "vikings", "bengals", "browns", "dolphins",
    "jets", "raiders", "chargers", "broncos", "titans", "jaguars",
    "colts", "texans", "seahawks", "rams", "cardinals", "saints",
    "buccaneers", "falcons", "panthers", "giants", "commanders",
    "lions", "bears",
    # MLB
    "yankees", "red sox", "dodgers", "mets", "cubs", "astros", "braves",
    "phillies", "orioles", "blue jays", "rays", "guardians", "white sox",
    "tigers", "royals", "twins", "athletics", "mariners", "angels",
    "rangers", "diamondbacks", "rockies", "padres", "giants",
    "cardinals", "pirates", "brewers", "reds", "marlins", "nationals",
}


def _has_vs_pattern(title: str) -> bool:
    """Detect 'X vs. Y' or 'X vs Y' pattern with short title (typical sports matchup format)."""
    lower = title.lower()
    if " vs. " in lower or " vs " in lower:
        # Short titles with vs are almost always sports
        if len(title) < 80:
            return True
    return False

# Kalshi ticker prefixes that indicate sports markets
SPORTS_TICKER_PREFIXES = (
    "KXNBA", "KXNFL", "KXMLB", "KXNHL", "KXUCL", "KXEPL", "KXLALIGA",
    "KXBUNDES", "KXSERIE", "KXLIGUE", "KXPGA", "KXUFC", "KXATP", "KXWTA",
    "KXIPL", "KXT20", "KXCWC", "KXF1", "KXNASCAR", "KXMOTO",
    "KXNBASGP",  # NBA same-game parlay
)


def _is_sports(title: str, ticker: str = "", category: str = "") -> bool:
    # Ticker prefix check (most reliable for Kalshi)
    if ticker and ticker.upper().startswith(SPORTS_TICKER_PREFIXES):
        return True
    # Category check
    if category and category.lower() in ("sports",):
        return True
    # Title keyword check
    lower = title.lower()
    if any(kw in lower for kw in SPORTS_KEYWORDS):
        return True
    # Pro team name check
    if any(team in lower for team in SPORTS_TEAMS):
        return True
    # "X vs. Y" pattern (short titles)
    if _has_vs_pattern(title):
        return True
    return False


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
    ticker: str = "",
    category: str = "",
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
        threshold = TELEGRAM_SPORTS_THRESHOLD_USD if _is_sports(title, ticker, category) else TELEGRAM_THRESHOLD_USD
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
