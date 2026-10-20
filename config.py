import os
from dotenv import load_dotenv

load_dotenv()

# --- Anthropic ---
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# --- Kalshi ---
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")  # path to RSA PEM file
KALSHI_HOST = os.getenv("KALSHI_HOST", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_WS_HOST = os.getenv("KALSHI_WS_HOST", "wss://api.elections.kalshi.com/trade-api/ws/v2")
KALSHI_DEMO = os.getenv("KALSHI_DEMO", "false").lower() == "true"
if KALSHI_DEMO:
    KALSHI_HOST = "https://demo-api.kalshi.co/trade-api/v2"
    KALSHI_WS_HOST = "wss://demo-api.kalshi.co/trade-api/ws/v2"

# --- Twitter API v2 ---
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_IDS = [
    c.strip() for c in os.getenv("TELEGRAM_CHANNEL_IDS", "").split(",") if c.strip()
]

# --- NewsAPI (optional, RSS fallback) ---
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")

# --- RSS Feeds (fallback) ---
RSS_FEEDS = [
    # Tech / AI
    "https://news.google.com/rss/search?q=AI+artificial+intelligence&hl=en-US&gl=US&ceid=US:en",
    "https://feeds.feedburner.com/TechCrunch",
    "https://feeds.arstechnica.com/arstechnica/technology-lab",
    "https://www.theverge.com/rss/index.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    # Politics / Economy (matches Kalshi's biggest categories)
    "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",
    "https://feeds.bbci.co.uk/news/politics/rss.xml",
    "https://news.google.com/rss/search?q=federal+reserve+OR+inflation+OR+tariff+OR+interest+rate&hl=en-US&gl=US&ceid=US:en",
    # Weather (Kalshi temperature / hurricane markets)
    "https://news.google.com/rss/search?q=weather+forecast+OR+hurricane+OR+temperature+record&hl=en-US&gl=US&ceid=US:en",
    # Crypto
    "https://news.google.com/rss/search?q=bitcoin+OR+ethereum+OR+crypto&hl=en-US&gl=US&ceid=US:en",
    # Breaking / general
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
]

# --- Pipeline Settings ---
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MAX_BET_USD = float(os.getenv("MAX_BET_USD", "50"))
MAX_BET_SPORTS_USD = float(os.getenv("MAX_BET_SPORTS_USD", "25"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "100"))
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.10"))
NEWS_LOOKBACK_HOURS = 6

# --- V4 Settings ---
MAX_VOLUME_USD = float(os.getenv("MAX_VOLUME_USD", "500000"))
MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME_USD", "0"))
MATERIALITY_THRESHOLD = float(os.getenv("MATERIALITY_THRESHOLD", "0.6"))
SPEED_TARGET_SECONDS = float(os.getenv("SPEED_TARGET_SECONDS", "5"))
CLASSIFICATION_MODEL = "claude-haiku-4-5-20251001"
SCORING_MODEL = "claude-opus-4-6"
CLAUDE_MODEL = SCORING_MODEL  # alias used by scorer.py

# --- Categories to track ---
MARKET_CATEGORIES = [
    "ai",
    "technology",
    "crypto",
    "politics",
    "science",
    "economy",
    "weather",
    "health",
    "financials",
    "transportation",
    "companies",
    "social",
    "world",
    "other",
    # "sports" excluded — whale bot skips sports
]

# --- Twitter filter keywords (for filtered stream rules) ---
TWITTER_KEYWORDS = [
    "OpenAI", "GPT-5", "Anthropic", "Claude", "Google AI", "Gemini",
    "Bitcoin", "Ethereum", "Solana", "crypto",
    "Fed rate", "tariff", "Congress", "White House",
    "SpaceX", "Starship", "NASA",
    "Apple", "NVIDIA", "Microsoft", "Google",
]
