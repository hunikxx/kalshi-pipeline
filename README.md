# Kalshi Pipeline V5

A whale-following prediction market bot that tracks large trades on both Polymarket and Kalshi, then auto-trades on Kalshi.

```
Kalshi WebSocket Trade Feed (real-time)
        ↓ detect $15K+ whale trades ($25K for sports)
        ↓ fetch unknown markets on-the-fly
        ↓ price filter ($0.05–$0.85)
        ↓ OI-weighted bet sizing + sweet spot pricing + whale clustering
        ↓ try at whale price, bump +1¢/+2¢/+3¢ if not filled, cancel if still unfilled
        ↓ auto-trade on Kalshi

Polymarket Public API (5s polling)
        ↓ detect $20K+ whale trades (politics + crypto)
        ↓ track wallets — patterns: doubling down, flip, hedge, repeat
        ↓ score 0–100 → threshold 30+ to trade
        ↓ keyword match to Kalshi market (45%+ Jaccard)
        ↓ auto-trade on Kalshi

Both → SQLite log → live dashboard (P/L from Kalshi API) → exit on whale reversal or +25¢ price target
```

## Version History

### V1 → V2: The Classification Pivot

V1 scraped RSS feeds (5-60 min delay), asked Claude "what's the probability?" (wrong question for LLMs), and competed on high-volume markets (where every bot already operates).

V2 inverted all three:
- **Speed**: Real-time Twitter/Telegram streams instead of stale RSS
- **Classification**: Claude classifies "bullish or bearish?" instead of estimating probability — a task LLMs are actually good at
- **Niche markets**: Only trades markets under $500K volume where the crowd is small and slow

### V2 → V3: Hardened for Production

V2 had critical bugs that would bite in 24/7 operation. V3 fixes them:

- **Race condition fix**: `asyncio.Lock` protects `tracked_markets` and `snapshots` from concurrent WebSocket + refresh modification
- **Robust JSON parsing**: Replaced brittle backtick-split with 3-strategy extractor (code fence regex → brace extraction → raw parse)
- **Memory leak fix**: Unbounded dedup sets replaced with `OrderedDict` with FIFO eviction (5K/10K caps)
- **Graceful shutdown**: Named `asyncio.Task`s, proper SIGINT/SIGTERM handlers, awaited cleanup
- **Circuit breaker**: Pauses classification for 60s after 10 consecutive failures, prevents cascade

### V4 → V5: Whale Following

V4 relied on news classification with Claude. V5 pivots to a pure whale-following strategy with zero LLM cost:

**Dual-source whale tracking:**
- **Kalshi WebSocket**: Real-time trade feed via `trade` channel — every executed trade streamed instantly
- **Polymarket polling**: Public API polled every 5s for $20K+ trades in politics + crypto sectors
- **On-the-fly market fetch**: Unknown tickers are fetched from Kalshi API and cached — never misses a whale

**Wallet intelligence (Polymarket):**
- Tracks wallet addresses (SHA-256 hashed) with full trade history
- Detects patterns: doubling_down, flip, hedge, heavy_actor, repeat_whale
- Scores alerts 0–100 based on trade size, wallet history, pattern, price extremity
- Score 30+ triggers auto-trade on matched Kalshi market

**Cross-platform matching:**
- Keyword Jaccard similarity (45% threshold) matches Polymarket titles to Kalshi markets
- Stopword filtering for accurate matching
- 5-minute cooldown per market prevents duplicate trades

**Smart bet sizing:**
- **Open interest weighting**: Whale trade as % of market OI scales the bet
- **Sweet spot pricing**: Lower-priced contracts (more upside) get up to 1.5x bet multiplier
- **Whale clustering**: 2+ whales on same market in 10 min = 1.5–2x bet boost
- Price filter: only trade contracts between $0.05–$0.85

**Order execution:**
- Try at whale's price +1¢, bump to +2¢ then +3¢ if not filled (10s per attempt)
- Auto-cancel unfilled orders after all attempts
- Sports markets require $25K+ whale (higher noise threshold)

**Exit strategy:**
- Whale reversal: same wallet flips to opposite side → sell
- Price target: +25¢ move in our favor → sell
- Startup holdings check: auto-sell positions above threshold on restart

**Live dashboard:**
- Real-time P/L from Kalshi portfolio API (cached 30s)
- Balance, unrealized P/L, realized P/L, total value
- Timestamped trade log with fill status (FILLED/CANCELLED/DRY RUN)

---

## Setup (2 minutes)

### One-Command Setup

```bash
git clone https://github.com/hunikxx/kalshi-pipeline.git
cd kalshi-pipeline
bash setup.sh
```

### Manual Setup

```bash
git clone https://github.com/hunikxx/kalshi-pipeline.git
cd kalshi-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Add your keys to `.env`:

```
ANTHROPIC_API_KEY=sk-ant-...         # Required
TWITTER_BEARER_TOKEN=...             # Optional — real-time news stream
TELEGRAM_BOT_TOKEN=...               # Optional — channel monitoring
KALSHI_API_KEY_ID=...                # Optional — live trading only
KALSHI_PRIVATE_KEY_PATH=./key.pem    # Optional — RSA private key for Kalshi
```

### Verify

```bash
python cli.py verify
```

---

## How to Use

### V2: Event-Driven Pipeline (Recommended)

```bash
# Start the real-time pipeline — monitors news streams, classifies, trades
python cli.py watch

# Enable live trading
python cli.py watch --live
```

The `watch` command runs indefinitely. It connects to your configured news sources (Twitter, Telegram, RSS fallback), matches breaking headlines to niche Kalshi markets, classifies each with Claude, and executes trades when it finds edge.

### V1: Synchronous Pipeline

```bash
# Single scan — scrape RSS, score markets, log signals
python cli.py run

python cli.py run --max 15 --hours 12
```

### Live Dashboard

```bash
python cli.py dashboard
```

### Backtest

```bash
# Validate the V2 strategy against resolved markets
python cli.py backtest

python cli.py backtest --limit 50 --category ai
```

### All Commands

| Command | What it does |
|---|---|
| `python cli.py watch` | V2: Real-time event-driven pipeline |
| `python cli.py run` | V1: Synchronous RSS-based pipeline |
| `python cli.py dashboard` | Live terminal dashboard |
| `python cli.py backtest` | Backtest against resolved markets |
| `python cli.py calibrate` | Classification accuracy report |
| `python cli.py niche` | Browse niche markets (volume-filtered) |
| `python cli.py verify` | Check all API keys and connections |
| `python cli.py scrape` | Test news scraper |
| `python cli.py markets` | Browse all active markets |
| `python cli.py trades` | View trade log |
| `python cli.py stats` | Performance + latency + calibration stats |

---

## Architecture

### V5 Pipeline (Whale Following)

```
market_watcher.py      Kalshi WebSocket trade feed + HTTP fallback — real-time whale detection
polymarket_tracker.py  Polymarket $20K+ whale tracking — wallet profiling, pattern detection
pipeline.py            Orchestrator — dual whale feeds → signal scoring → auto-trade (asyncio)
markets.py             Kalshi market loading via /events API + on-the-fly ticker fetch
executor.py            Trade execution — dry-run + live Kalshi orders (async)
kalshi_auth.py         RSA-PSS request signing for Kalshi API
```

### Shared Infrastructure

```
logger.py           SQLite — trades, news events, calibration, latency tracking
config.py           All settings, API keys, thresholds
dashboard.py        Bloomberg Terminal-style live dashboard
cli.py              CLI — watch, run, backtest, calibrate, niche, verify, etc.
```

---

## How It Actually Works

### 1. Kalshi Trade Feed
WebSocket connects to `wss://api.elections.kalshi.com/trade-api/ws/v2` and subscribes to the `trade` channel. Every executed trade on Kalshi streams in real-time. HTTP polling (`/markets/trades`) activates as fallback if WebSocket drops.

### 2. Polymarket Whale Detection
Public API polled every 5 seconds for trades $20K+ in politics and crypto sectors. Each trade's wallet address is tracked — the system builds a profile of repeat whales and detects behavioral patterns (doubling down, flipping, hedging).

### 3. Kalshi Whale Detection
Trades $15K+ trigger auto-trade signals ($25K for sports markets). Unknown market tickers are fetched on-the-fly from the Kalshi API and cached. Contracts priced below $0.05 or above $0.85 are filtered out.

### 4. Cross-Platform Matching
Polymarket whale trades are matched to Kalshi markets by keyword Jaccard similarity (45% threshold). Stopwords are removed before comparison. Matched trades are scored 0–100 — score 35+ triggers a trade.

### 5. Bet Sizing
Bet amount ($1–$10) is calculated from multiple factors:
- **OI ratio**: whale trade as percentage of market open interest
- **Sweet spot pricing**: lower-priced contracts (more upside) get up to 1.5x multiplier
- **Whale clustering**: multiple whales on same market in 10 min = 1.5–2x boost

### 6. Order Execution
Orders try at whale price +1¢. If not filled in 10s, bumps to +2¢, then +3¢. Auto-cancels after all attempts. Dry-run by default. Safety: $10 max bet, $100 daily limit. 5-minute cooldown per market.

### 7. Exit Strategy
- **Whale reversal**: same Polymarket wallet flips to opposite side → sell
- **Price target**: +25¢ move in our favor → sell
- **Startup check**: scans existing holdings on restart, sells any above threshold

---

## Configuration

| Setting | Default | What it does |
|---|---|---|
| `DRY_RUN` | `true` | Set to `false` for live trading |
| `MAX_BET_USD` | `10` | Maximum single bet |
| `DAILY_LOSS_LIMIT_USD` | `100` | Pipeline halts if breached |
| `KALSHI_WHALE_USD` | `15000` | Kalshi trade size to auto-trade ($25K sports) |
| `POLY_WHALE_USD` | `20000` | Polymarket trade size to track |
| `POLY_SCORE_MIN` | `30` | Minimum Polymarket whale score to trade |
| `POLY_MATCH_PCT` | `45%` | Keyword match threshold for cross-platform |
| `PRICE_FILTER` | `$0.05–$0.85` | Only trade contracts in this range |
| `PRICE_TARGET` | `$0.25` | Sell when up 25¢ per contract |

---

## Safety

- Dry-run mode ON by default
- $10 max single bet, $100 daily limit
- Price filter: only trade contracts $0.05–$0.85
- Duplicate trade prevention — 5-minute cooldown per market
- Exit strategy — whale reversal + price target (+8¢)
- Trade deduplication by trade_id (WebSocket) and tx_hash (Polymarket)
- Wallet addresses SHA-256 hashed, never stored in plaintext
- All API keys in `.env`, never committed
- Zero LLM cost — no Claude API calls in V5

---

Built by [@hunikxx](https://github.com/hunikxx)

---

## Disclaimer

This project is for **entertainment and educational purposes only**. It is not financial advice. The authors are not responsible for any financial losses incurred through the use of this software. Prediction market trading carries significant risk — you can lose money. Never trade with funds you cannot afford to lose. Past performance of any strategy does not guarantee future results. Use at your own risk.
