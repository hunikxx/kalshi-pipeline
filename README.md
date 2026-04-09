# Kalshi Pipeline V4

An AI-powered breaking news detector that classifies events against Kalshi prediction markets and trades automatically when it finds edge.

```
Breaking News (Twitter / Telegram / RSS)
        ↓ cluster similar headlines (10s window)
Match to niche markets (< $500K volume)
        ↓
Claude Classification: bullish / bearish / neutral + materiality + confidence
        ↓
Edge = materiality × confidence × source_weight × time_decay × price_room
        ↓
Adaptive Kelly sizing → duplicate/correlation guard → execute
        ↓
SQLite log → calibration tracking → exit on contradicting news
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

### V3 → V4: Smarter Edge + Revenue Optimization

V3 was reliable but left money on the table. V4 makes the core algorithm significantly smarter:

**Better signal quality:**
- **Matcher minimum threshold**: Requires 2+ keyword hits AND 15% relevance score — eliminates single-keyword false positives (e.g. "OpenAI" alone no longer matches every AI market)
- **Enriched classification prompt**: Claude now sees market end date, time remaining, volume, and source reliability tier — can judge "already priced in" vs "fresh news"
- **Confidence output**: Claude rates its own certainty (0-1). Low-confidence classifications (< 0.4) are rejected entirely

**Smarter edge formula:**
- **Old**: `edge = materiality × price_room`
- **New**: `edge = materiality × confidence × source_weight × time_decay × price_room`
- **Source weighting**: Reuters/Bloomberg = 1.0×, TechCrunch/RSS = 0.75×, Twitter/Telegram = 0.5×
- **Time decay**: Markets closing in < 1 day get 1.2× (news is very actionable), 30+ days get 0.5× (will be priced in)

**Adaptive position sizing:**
- Kelly multiplier scales with calibration accuracy: 0.05× at < 45% (canary), up to 0.5× at 75%+ (proven edge)
- Removed arbitrary `bankroll = daily_limit × 10` — bankroll is now just `DAILY_LOSS_LIMIT`

**News velocity clustering:**
- Headlines buffered in 10s windows, grouped by token-overlap fingerprint
- 3 tweets about "Fed cuts rates" → 1 classification with "[3 reports from: twitter, rss, telegram]"
- Velocity multiplier: 2 sources = 1.15× edge, 3+ sources = 1.3× edge

**Exit strategy:**
- Pipeline tracks open positions
- Contradicting news (long YES + new bearish with materiality above threshold) triggers an EXIT signal
- Exit flips the side and matches entry size

**Duplicate trade prevention:**
- Same `(headline, market)` pair won't re-fire within 5-minute cooldown
- Bounded to 1K entries with FIFO eviction

**Kalshi migration:**
- Full migration from Polymarket to Kalshi API (RSA-PSS auth, REST + WebSocket)
- Contract-based order sizing, limit orders via `POST /portfolio/orders`
- Demo mode support (`KALSHI_DEMO=true`)

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

### V4 Pipeline (Event-Driven)

```
news_stream.py      Real-time news — Twitter API v2, Telegram, RSS fallback
pipeline.py         Event-driven orchestrator — clustering, exit strategy, velocity (asyncio)
matcher.py          Routes breaking news to relevant markets (min 2-keyword threshold)
classifier.py       Claude classification — direction + materiality + confidence + source tier
edge.py             Composite edge — source weight × confidence × time decay × price room
                    Adaptive Kelly sizing from calibration accuracy
executor.py         Trade execution — dry-run + live Kalshi orders (async)
market_watcher.py   Kalshi WebSocket + REST — live prices, niche filter, momentum
kalshi_auth.py      RSA-PSS request signing for Kalshi API
calibrator.py       Tracks classification accuracy over time
backtest.py         Historical replay for strategy validation
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

### 1. News Detection
Real-time streams from Twitter (filtered by keywords: OpenAI, Bitcoin, Fed rate, etc.), Telegram channels, and RSS fallback. Events are deduplicated and timestamped with receive latency.

### 2. News Clustering (V4)
Headlines are buffered in 10-second windows. Similar headlines are grouped by token-overlap fingerprint — so "Fed cuts rates by 50bp" and "Federal Reserve cuts interest rates" become one cluster. The best headline (lowest latency, highest-tier source) represents the cluster. Multi-source clusters get a velocity multiplier.

### 3. Market Matching
Each headline is matched to active niche markets (<$500K volume) by keyword overlap. Requires at least 2 keyword hits and a 15% relevance score to prevent false positives. Only relevant markets proceed to classification.

### 4. Classification
Instead of "what's the probability?", Claude is asked: *"Does this news make the market MORE likely to resolve YES, MORE likely to resolve NO, or is it NOT RELEVANT?"*

Claude sees the full market context: current price, end date, time remaining, volume, and source reliability tier. It returns direction, materiality (0-1), and confidence (0-1).

### 5. Edge Detection (V4)
Edge is a composite score: `materiality × confidence × source_weight × time_decay × price_room`.

- Source weight: Reuters = 1.0×, RSS = 0.75×, Twitter = 0.5×
- Time decay: expiring markets (< 1 day) get 1.2×, long-horizon (30+ days) get 0.5×
- Velocity bonus: 2 corroborating sources = 1.15×, 3+ = 1.3×
- Position sizing uses adaptive Kelly — multiplier scales from 0.05× to 0.5× based on proven calibration accuracy

### 6. Exit Strategy (V4)
The pipeline tracks open positions. When contradicting news arrives (e.g., long YES on a market, then strong bearish news), an exit signal fires — flips the side and matches the entry size.

### 7. Execution
Dry-run by default. Live mode places limit orders via the Kalshi API. Safety: $25 max bet, $100 daily limit. Duplicate trade prevention blocks the same headline×market pair from firing within 5 minutes.

### 8. Calibration
Every trade is tracked. As markets resolve, the system measures whether its classifications were correct. Accuracy feeds back into adaptive Kelly sizing — proven accuracy means larger bets.

---

## Configuration

| Setting | Default | What it does |
|---|---|---|
| `DRY_RUN` | `true` | Set to `false` for live trading |
| `MAX_BET_USD` | `25` | Maximum single bet |
| `DAILY_LOSS_LIMIT_USD` | `100` | Pipeline halts if breached |
| `EDGE_THRESHOLD` | `0.10` | Minimum edge to trigger trade |
| `MAX_VOLUME_USD` | `500000` | Only trade markets below this volume |
| `MIN_VOLUME_USD` | `1000` | Skip dead markets |
| `MATERIALITY_THRESHOLD` | `0.6` | Minimum materiality to act on |
| `SPEED_TARGET_SECONDS` | `5` | Target news-to-trade latency |

---

## Safety

- Dry-run mode ON by default
- $25 max single bet, $100 daily limit
- Adaptive Kelly sizing — shrinks bets when accuracy is unproven, grows when calibrated
- Circuit breaker — pauses classification after 10 consecutive failures (60s cooldown)
- Duplicate trade prevention — same headline×market can't fire within 5 minutes
- Confidence gate — low-confidence classifications (< 0.4) are rejected
- Source discounting — unverified Twitter/Telegram rumors get 0.5× weight
- Exit strategy — contradicting news triggers position exit
- Niche market filter prevents competing against sophisticated bots
- Graceful shutdown — SIGINT/SIGTERM properly cancels all tasks
- All API keys in `.env`, never committed

---

Built by [@hunikxx](https://github.com/hunikxx)

---

## Disclaimer

This project is for **entertainment and educational purposes only**. It is not financial advice. The authors are not responsible for any financial losses incurred through the use of this software. Prediction market trading carries significant risk — you can lose money. Never trade with funds you cannot afford to lose. Past performance of any strategy does not guarantee future results. Use at your own risk.
