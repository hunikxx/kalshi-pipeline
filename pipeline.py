#!/usr/bin/env python3
"""
Kalshi Pipeline — V1 (synchronous) and V2 (async event-driven).
V1: Scrape → Score → Edge → Trade (loop-based)
V2: News stream → Match → Classify → Edge → Trade (event-driven)
"""
from __future__ import annotations

import asyncio
import time
import logging
from collections import OrderedDict

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

import config
import logger
from scraper import scrape_all
from markets import Market, fetch_active_markets, filter_by_categories
from scorer import score_market, filter_news_for_market
from edge import detect_edge, detect_edge_v2, Signal
from executor import execute_trade, execute_trade_async
from news_stream import NewsAggregator, NewsEvent
from market_watcher import MarketWatcher
from matcher import match_news_to_markets
from classifier import classify_async

console = Console()
log = logging.getLogger(__name__)


# ============================================================
# V2: Event-Driven Pipeline
# ============================================================

class PipelineV2:
    """Async event-driven pipeline. Runs indefinitely."""

    CLUSTER_WINDOW = 10.0  # seconds to buffer similar headlines

    def __init__(self):
        self.news_queue: asyncio.Queue = asyncio.Queue()
        self.signal_queue: asyncio.Queue = asyncio.Queue()
        self.news_aggregator = NewsAggregator(self.news_queue)
        self.market_watcher = MarketWatcher()
        self.running = False
        # Clustering buffer
        self._cluster_buffer: list[NewsEvent] = []
        self._cluster_queue: asyncio.Queue = asyncio.Queue()
        # Open positions for exit strategy
        self._open_positions: OrderedDict[str, Signal] = OrderedDict()  # market_id → Signal
        self._max_open = 500
        self.stats = {
            "news_processed": 0,
            "markets_matched": 0,
            "signals_found": 0,
            "trades_executed": 0,
            "clusters_formed": 0,
            "exits_triggered": 0,
        }

    async def run(self):
        """Start all pipeline components concurrently."""
        self.running = True
        mode = "[red bold]LIVE[/red bold]" if not config.DRY_RUN else "[yellow]DRY RUN[/yellow]"
        console.print(Panel(f"Pipeline V2 Starting  |  Mode: {mode}", style="bright_green"))
        console.print(f"  Niche filter: ${config.MIN_VOLUME_USD:,.0f} - ${config.MAX_VOLUME_USD:,.0f} volume")
        console.print(f"  Materiality threshold: {config.MATERIALITY_THRESHOLD}")
        console.print(f"  Speed target: {config.SPEED_TARGET_SECONDS}s")
        console.print()

        try:
            await asyncio.gather(
                self.news_aggregator.run(),
                self.market_watcher.run(),
                self._collect_news(),
                self._flush_clusters(),
                self._process_clusters(),
                self._execute_signals(),
                self._status_printer(),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            self.running = False

    # ------------------------------------------------------------------
    # Stage 1: Collect news into buffer
    # ------------------------------------------------------------------

    async def _collect_news(self):
        """Drain news queue into the cluster buffer."""
        while True:
            event: NewsEvent = await self.news_queue.get()
            self.stats["news_processed"] += 1
            logger.log_news_event(
                headline=event.headline,
                source=event.source,
                received_at=event.received_at.isoformat(),
                latency_ms=event.latency_ms,
            )
            self._cluster_buffer.append(event)

    # ------------------------------------------------------------------
    # Stage 2: Periodically flush buffer into clusters
    # ------------------------------------------------------------------

    @staticmethod
    def _headline_fingerprint(headline: str) -> str:
        """Reduce headline to sorted content tokens for clustering."""
        stopwords = {
            "the", "a", "an", "is", "are", "was", "were", "be", "to", "of",
            "in", "on", "at", "by", "for", "and", "or", "not", "it", "its",
            "this", "that", "has", "have", "had", "will", "would", "could",
            "says", "said", "report", "reports", "breaking", "just",
        }
        tokens = sorted(set(
            w.strip(".,!?:;\"'()[]") for w in headline.lower().split()
            if w.strip(".,!?:;\"'()[]") not in stopwords
            and len(w.strip(".,!?:;\"'()[]")) > 2
        ))
        return " ".join(tokens[:8])

    def _build_clusters(self, events: list[NewsEvent]) -> list[list[NewsEvent]]:
        """Group events whose fingerprints overlap significantly."""
        clusters: dict[str, list[NewsEvent]] = {}
        for ev in events:
            fp = self._headline_fingerprint(ev.headline)
            fp_tokens = set(fp.split())
            merged = False
            for key in list(clusters.keys()):
                key_tokens = set(key.split())
                overlap = len(key_tokens & fp_tokens)
                needed = min(3, max(len(key_tokens), len(fp_tokens)) // 2)
                if overlap >= needed:
                    clusters[key].append(ev)
                    merged = True
                    break
            if not merged:
                clusters[fp] = [ev]
        return list(clusters.values())

    async def _flush_clusters(self):
        """Every CLUSTER_WINDOW seconds, drain buffer and emit clusters."""
        while True:
            await asyncio.sleep(self.CLUSTER_WINDOW)
            if not self._cluster_buffer:
                continue
            batch = self._cluster_buffer[:]
            self._cluster_buffer.clear()
            clusters = self._build_clusters(batch)
            self.stats["clusters_formed"] += len(clusters)
            for cluster in clusters:
                await self._cluster_queue.put(cluster)

    # ------------------------------------------------------------------
    # Stage 3: Process each cluster — classify + detect edge + check exits
    # ------------------------------------------------------------------

    async def _process_clusters(self):
        """Consume clusters: match → classify → edge → queue signal."""
        while True:
            cluster: list[NewsEvent] = await self._cluster_queue.get()
            velocity = len(cluster)

            # Pick best representative (prefer lowest latency)
            cluster.sort(key=lambda e: e.latency_ms)
            primary = cluster[0]

            # Build combined headline for multi-source clusters
            if velocity > 1:
                sources = sorted(set(e.source for e in cluster))
                combined = (
                    f"{primary.headline}\n"
                    f"[{velocity} reports from: {', '.join(sources)}]"
                )
            else:
                combined = primary.headline

            # Match to markets
            matched = match_news_to_markets(
                primary.headline,
                self.market_watcher.tracked_markets,
            )
            if not matched:
                continue

            self.stats["markets_matched"] += len(matched)

            for market in matched:
                try:
                    classification = await classify_async(
                        combined, market, primary.source
                    )

                    # --- Exit check: does this contradict an open position? ---
                    existing = self._open_positions.get(market.condition_id)
                    if existing and self._is_contradicting(existing, classification):
                        self.stats["exits_triggered"] += 1
                        exit_signal = self._build_exit_signal(existing, classification, primary)
                        await self.signal_queue.put(exit_signal)
                        del self._open_positions[market.condition_id]
                        console.print(
                            f"  [red]EXIT[/red] "
                            f"[{primary.source}] contradicting {existing.side} "
                            f"on \"{market.question[:40]}...\" "
                            f"→ {exit_signal.side} ${exit_signal.bet_amount}"
                        )
                        continue

                    signal = detect_edge_v2(market, classification, primary)
                    if not signal:
                        continue

                    # Apply velocity bonus
                    if velocity >= 3:
                        signal.edge *= 1.3
                        signal.bet_amount = min(
                            round(signal.bet_amount * 1.3, 2), config.MAX_BET_USD
                        )
                    elif velocity >= 2:
                        signal.edge *= 1.15
                        signal.bet_amount = min(
                            round(signal.bet_amount * 1.15, 2), config.MAX_BET_USD
                        )

                    # Track as open position
                    self._open_positions[market.condition_id] = signal
                    while len(self._open_positions) > self._max_open:
                        self._open_positions.popitem(last=False)

                    self.stats["signals_found"] += 1
                    await self.signal_queue.put(signal)

                    vel_tag = f" vel:{velocity}x" if velocity > 1 else ""
                    console.print(
                        f"  [bright_green]SIGNAL[/bright_green] "
                        f"[{primary.source}] {classification.direction.upper()} "
                        f"mat:{classification.materiality:.2f}{vel_tag} "
                        f"→ {signal.side} ${signal.bet_amount} "
                        f"on \"{market.question[:40]}...\" "
                        f"({signal.total_latency_ms}ms)"
                    )
                except Exception as e:
                    log.warning(f"[pipeline] Classification error: {e}")

    # ------------------------------------------------------------------
    # Exit strategy helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_contradicting(existing: Signal, new_cls) -> bool:
        """Return True if new classification contradicts the open position."""
        if new_cls.direction == "neutral":
            return False
        if new_cls.materiality < config.MATERIALITY_THRESHOLD:
            return False
        # Bullish existing + bearish news = contradiction (and vice versa)
        if existing.side == "YES" and new_cls.direction == "bearish":
            return True
        if existing.side == "NO" and new_cls.direction == "bullish":
            return True
        return False

    @staticmethod
    def _build_exit_signal(existing: Signal, new_cls, event: NewsEvent) -> Signal:
        """Build a signal that closes the existing position."""
        # Flip the side to exit
        exit_side = "NO" if existing.side == "YES" else "YES"
        return Signal(
            market=existing.market,
            claude_score=new_cls.materiality,
            market_price=existing.market.yes_price,
            edge=existing.edge,  # exit at original edge for logging
            side=exit_side,
            bet_amount=existing.bet_amount,  # exit same size as entry
            reasoning=f"EXIT: {new_cls.reasoning}",
            headlines=event.headline,
            news_source=event.source,
            classification=new_cls.direction,
            materiality=new_cls.materiality,
            news_latency_ms=event.latency_ms,
            classification_latency_ms=new_cls.latency_ms,
            total_latency_ms=event.latency_ms + new_cls.latency_ms,
        )

    async def _execute_signals(self):
        """Execute trades from the signal queue."""
        while True:
            signal: Signal = await self.signal_queue.get()
            result = await execute_trade_async(signal)
            self.stats["trades_executed"] += 1

            status_color = "bright_green" if result["status"] in ("dry_run", "executed") else "red"
            console.print(
                f"  [{status_color}]{result['status']}[/{status_color}] "
                f"{result['side']} ${result['amount']:.2f} "
                f"on \"{result['market'][:40]}\" "
                f"(edge:{result['edge']:.1%} latency:{result.get('latency_ms', 0)}ms)"
            )

    async def _status_printer(self):
        """Print periodic status updates."""
        while True:
            await asyncio.sleep(30)
            ns = self.news_aggregator.stats
            console.print(
                f"\n  [dim]Status: "
                f"news={self.stats['news_processed']} "
                f"(tw:{ns.get('twitter', 0)} tg:{ns.get('telegram', 0)} rss:{ns.get('rss', 0)}) "
                f"clusters={self.stats['clusters_formed']} "
                f"matched={self.stats['markets_matched']} "
                f"signals={self.stats['signals_found']} "
                f"exits={self.stats['exits_triggered']} "
                f"trades={self.stats['trades_executed']} "
                f"positions={len(self._open_positions)} "
                f"markets={len(self.market_watcher.tracked_markets)}[/dim]\n"
            )


def run_pipeline_v2():
    """Entry point for V2 event-driven pipeline."""
    pipeline = PipelineV2()
    try:
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        console.print(f"\n[bright_green]Pipeline stopped. {pipeline.stats}[/bright_green]")


# ============================================================
# V1: Synchronous Loop Pipeline (preserved for backward compat)
# ============================================================

def run_pipeline(
    max_markets: int = 10,
    lookback_hours: int | None = None,
    categories: list[str] | None = None,
) -> list[dict]:
    """V1: Run the full pipeline once. Returns list of trade results."""

    run_id = logger.log_run_start()
    results = []
    signals: list[Signal] = []

    mode = "[yellow]DRY RUN[/yellow]" if config.DRY_RUN else "[red bold]LIVE[/red bold]"
    console.print(Panel(f"Pipeline V1 Run #{run_id}  |  Mode: {mode}", style="cyan"))

    # Step 1: Scrape News
    console.print("\n[bold]1. Scraping news...[/bold]")
    news = scrape_all(lookback_hours)
    console.print(f"   Found {len(news)} unique headlines")

    if not news:
        console.print("[yellow]   No news found. Aborting run.[/yellow]")
        logger.log_run_end(run_id, 0, 0, 0, "no_news")
        return results

    # Step 2: Fetch Markets
    console.print("\n[bold]2. Fetching Kalshi markets...[/bold]")
    all_markets = fetch_active_markets(limit=100)
    markets = filter_by_categories(all_markets, categories)[:max_markets]
    console.print(f"   {len(markets)} markets in target categories (of {len(all_markets)} total)")

    if not markets:
        console.print("[yellow]   No markets found. Aborting run.[/yellow]")
        logger.log_run_end(run_id, 0, 0, 0, "no_markets")
        return results

    # Step 3: Score Each Market
    console.print(f"\n[bold]3. Scoring {len(markets)} markets against news...[/bold]")

    for i, market in enumerate(markets):
        console.print(f"\n   [{i+1}/{len(markets)}] {market.question[:80]}")
        console.print(f"   Market price: YES={market.yes_price:.2f} NO={market.no_price:.2f}")

        relevant_news = filter_news_for_market(market, news)
        console.print(f"   Relevant headlines: {len(relevant_news)}")

        score_result = score_market(market, relevant_news)
        claude_score = score_result["confidence"]
        reasoning = score_result["reasoning"]
        console.print(f"   Claude score: {claude_score:.2f}  (market: {market.yes_price:.2f})")

        headlines_str = "\n".join(n.headline for n in relevant_news[:5])
        signal = detect_edge(market, claude_score, reasoning, headlines_str)

        if signal:
            edge_pct = signal.edge * 100
            console.print(f"   [green bold]SIGNAL: {signal.side} | Edge: {edge_pct:.1f}% | Size: ${signal.bet_amount}[/green bold]")
            signals.append(signal)
        else:
            edge = abs(claude_score - market.yes_price)
            console.print(f"   [dim]No edge (diff: {edge:.2f}, threshold: {config.EDGE_THRESHOLD})[/dim]")

        time.sleep(0.5)

    # Step 4: Execute Trades
    if signals:
        console.print(f"\n[bold]4. Executing {len(signals)} trades...[/bold]")
        for signal in signals:
            result = execute_trade(signal)
            results.append(result)
            status_color = "green" if result["status"] in ("dry_run", "executed") else "red"
            console.print(f"   [{status_color}]{result['status']}[/{status_color}] {result['market'][:60]} | {result['side']} ${result['amount']}")
    else:
        console.print("\n[bold]4. No signals — nothing to execute.[/bold]")

    logger.log_run_end(run_id, len(markets), len(signals), len(results))
    _print_summary(results, len(markets), len(signals))
    return results


def _print_summary(results: list[dict], markets_scanned: int, signals_found: int):
    table = Table(title="Pipeline Summary", show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Markets scanned", str(markets_scanned))
    table.add_row("Signals found", str(signals_found))
    table.add_row("Trades placed", str(len(results)))
    table.add_row("Mode", "DRY RUN" if config.DRY_RUN else "LIVE")
    console.print(table)


if __name__ == "__main__":
    run_pipeline()
