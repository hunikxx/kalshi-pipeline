#!/usr/bin/env python3
"""
Kalshi Pipeline — Live Terminal Dashboard
Read-only viewer that displays data from the V4 pipeline's SQLite database.
Run alongside `python cli.py watch` in a separate terminal.
"""
from __future__ import annotations

import time
import sys
from datetime import datetime, timezone

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

import config
import logger


def _is_live_mode() -> bool:
    """Detect mode from recent trades in the database."""
    trades = logger.get_recent_trades(limit=5)
    if not trades:
        return not config.DRY_RUN
    return any(t["status"] in ("filled", "placed", "rejected_daily_limit") or
               t["status"].startswith("error_http") for t in trades)

console = Console()

# --- Color Palette ---
ACCENT = "bright_green"
DIM = "bright_black"
WARN = "yellow"
LOSS = "red"
WIN = "bright_green"
MUTED = "dim white"
WHALE = "bright_magenta"
POLY = "bright_cyan"


def make_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=2),
    )
    layout["left"].split_column(
        Layout(name="status", ratio=1),
        Layout(name="performance", ratio=1),
    )
    layout["right"].split_column(
        Layout(name="poly_whales", ratio=3),
        Layout(name="kalshi_flow", ratio=2),
        Layout(name="trades", ratio=3),
    )
    return layout


def render_header() -> Panel:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    grid = Table.grid(expand=True)
    grid.add_column(justify="left", ratio=1)
    grid.add_column(justify="center", ratio=2)
    grid.add_column(justify="right", ratio=1)
    grid.add_row(
        Text(" KALSHI PIPELINE", style="bold bright_green"),
        Text("POLYMARKET WHALES + KALSHI FLOW → TRADE", style=DIM),
        Text(f"{now} ", style=MUTED),
    )
    return Panel(grid, style="bright_green", box=box.HEAVY)


def render_status() -> Panel:
    table = Table(show_header=False, box=None, padding=(0, 1), expand=True)
    table.add_column("label", style=MUTED, width=18)
    table.add_column("value", style=ACCENT)

    stats = logger.get_trade_stats()
    all_flow = logger.get_recent_flow_alerts(limit=200)

    # Separate poly vs kalshi flow alerts
    poly_alerts = [a for a in all_flow if a.get("ticker", "").startswith("POLY:")]
    kalshi_alerts = [a for a in all_flow if not a.get("ticker", "").startswith("POLY:")]

    # Pipeline status from flow activity
    if all_flow:
        last_time = all_flow[0].get("created_at", "")
        try:
            last_dt = datetime.fromisoformat(last_time)
            age = (datetime.now(timezone.utc) - last_dt.replace(tzinfo=timezone.utc)).total_seconds()
            if age < 30:
                status_text = "[bright_green]●[/bright_green] ACTIVE"
            elif age < 120:
                status_text = "[yellow]◌[/yellow] POLLING"
            else:
                status_text = "[red]○[/red] STALE"
        except (ValueError, TypeError):
            status_text = "[yellow]○[/yellow] UNKNOWN"
    else:
        status_text = "[yellow]○[/yellow] WAITING"

    live = _is_live_mode()
    mode = "[bright_green]LIVE[/bright_green]" if live else f"[{WARN}]DRY RUN[/{WARN}]"

    table.add_row("Pipeline", status_text)
    table.add_row("Mode", mode)
    table.add_row("", "")
    table.add_row("Max Bet", f"${config.MAX_BET_USD:.2f}")
    table.add_row("Daily Limit", f"${config.DAILY_LOSS_LIMIT_USD:.2f}")
    table.add_row("", "")
    table.add_row("Poly Whales", f"[{WHALE}]{len(poly_alerts)}[/{WHALE}]")
    table.add_row("Kalshi Flow", f"[{POLY}]{len(kalshi_alerts)}[/{POLY}]")
    table.add_row("Total Signals", f"[{ACCENT}]{stats['total_trades']}[/{ACCENT}]")
    table.add_row("", "")
    table.add_row("Kalshi Whale", "$15K+ ($25K sports)")
    table.add_row("Kalshi Display", "$5,000+ dim")
    table.add_row("Poly Whale", "$20,000+")
    table.add_row("Poly Match", "45%+ keyword")
    table.add_row("Poly Poll", "5s")
    table.add_row("Sectors", f"[{POLY}]POLITICS CRYPTO[/{POLY}]")
    table.add_row("Price Filter", "$0.05–$0.85")
    table.add_row("Poly Score", "30+ to trade")
    table.add_row("Boosts", "sweet spot + cluster")

    return Panel(table, title="[bold]PIPELINE STATUS[/bold]", border_style="bright_green", box=box.ROUNDED)


def render_performance() -> Panel:
    stats = logger.get_trade_stats()
    trades = logger.get_recent_trades(limit=100)
    daily_spent = abs(logger.get_daily_pnl())
    cal = logger.get_calibration_stats()

    total = stats["total_trades"]
    by_status = stats["by_status"]
    dry_runs = by_status.get("dry_run", 0)
    filled = by_status.get("filled", 0)
    placed = by_status.get("placed", 0)
    errors = sum(v for k, v in by_status.items() if k.startswith("error"))

    # Separate by source
    poly_trades = [t for t in trades if (t.get("news_source") or "") == "polymarket"]
    flow_trades = [t for t in trades if (t.get("news_source") or "") == "flow"]
    exit_trades = [t for t in trades if "EXIT" in (t.get("reasoning") or "")]

    total_wagered = stats.get("total_wagered", 0)
    avg_edge = sum(t.get("edge", 0) for t in trades) / max(len(trades), 1) * 100

    table = Table(show_header=False, box=None, padding=(0, 1), expand=True)
    table.add_column("label", style=MUTED, width=18)
    table.add_column("value")

    table.add_row("Total Trades", f"[{ACCENT}]{total}[/{ACCENT}]")
    table.add_row("  Poly whale", f"[{WHALE}]{len(poly_trades)}[/{WHALE}]")
    table.add_row("  Kalshi flow", f"[{POLY}]{len(flow_trades)}[/{POLY}]")
    table.add_row("  Exits", f"[{LOSS}]{len(exit_trades)}[/{LOSS}]")
    table.add_row("", "")
    table.add_row("Dry Runs", f"[{WARN}]{dry_runs}[/{WARN}]")
    cancelled = by_status.get("cancelled", 0)
    table.add_row("Filled", f"[{WIN}]{filled}[/{WIN}]")
    table.add_row("Cancelled", f"[{LOSS}]{cancelled}[/{LOSS}]")
    if errors:
        table.add_row("Errors", f"[{LOSS}]{errors}[/{LOSS}]")
    table.add_row("", "")
    # Live P/L from Kalshi
    pnl = logger.get_portfolio_pnl()
    unrealized = pnl["unrealized_pnl"]
    pnl_color = WIN if unrealized >= 0 else LOSS
    pnl_sign = "+" if unrealized >= 0 else ""

    realized = pnl.get("realized_pnl", 0)
    rpnl_color = WIN if realized >= 0 else LOSS
    rpnl_sign = "+" if realized >= 0 else ""
    total_bal = pnl.get("total_balance", 0)

    table.add_row("Balance", f"[{ACCENT}]${pnl['balance']:.2f}[/{ACCENT}]")
    table.add_row("Total Value", f"[{ACCENT}]${total_bal:.2f}[/{ACCENT}]")
    table.add_row("Open Positions", f"[{ACCENT}]{pnl['open_positions']}[/{ACCENT}]")
    table.add_row("Unrealized P/L", f"[{pnl_color}]{pnl_sign}${unrealized:.2f}[/{pnl_color}]")
    table.add_row("Realized P/L", f"[{rpnl_color}]{rpnl_sign}${realized:.2f}[/{rpnl_color}]")
    table.add_row("", "")
    table.add_row("Daily Exposure", f"[{ACCENT}]${daily_spent:.2f}[/{ACCENT}]")
    table.add_row("Total Wagered", f"[{ACCENT}]${total_wagered:.2f}[/{ACCENT}]")

    if cal["total"] > 0:
        color = WIN if cal["accuracy"] >= 55 else (WARN if cal["accuracy"] >= 45 else LOSS)
        table.add_row("", "")
        table.add_row("Accuracy", f"[{color}]{cal['accuracy']:.1f}%[/{color}] ({cal['total']} resolved)")

    if trades:
        best = max(t.get("edge", 0) for t in trades)
        table.add_row("Best Edge", f"[{WIN}]{best:.1%}[/{WIN}]")

    return Panel(table, title="[bold]PERFORMANCE[/bold]", border_style="bright_cyan", box=box.ROUNDED)


def render_poly_whales() -> Panel:
    """Show recent Polymarket whale alerts."""
    all_alerts = logger.get_recent_flow_alerts(limit=100)
    poly_alerts = [a for a in all_alerts if a.get("ticker", "").startswith("POLY:")][:12]

    table = Table(show_header=True, box=box.SIMPLE_HEAD, expand=True, padding=(0, 1))
    table.add_column("Time", width=6, style=MUTED)
    table.add_column("Market", max_width=28)
    table.add_column("Side", justify="center", width=10)
    table.add_column("Size", justify="right", width=9)
    table.add_column("Price", justify="right", width=6)
    table.add_column("Score", justify="center", width=6)

    if not poly_alerts:
        table.add_row(f"[{DIM}]Polling Polymarket for $25K+ trades...[/{DIM}]", "", "", "", "", "")
    else:
        for a in poly_alerts:
            created = a.get("created_at", "")[:19]
            time_str = created[11:16] if len(created) >= 16 else "—"

            raw_side = a.get("side", "")
            # Parse "BUY_Yes" / "SELL_No" format
            if "_" in raw_side:
                action, outcome = raw_side.split("_", 1)
                if action == "BUY":
                    side_str = f"[{WIN}]BUY {outcome}[/{WIN}]"
                else:
                    side_str = f"[{LOSS}]SELL {outcome}[/{LOSS}]"
            else:
                side_str = f"[{DIM}]{raw_side}[/{DIM}]"

            size = a.get("size", 0)
            price = a.get("price", 0)
            score_val = a.get("multiple", 0) * 10  # we stored score/10 as multiple

            if score_val >= 60:
                score_str = f"[{LOSS}]{score_val:.0f}[/{LOSS}]"
            elif score_val >= 35:
                score_str = f"[{WARN}]{score_val:.0f}[/{WARN}]"
            else:
                score_str = f"[{DIM}]{score_val:.0f}[/{DIM}]"

            table.add_row(
                time_str,
                a["market_question"][:28],
                side_str,
                f"[bold]${size:,.0f}[/bold]",
                f"${price:.2f}" if price > 0 else "—",
                score_str,
            )

    return Panel(
        table,
        title="[bold]POLYMARKET WHALES[/bold]  ·  Politics + Crypto · $25K+",
        border_style="bright_magenta",
        box=box.ROUNDED,
    )


def render_kalshi_flow() -> Panel:
    """Show recent Kalshi orderbook flow alerts."""
    all_alerts = logger.get_recent_flow_alerts(limit=100)
    kalshi_alerts = [a for a in all_alerts if not a.get("ticker", "").startswith("POLY:")][:8]

    table = Table(show_header=True, box=box.SIMPLE_HEAD, expand=True, padding=(0, 1))
    table.add_column("Time", width=6, style=MUTED)
    table.add_column("Market", max_width=25)
    table.add_column("Side", justify="center", width=5)
    table.add_column("Size", justify="right", width=8)
    table.add_column("Mult", justify="right", width=5)
    table.add_column("Type", justify="center", width=6)

    if not kalshi_alerts:
        table.add_row(f"[{DIM}]Streaming Kalshi trades...[/{DIM}]", "", "", "", "", "")
    else:
        for a in kalshi_alerts:
            created = a.get("created_at", "")[:19]
            time_str = created[11:16] if len(created) >= 16 else "—"
            side_style = WIN if a["side"] == "YES" else WHALE
            mult = a.get("multiple", 0)
            mult_color = LOSS if mult >= 5 else (WARN if mult >= 3 else ACCENT)
            size = a.get("size", 0)

            if mult >= 5:
                type_str = f"[{LOSS}]WHALE[/{LOSS}]"
            else:
                type_str = f"[{DIM}]ORDER[/{DIM}]"

            table.add_row(
                time_str,
                a["market_question"][:25],
                f"[{side_style}]{a['side']}[/{side_style}]",
                f"[bold]{size:,.0f}[/bold]",
                f"[{mult_color}]{mult:.1f}x[/{mult_color}]",
                type_str,
            )

    return Panel(
        table,
        title="[bold]KALSHI FLOW[/bold]  ·  Trade Feed (WS + HTTP)",
        border_style="bright_cyan",
        box=box.ROUNDED,
    )


def render_trades() -> Panel:
    trades = logger.get_recent_trades(limit=10)

    table = Table(show_header=True, box=box.SIMPLE_HEAD, expand=True, padding=(0, 1))
    table.add_column("Time", width=6, style=MUTED)
    table.add_column("Action", justify="center", width=8)
    table.add_column("Market", max_width=28)
    table.add_column("Bet", justify="right", width=7)
    table.add_column("Edge", justify="right", width=6)
    table.add_column("Src", width=8)
    table.add_column("Status", justify="center", width=9)

    if not trades:
        table.add_row(f"[{DIM}]No trades yet — tracking whales...[/{DIM}]", "", "", "", "", "", "")
    else:
        for t in trades:
            created = t.get("created_at", "")[:19]
            time_str = created[11:16] if len(created) >= 16 else "—"
            reasoning = t.get("reasoning") or ""
            src = (t.get("news_source") or "—")[:8]
            is_exit = "EXIT" in reasoning

            # Action column with color
            if is_exit:
                action_str = f"[bold {LOSS}]SELL[/bold {LOSS}]"
            elif t["side"] == "YES":
                action_str = f"[bold {WIN}]BUY YES[/bold {WIN}]"
            else:
                action_str = f"[bold {WHALE}]BUY NO[/bold {WHALE}]"

            # Source with color
            if src == "polymark":
                src_str = f"[{WHALE}]POLY[/{WHALE}]"
            elif src == "flow":
                src_str = f"[{POLY}]FLOW[/{POLY}]"
            else:
                src_str = f"[{DIM}]{src}[/{DIM}]"

            # Status
            status = t["status"]
            if status == "dry_run":
                status_str = f"[{WARN}]DRY RUN[/{WARN}]"
            elif status == "filled":
                status_str = f"[{WIN}]FILLED[/{WIN}]"
            elif status == "cancelled":
                status_str = f"[{LOSS}]CANCEL[/{LOSS}]"
            elif status.startswith("error"):
                status_str = f"[{LOSS}]ERROR[/{LOSS}]"
            elif status == "rejected_daily_limit":
                status_str = f"[{LOSS}]LIMIT[/{LOSS}]"
            else:
                status_str = f"[{DIM}]{status[:9]}[/{DIM}]"

            table.add_row(
                time_str,
                action_str,
                t["market_question"][:28],
                f"${t['amount_usd']:.2f}",
                f"{t['edge']:.0%}",
                src_str,
                status_str,
            )

    return Panel(table, title="[bold]TRADE LOG[/bold]  ·  Buys + Exits", border_style="bright_green", box=box.ROUNDED)


def render_footer() -> Panel:
    grid = Table.grid(expand=True)
    grid.add_column(justify="left", ratio=2)
    grid.add_column(justify="center", ratio=3)
    grid.add_column(justify="right", ratio=2)

    all_flow = logger.get_recent_flow_alerts(limit=1)

    if all_flow:
        a = all_flow[0]
        is_poly = a.get("ticker", "").startswith("POLY:")
        if is_poly:
            headline_text = (
                f"[{WHALE}]>[/{WHALE}] [{MUTED}]POLY:[/{MUTED}] "
                f"{a.get('side', '')} ${a.get('size', 0):,.0f} "
                f"on {a.get('market_question', '')[:50]}"
            )
        else:
            headline_text = (
                f"[{POLY}]>[/{POLY}] [{MUTED}]KALSHI:[/{MUTED}] "
                f"{a.get('side', '')} {a.get('size', 0):,.0f} contracts "
                f"({a.get('multiple', 0):.1f}x) on {a.get('market_question', '')[:40]}"
            )
    else:
        headline_text = f"[{DIM}]Waiting for whale activity...[/{DIM}]"

    stats = logger.get_trade_stats()
    all_alerts = logger.get_recent_flow_alerts(limit=200)
    poly_count = len([a for a in all_alerts if a.get("ticker", "").startswith("POLY:")])
    mode = "LIVE" if _is_live_mode() else "DRY"

    grid.add_row(
        headline_text,
        f"[{DIM}]Ctrl+C to exit  ·  trades.db[/{DIM}]",
        f"[{DIM}]{mode}[/{DIM}]  |  Trades: [{ACCENT}]{stats['total_trades']}[/{ACCENT}]  Whales: [{WHALE}]{poly_count}[/{WHALE}] ",
    )
    return Panel(grid, style="bright_green", box=box.HEAVY)


def run_dashboard(scan_interval: float = 2.0):
    """Launch the live dashboard. Reads from SQLite on each refresh."""
    layout = make_layout()

    try:
        with Live(layout, console=console, refresh_per_second=1, screen=True) as live:
            while True:
                layout["header"].update(render_header())
                layout["status"].update(render_status())
                layout["performance"].update(render_performance())
                layout["poly_whales"].update(render_poly_whales())
                layout["kalshi_flow"].update(render_kalshi_flow())
                layout["trades"].update(render_trades())
                layout["footer"].update(render_footer())
                time.sleep(scan_interval)

    except KeyboardInterrupt:
        stats = logger.get_trade_stats()
        console.print(f"\n[{ACCENT}]Dashboard closed. {stats['total_trades']} trades in database.[/{ACCENT}]")


if __name__ == "__main__":
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else 2.0
    run_dashboard(scan_interval=interval)
