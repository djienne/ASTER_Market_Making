#!/usr/bin/env python3
"""Unified terminal dashboard for balances, positions, orders, and mark prices."""

import argparse
import asyncio
import json
import logging
import os
import sys
import signal
import time
from contextlib import suppress
from datetime import datetime
from typing import Dict, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosedOK

import utils
from api_client import ApiClient

STABLE_ASSETS = ("USDT", "USDC", "USDF")
MAX_ORDER_EVENTS = 4
REST_REFRESH_INTERVAL = 15
MARK_STREAM_RETRY = 3
USER_STREAM_RETRY = 5
LISTEN_KEY_KEEPALIVE_INTERVAL = 30 * 60  # listen keys expire after 60 min without a keepalive
REALIZED_PNL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "realized_pnl.json")
REALIZED_PNL_HISTORY_LIMIT = 2

# Network/parse errors are logged as warnings; anything else gets a traceback.
EXPECTED_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, websockets.WebSocketException)

# The REST account snapshot (/fapi/v3/account) and the ACCOUNT_UPDATE event
# carry the same balance/position fields under different keys.
REST_ACCOUNT_KEYS = {
    "asset": "asset", "wallet": "walletBalance",
    "symbol": "symbol", "amount": "positionAmt", "entry": "entryPrice",
    "unrealized": ("unRealizedProfit", "unrealizedProfit", "unrealizedPnl"),
}
WS_ACCOUNT_KEYS = {
    "asset": "a", "wallet": "wb",
    "symbol": "s", "amount": "pa", "entry": "ep",
    "unrealized": ("up", "unRealizedProfit", "unrealizedProfit", "unrealizedPnl"),
}

logger = logging.getLogger("TerminalDashboard")

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[91m"
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"

USE_COLOR = os.getenv("NO_COLOR") is None

def enable_ansi_windows():
    """Enables ANSI escape sequences in the Windows terminal."""
    if os.name == 'nt':
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            # Set console mode to include ENABLE_VIRTUAL_TERMINAL_PROCESSING
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
        except (ctypes.ArgumentError, OSError, AttributeError):
            pass

def colorize(text: str, color: str) -> str:
    return f"{color}{text}{RESET}" if USE_COLOR else text


def to_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def format_event_time(event_time_ms) -> str:
    if not event_time_ms:
        return "--"
    return datetime.fromtimestamp(event_time_ms / 1000).strftime("%H:%M:%S.%f")[:-3]


class TerminalDashboard:
    """Maintains shared state for the combined account/order/price dashboard."""

    def __init__(
        self,
        stop_event: asyncio.Event,
        refresh_interval: int = REST_REFRESH_INTERVAL,
    ) -> None:
        self.stop_event = stop_event
        self.refresh_interval = refresh_interval

        self.balances: Dict[str, float] = {}  # asset -> wallet balance
        self.positions: Dict[str, Dict[str, float]] = {}
        self.order_symbols: Dict[str, float] = {}  # symbol -> monotonic time of its latest order event
        self.order_events = []
        self.mark_prices: Dict[str, Dict[str, float]] = {}
        self.realized_pnl_total = 0.0
        self.realized_pnl_history: list[Dict[str, object]] = []
        self._last_persisted_pnl = 0.0

        self.account_update_count = 0
        self.order_update_count = 0
        self.trade_count = 0
        self.last_reason = "INIT"
        self.last_event_time = "--"
        self.margin_alerts = []

        self.start_time = datetime.now()
        self.latest_snapshot_time: Optional[datetime] = None

        self.mark_symbols = set()
        self.mark_stream_event = asyncio.Event()
        self.mark_stream_event.set()
        self._first_render = True  # only the first frame switches to the alternate screen and clears it
        self._last_book_render = 0.0
        self._book_render_interval = 0.3

        self._load_realized_pnl()

    @staticmethod
    def _summarize_exception(exc: Exception) -> str:
        if isinstance(exc, aiohttp.ClientResponseError):
            status = exc.status
            message = exc.message or exc.__class__.__name__
            return f"HTTP {status} {message}" if status else message
        if isinstance(exc, aiohttp.ClientConnectionError):
            return "Connection error"
        if isinstance(exc, asyncio.TimeoutError):
            return "Timeout"
        return exc.__class__.__name__

    def _report_error(self, label: str, exc: Exception) -> None:
        """Log ``exc`` and show it as the last reason, e.g. label "Mark stream"."""
        if isinstance(exc, EXPECTED_ERRORS):
            logger.warning("%s error: %s", label, exc)
        else:
            logger.error("Unexpected %s error: %s", label.lower(), exc, exc_info=exc)
        self.last_reason = f"{label} error ({self._summarize_exception(exc)})"
        self.render(f"{label.upper()} ERROR")

    # ------------------------------------------------------------------
    # Snapshot helpers
    # ------------------------------------------------------------------
    def _refresh_mark_symbols(self) -> None:
        """Stream mark prices for every symbol with a position or order activity."""
        symbols = {symbol for symbol in self.positions if symbol} | set(self.order_symbols)
        if symbols != self.mark_symbols:
            self.mark_symbols = symbols
            self.mark_stream_event.set()

    def _load_realized_pnl(self) -> None:
        if not os.path.isfile(REALIZED_PNL_FILE):
            self._last_persisted_pnl = self.realized_pnl_total
            return
        try:
            with open(REALIZED_PNL_FILE, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            total_value = payload.get("total", 0.0)
            try:
                parsed_total = float(total_value)
            except (TypeError, ValueError):
                parsed_total = 0.0
            if abs(parsed_total) > 1e-9:
                self.realized_pnl_total = parsed_total
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning(f"Failed to load realized PnL file: {exc}")
        finally:
            self._last_persisted_pnl = self.realized_pnl_total

    def _persist_realized_pnl(self) -> None:
        if abs(self.realized_pnl_total - self._last_persisted_pnl) < 1e-9:
            return
        payload = {"total": round(self.realized_pnl_total, 10)}
        try:
            with open(REALIZED_PNL_FILE, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            self._last_persisted_pnl = self.realized_pnl_total
        except OSError as exc:
            logger.warning(f"Failed to persist realized PnL: {exc}")

    def _record_realized_pnl(self, entry: Dict[str, object]) -> None:
        if entry.get("exec") != "TRADE":
            return
        pnl_delta = to_float(entry.get("realized"))
        if abs(pnl_delta) < 1e-9:
            return
        self.realized_pnl_total += pnl_delta
        record = {
            "time": entry.get("time", "--"),
            "symbol": entry.get("symbol", "N/A"),
            "side": entry.get("side", "N/A"),
            "pnl": pnl_delta,
        }
        self.realized_pnl_history.insert(0, record)
        del self.realized_pnl_history[REALIZED_PNL_HISTORY_LIMIT:]
        self._persist_realized_pnl()

    def _recalc_unrealized(self, symbol: str) -> None:
        symbol = symbol.upper()
        pos = self.positions.get(symbol)
        if not pos:
            return
        mark_info = self.mark_prices.get(symbol)
        mark_price = mark_info.get("mark") if mark_info else None
        if mark_price is None:
            return
        entry_price = pos.get("entry")
        if entry_price is None:
            return
        amount = pos.get("amount", 0.0)
        pos["unrealized"] = (mark_price - entry_price) * amount

    # ------------------------------------------------------------------
    # Data ingestion
    # ------------------------------------------------------------------
    def _apply_account_rows(self, balances, positions, keys: Dict[str, object], pop_flat: bool) -> None:
        """Apply balance/position rows keyed per REST_ACCOUNT_KEYS or WS_ACCOUNT_KEYS.

        A flat WS row closes the symbol (pop_flat). The REST snapshot rebuilds
        positions from scratch, so there a flat row (e.g. the idle hedge-mode
        side of an open position) is just skipped.
        """
        for balance in balances:
            asset = balance.get(keys["asset"])
            if asset:
                self.balances[asset] = to_float(balance.get(keys["wallet"]))

        for position in positions:
            symbol = position.get(keys["symbol"], "N/A").upper()
            amount = to_float(position.get(keys["amount"]))
            if amount == 0:
                if pop_flat:
                    self.positions.pop(symbol, None)
                continue
            raw_unrealized = next((position[key] for key in keys["unrealized"] if position.get(key)), None)
            self.positions[symbol] = {
                "amount": amount,
                "entry": to_float(position.get(keys["entry"])),
                "unrealized": to_float(raw_unrealized),
            }
            self._recalc_unrealized(symbol)

    def update_from_snapshot(self, data: Dict[str, object]) -> None:
        self.positions.clear()
        self._apply_account_rows(data.get("assets", []), data.get("positions", []), REST_ACCOUNT_KEYS, pop_flat=False)
        self.latest_snapshot_time = datetime.now()
        self.margin_alerts.clear()
        self.last_reason = "REST SNAPSHOT"
        self._refresh_mark_symbols()

    def handle_account_update(self, payload: Dict[str, object], event_time: int = 0) -> None:
        self.last_reason = payload.get("m", "ACCOUNT_UPDATE")
        self.account_update_count += 1
        self.last_event_time = format_event_time(event_time)
        self._apply_account_rows(payload.get("B", []), payload.get("P", []), WS_ACCOUNT_KEYS, pop_flat=True)
        self.latest_snapshot_time = datetime.now()
        self._refresh_mark_symbols()

    def handle_order_update(self, order: Dict[str, object]) -> None:
        timestamp = format_event_time(order.get("T") or order.get("O"))
        symbol = order.get("s", "N/A").upper()
        entry = {
            "time": timestamp,
            "symbol": symbol,
            "side": order.get("S", "N/A"),
            "status": order.get("X", "N/A"),
            "exec": order.get("x", "N/A"),
            "qty": to_float(order.get("q")),
            "filled": to_float(order.get("z")),
            "price": to_float(order.get("p")),
            "avg": to_float(order.get("ap")),
            "realized": to_float(order.get("rp")),
            "order_id": order.get("i") or order.get("orderId"),
            "client_id": order.get("C") or order.get("c") or order.get("clientOrderId"),
        }
        self.order_events.insert(0, entry)
        del self.order_events[MAX_ORDER_EVENTS:]
        self._record_realized_pnl(entry)
        if symbol:
            self.order_symbols[symbol] = time.monotonic()

        self.order_update_count += 1
        if order.get("x") == "TRADE":
            self.trade_count += 1
        self.last_event_time = timestamp
        self._refresh_mark_symbols()

    def handle_margin_call(self, payload: Dict[str, object], event_time: int = 0) -> None:
        self.last_event_time = format_event_time(event_time)
        alerts = []
        for pos in payload.get("p", []):
            symbol = pos.get("s", "N/A")
            side = pos.get("ps", "N/A")
            amount = pos.get("pa", "0")
            pnl = pos.get("up", "0")
            alerts.append(f"{symbol} {side} {amount} (PnL {pnl})")
        self.margin_alerts = alerts or ["Margin call event received"]
        self.last_reason = "MARGIN_CALL"

    # ------------------------------------------------------------------
    # Rendering helpers
    # ------------------------------------------------------------------
    def render(self, status: str = "WAITING") -> None:
        now = datetime.now()
        uptime = now - self.start_time
        stable_total = 0.0
        stable_lines = []
        for asset in STABLE_ASSETS:
            bal = self.balances.get(asset, 0.0)
            stable_total += bal
            stable_lines.append(f"  {asset}: {bal:,.4f} {asset}")
        other_balances = []
        for asset, amount in sorted(self.balances.items()):
            if asset in STABLE_ASSETS:
                continue
            if abs(amount) < 0.01:
                continue
            other_balances.append(f"  {asset}: {amount:,.4f} {asset}")
        total_unrealized = sum(pos.get("unrealized", 0.0) for pos in self.positions.values())
        total_equity = stable_total + total_unrealized
        snapshot = (
            self.latest_snapshot_time.strftime("%Y-%m-%d %H:%M:%S")
            if self.latest_snapshot_time
            else "--"
        )

        header = colorize("=== ASTER TERMINAL DASHBOARD ===", CYAN + BOLD)
        lines: list[str] = []

        lines.append(header)
        lines.append(
            f"Snapshot: {snapshot} | Rendered: {now.strftime('%Y-%m-%d %H:%M:%S')} | Status: "
            f"{colorize(status, YELLOW if status not in {'CONNECTED', 'IDLE'} else GREEN)}"
        )
        lines.append(
            f"Uptime: {int(uptime.total_seconds() // 60)}m {int(uptime.total_seconds() % 60)}s | "
            f"Last reason: {self.last_reason}"
        )
        lines.append("")

        lines.append(colorize("Account Summary", BOLD))
        lines.append(f"  Total Stablecoins: {stable_total:,.4f} USD")
        lines.append(f"  Total Unrealized PnL: {total_unrealized:,.4f} USD")
        lines.append(f"  Total Equity: {total_equity:,.4f} USD")
        lines.append("")

        lines.append(colorize("Stablecoin Breakdown:", BOLD))
        lines.extend(stable_lines)
        if other_balances:
            lines.append("")
            lines.append(colorize("Other Balances:", BOLD))
            lines.extend(other_balances)

        lines.append("")
        lines.append(colorize("Open Positions:", BOLD))
        if self.positions:
            header_row = (
                f"{'Symbol':<10}{'Side':<6}{'Amount':>12}{'Entry':>12}{'Mark':>12}{'Mid':>12}{'Quote':>14}{'Unreal PnL':>14}{'Funding%':>10}"
            )
            lines.append(header_row)
            for symbol, pos in sorted(self.positions.items()):
                amount = pos['amount']
                side = "LONG" if amount > 0 else "SHORT"
                mark_info = self.mark_prices.get(symbol)
                mark_display = "--"
                mid_display = '--'.rjust(12)
                funding_display = '--'.rjust(10)
                mark_val = None
                mid_val = None
                if mark_info:
                    mark_val = mark_info.get('mark')
                    if mark_val is not None:
                        mark_display = f"{mark_val:,.3f}"
                    mid_val = mark_info.get('mid')
                    if mid_val is not None:
                        mid_display = f"{mid_val:>12.3f}"
                    funding_val = mark_info.get('funding')
                    if funding_val is not None:
                        funding_display = colorize(f"{funding_val:.4f}%".rjust(10), GREEN if funding_val >= 0 else RED)
                pnl_value = pos.get('unrealized', 0.0)
                entry_price = pos.get('entry')
                entry_display = f"{entry_price:>12.3f}" if entry_price is not None else '--'.rjust(12)
                quote_ref = mid_val if mid_val is not None else mark_val if mark_val is not None else entry_price
                quote_text = '--'.rjust(14)
                if quote_ref is not None:
                    quote_value = amount * quote_ref
                    quote_text = colorize(f"{quote_value:>14.3f}", GREEN if quote_value >= 0 else RED)
                amount_text = colorize(f"{amount:>12.4f}", GREEN if amount >= 0 else RED)
                side_cell = colorize(f"{side:<6}", GREEN if amount > 0 else RED)
                pnl_text = colorize(f"{pnl_value:>14.2f}", GREEN if pnl_value >= 0 else RED)
                mark_cell = f"{mark_display:>12}"
                lines.append(
                    f"{symbol:<10}{side_cell}{amount_text}{entry_display}"
                    f"{mark_cell}{mid_display}{quote_text}{pnl_text} {funding_display}"
                )
        else:
            lines.append(colorize('  None', DIM))

        lines.append("")
        lines.append(colorize("Active Order Mid Prices:", BOLD))
        if self.order_symbols:
            # most recent order activity first
            for symbol, _ in sorted(self.order_symbols.items(), key=lambda item: item[1], reverse=True):
                mark_info = self.mark_prices.get(symbol, {})
                mid_value = None
                if isinstance(mark_info, dict):
                    mid_value = mark_info.get("mid")
                    if mid_value in (None, 0):
                        mid_value = mark_info.get("mark")
                mid_display = "--"
                if mid_value not in (None, 0):
                    mid_display = f"{mid_value:.3f}"
                lines.append(f"  {symbol:<10} mid {mid_display:>10}")
        else:
            lines.append(colorize("  None", DIM))

        lines.append("")
        lines.append(colorize("Realized Trade PnL:", BOLD))
        total_line = f"  Total: {self.realized_pnl_total:+.4f} USD"
        lines.append(colorize(total_line, GREEN if self.realized_pnl_total >= 0 else RED))
        if self.realized_pnl_history:
            for record in self.realized_pnl_history:
                pnl_value = record.get("pnl", 0.0)
                line = f"  {record.get('time', '--'):<8} {record.get('symbol', 'N/A'):<10} {pnl_value:+.4f} USD"
                lines.append(colorize(line, GREEN if pnl_value >= 0 else RED))
        else:
            lines.append(colorize("  No realized trades yet.", DIM))

        lines.append("")
        lines.append(colorize("Recent Orders:", BOLD))
        display_events = list(self.order_events[:MAX_ORDER_EVENTS])
        while len(display_events) < MAX_ORDER_EVENTS:
            display_events.append(None)
        for entry in display_events:
            if entry:
                qty = entry["qty"]
                filled = entry["filled"]
                progress = f"{filled:.4f}/{qty:.4f}" if qty else f"{filled:.4f}"
                avg_price = f"{entry['avg']:.3f}" if entry["avg"] else '0.000'
                realized = entry["realized"]
                if abs(realized) < 1e-9:
                    pnl_label = "0.00 USD"
                else:
                    pnl_label = colorize(f"{realized:+.4f} USD", GREEN if realized >= 0 else RED)
                time_str = entry['time']
                symbol = entry['symbol']
                side_str = entry['side']
                status_str = entry['status']
                exec_type = entry['exec']
                progress_str = progress
                avg_str = avg_price
                price_value = entry['price']
                price_str = f"{price_value:.3f}" if price_value else '0.000'
                pct_str = '--'
                mark_info = self.mark_prices.get(symbol)
                ref_price = None
                mark_str = '--'
                if isinstance(mark_info, dict):
                    mark_val = mark_info.get('mark')
                    if mark_val not in (None, 0):
                        ref_price = mark_val
                        mark_str = f"{mark_val:.3f}"
                if price_value and ref_price and ref_price != 0:
                    pct = (price_value - ref_price) / ref_price * 100
                    pct_str = colorize(f"{pct:+.2f}%", GREEN if pct <= 0 else RED)
                pnl_str = pnl_label
                order_id = entry.get("order_id")
                client_id = entry.get("client_id")
                order_label = str(order_id) if order_id not in (None, "") else "--"
                client_label = str(client_id) if client_id not in (None, "") else "--"
                if order_label != "--":
                    order_label = f"#{order_label}"
                lines.append(
                    f"  {time_str:<8} {symbol:<10} {order_label:<13} {side_str:<5} {status_str:<13} ({exec_type:<8}) "
                    f"qty {progress_str:<18} avg {avg_str:>7} limit {price_str:>8} mark {mark_str:>8} dev {pct_str:<9} pnl {pnl_str:<12} cid {client_label:<12}"
                )
            else:
                lines.append(colorize("  -- waiting for order activity --", DIM))

        lines.append("")
        lines.append(colorize("Alerts:", BOLD))
        if self.margin_alerts:
            for note in self.margin_alerts[-3:]:
                lines.append(colorize(f"  ! {note}", RED))
        else:
            lines.append(colorize("  None", DIM))

        lines.append("")
        lines.append(colorize("Stats:", BOLD))
        lines.append(
            f"  Account updates: {self.account_update_count} | Order updates: {self.order_update_count} | Trades: {self.trade_count}"
        )
        lines.append(f"  Last event time: {self.last_event_time}")
        lines.append("")
        lines.append(colorize("Press Ctrl+C to exit.", DIM))

        # Use an alternate screen buffer to completely prevent scrolling
        buffer = []

        # Hide cursor before any writes
        buffer.append("\033[?25l")

        if self._first_render:
            # Switch to alternate screen buffer and clear it
            buffer.append("\033[?1049h")  # Enable alternate screen
            buffer.append("\033[2J")      # Clear screen
            buffer.append("\033[H")       # Move to home
            self._first_render = False
        else:
            # Just move to home position
            buffer.append("\033[H")

        # Write the content line by line
        for line in lines:
            buffer.append(line)
            buffer.append("\033[K")  # Clear to end of line
            buffer.append("\n")

        # Clear from cursor to end of screen
        buffer.append("\033[J")

        # Show cursor after rendering
        buffer.append("\033[?25h")

        # Single atomic write to stdout
        sys.stdout.write("".join(buffer))
        sys.stdout.flush()

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------
    async def periodic_refresh(self, client: ApiClient) -> None:
        while not self.stop_event.is_set():
            try:
                snapshot = await client.signed_request("GET", "/fapi/v3/account", {})
                self.update_from_snapshot(snapshot)
                self.render("REST REFRESH")
            except Exception as exc:
                self._report_error("Refresh", exc)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.refresh_interval)
            except asyncio.TimeoutError:
                continue
        self.last_reason = "Refresh stopped"

    async def mark_price_listener(self) -> None:
        while not self.stop_event.is_set():
            await self.mark_stream_event.wait()
            self.mark_stream_event.clear()
            if self.stop_event.is_set():
                break
            symbols = sorted(self.mark_symbols)
            if not symbols:
                self.mark_prices.clear()
                continue
            stream_parts = []
            for symbol in symbols:
                slug = symbol.lower()
                stream_parts.append(f"{slug}@markPrice@1s")
                stream_parts.append(f"{slug}@bookTicker")
            url = f"wss://fstream.asterdex.com/stream?streams={'/'.join(stream_parts)}"
            try:
                async with websockets.connect(url) as ws:
                    self.last_reason = "MARK STREAM"
                    self.render("MARK STREAM")
                    while not self.stop_event.is_set():
                        recv_task = asyncio.create_task(ws.recv())
                        change_task = asyncio.create_task(self.mark_stream_event.wait())
                        stop_task = asyncio.create_task(self.stop_event.wait())
                        done, pending = await asyncio.wait(
                            {recv_task, change_task, stop_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for task in pending:
                            if not task.done():
                                task.cancel()
                            with suppress(asyncio.CancelledError):
                                await task
                        if stop_task in done:
                            with suppress(Exception):
                                stop_task.result()
                            break
                        if change_task in done:
                            with suppress(Exception):
                                change_task.result()
                            break
                        message = recv_task.result()
                        data = json.loads(message)
                        payload = data.get("data", data)
                        event_type = payload.get("e")
                        if not event_type:
                            continue
                        symbol = payload.get("s", "").upper()
                        if not symbol:
                            continue
                        info = self.mark_prices.setdefault(symbol, {})
                        if event_type == "markPriceUpdate":
                            info["mark"] = to_float(payload.get("p"))
                            info["funding"] = to_float(payload.get("r")) * 100
                            self._recalc_unrealized(symbol)
                            self.render("MARK PRICE")
                            continue
                        if event_type != "bookTicker":
                            continue
                        bid = to_float(payload.get("b"))
                        ask = to_float(payload.get("a"))
                        if bid > 0 and ask > 0:
                            info["mid"] = (bid + ask) / 2
                        elif ask > 0:
                            info["mid"] = ask
                        elif bid > 0:
                            info["mid"] = bid
                        else:
                            continue
                        now = time.monotonic()
                        if now - self._last_book_render >= self._book_render_interval:
                            self._last_book_render = now
                            self.render("BOOK TICKER")
            except ConnectionClosedOK:
                self.last_reason = "Mark stream closed"
                self.render("MARK STREAM CLOSED")
                self.mark_stream_event.set()  # reconnect after the retry wait, not on the next symbol change
                await asyncio.sleep(MARK_STREAM_RETRY)
            except Exception as exc:
                self._report_error("Mark stream", exc)
                self.mark_stream_event.set()  # reconnect after the retry wait, not on the next symbol change
                await asyncio.sleep(MARK_STREAM_RETRY)
        self.last_reason = "Mark stream stopped"

    async def stream(self, client: ApiClient) -> None:
        """User data stream; reconnects with a fresh listen key after any close,
        expiry or error (the server also drops every connection at 24h)."""
        while not self.stop_event.is_set():
            await self._stream_once(client)
            if not self.stop_event.is_set():
                await asyncio.sleep(USER_STREAM_RETRY)

    async def listen_key_keepalive(self, client: ApiClient) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=LISTEN_KEY_KEEPALIVE_INTERVAL)
            except asyncio.TimeoutError:
                try:
                    await client.keepalive_listen_key()
                except Exception as exc:
                    logger.warning("Listen key keepalive failed: %s", exc)

    async def _stream_once(self, client: ApiClient) -> None:
        try:
            response = await client.create_listen_key()
            listen_key = response.get("listenKey")
            if not listen_key:
                raise aiohttp.ClientError(f"no listenKey in response: {response}")
            async with websockets.connect(f"wss://fstream.asterdex.com/ws/{listen_key}") as ws:
                self.render("CONNECTED")
                while not self.stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=3)
                    except asyncio.TimeoutError:
                        self.render("IDLE")
                        continue
                    except ConnectionClosedOK:
                        self.last_reason = "User stream closed"
                        self.render("CONNECTION CLOSED")
                        break
                    data = json.loads(message)
                    event_type = data.get("e", "unknown")
                    if event_type == "ACCOUNT_UPDATE":
                        self.handle_account_update(data.get("a", {}), data.get("E", 0))
                        self.render("ACCOUNT UPDATE")
                    elif event_type == "ORDER_TRADE_UPDATE":
                        self.handle_order_update(data.get("o", {}))
                        self.render("ORDER EVENT")
                    elif event_type == "MARGIN_CALL":
                        self.handle_margin_call(data, data.get("E", 0))
                        self.render("MARGIN CALL")
                    elif event_type == "listenKeyExpired":
                        self.last_reason = "listenKeyExpired"
                        self.render("LISTEN KEY EXPIRED")
                        break
                    else:
                        self.last_reason = f"Unhandled {event_type}"
                        self.render("UNHANDLED EVENT")
        except ConnectionClosedOK:
            self.last_reason = "Stream closed"
            self.render("CONNECTION CLOSED")
        except Exception as exc:
            if not self.stop_event.is_set():
                self._report_error("Stream", exc)


async def run_dashboard(args: argparse.Namespace) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(signum, frame):  # noqa: ARG001
        if not stop_event.is_set():
            loop.call_soon_threadsafe(stop_event.set)

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", signal.SIGINT)):
        try:
            signal.signal(sig, _handle_signal)
        except (ValueError, OSError):
            pass

    utils.load_project_env()

    api_user = os.getenv("API_USER")
    api_signer = os.getenv("API_SIGNER")
    api_private_key = os.getenv("API_PRIVATE_KEY")

    if not all([api_user, api_signer, api_private_key]):
        print("ERROR: Missing required environment variables")
        print("Required: API_USER, API_SIGNER, API_PRIVATE_KEY")
        return

    dashboard = TerminalDashboard(stop_event, refresh_interval=args.refresh_interval)

    # One client for the dashboard's lifetime: the user stream needs it for
    # listen-key keepalives and reconnects, and the REST refresh reuses it.
    async with ApiClient(api_user, api_signer, api_private_key) as client:
        snapshot = await client.signed_request("GET", "/fapi/v3/account", {})
        dashboard.update_from_snapshot(snapshot)
        await _run_dashboard_tasks(dashboard, client, stop_event, args)


async def _run_dashboard_tasks(dashboard, client, stop_event, args) -> None:
    tasks = {
        asyncio.create_task(dashboard.periodic_refresh(client)),
        asyncio.create_task(dashboard.mark_price_listener()),
        asyncio.create_task(dashboard.stream(client)),
        asyncio.create_task(dashboard.listen_key_keepalive(client)),
    }
    if args.duration > 0:
        duration_task = asyncio.create_task(asyncio.sleep(args.duration))
        tasks.add(duration_task)
    else:
        duration_task = None

    signal_task = asyncio.create_task(stop_event.wait())
    tasks.add(signal_task)

    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    if duration_task and duration_task in done:
        dashboard.render("TIMEOUT")
        print(f"\nReached duration limit ({args.duration}s); exiting.")

    stop_event.set()
    dashboard.mark_stream_event.set()

    for task in tasks:
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live account/order dashboard")
    parser.add_argument(
        "--duration",
        type=int,
        default=360000,
        help="Seconds to run before auto exit (<=0 to run until interrupted)",
    )
    parser.add_argument(
        "--refresh-interval",
        type=int,
        default=REST_REFRESH_INTERVAL,
        help="Seconds between REST account refresh calls",
    )
    return parser.parse_args()


def main() -> None:
    enable_ansi_windows()
    args = parse_args()
    try:
        asyncio.run(run_dashboard(args))
    finally:
        # Restore normal screen buffer and show cursor
        sys.stdout.write("\033[?1049l")  # Disable alternate screen
        sys.stdout.write("\033[?25h")    # Show cursor
        sys.stdout.flush()


if __name__ == "__main__":
    main()
