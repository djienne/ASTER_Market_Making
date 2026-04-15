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
import shutil
from contextlib import suppress
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional
from urllib.parse import urlencode

import aiohttp
import hmac
import hashlib
import websockets
from websockets.exceptions import ConnectionClosedOK
from dotenv import load_dotenv

from api_client import ApiClient

STABLE_ASSETS = ("USDT", "USDC", "USDF")
MAX_ORDER_EVENTS = 4
REST_REFRESH_INTERVAL = 15
MARK_STREAM_RETRY = 3
SPOT_REFRESH_INTERVAL = 10
SPOT_BASE_ASSET = "USDT"
SPOT_USD_RATE = Decimal("1")
SPOT_MIN_TOTAL = Decimal("0")
SPOT_STABLE_QUOTES = ("USDT", "BUSD", "USDC", "USDD", "USD")
SPOT_DISPLAY_ROWS = 4
REALIZED_PNL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "realized_pnl.json")
REALIZED_PNL_HISTORY_LIMIT = 2

ACTIVE_ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED", "PENDING_NEW", "ACCEPTED", "PENDING_CANCEL", "WORKING"}

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

def get_terminal_size():
    """Get terminal size, with fallback defaults."""
    try:
        size = shutil.get_terminal_size(fallback=(120, 40))
        return size.columns, size.lines
    except Exception:
        return 120, 40

def colorize(text: str, color: str) -> str:
    return f"{color}{text}{RESET}" if USE_COLOR else text


def to_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _decimal(value: Optional[str]) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _format_decimal(value: Optional[Decimal], precision: int = 4) -> str:
    if value is None:
        return "--"
    try:
        normalized = float(value)
    except (ValueError, TypeError):
        return "--"
    return f"{normalized:,.{precision}f}"


class SpotBalanceFetcher:
    """Async helper to retrieve and value spot balances."""

    PRICE_CACHE_TTL = 5.0

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_asset: str = SPOT_BASE_ASSET,
        usd_rate: Decimal = SPOT_USD_RATE,
        min_total: Decimal = SPOT_MIN_TOTAL,
        show_zero: bool = False,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_asset = base_asset.upper()
        self.usd_rate = usd_rate
        self.min_total = min_total
        self.show_zero = show_zero

        self.session = session or aiohttp.ClientSession()
        self._owns_session = session is None
        self.price_cache: Dict[str, tuple[Decimal, float]] = {}

    async def aclose(self) -> None:
        if self._owns_session and not self.session.closed:
            await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
        return False

    def _sign(self, params: Dict[str, str]) -> Dict[str, str]:
        query_string = urlencode(params, doseq=True)
        signature = hmac.new(self.api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256)
        signed = dict(params)
        signed["signature"] = signature.hexdigest()
        return signed

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, str]] = None,
        signed: bool = False,
    ) -> Dict[str, object]:
        params = params.copy() if params else {}
        headers = {"X-MBX-APIKEY": self.api_key, "User-Agent": "AsterTerminal/SpotBalance"}
        if signed:
            params.setdefault("recvWindow", "5000")
            params["timestamp"] = str(int(time.time() * 1000))
            params = self._sign(params)
        url = f"https://sapi.asterdex.com{path}"
        async with self.session.request(
            method,
            url,
            params=params if method.upper() == "GET" else None,
            data=params if method.upper() != "GET" else None,
            headers=headers,
        ) as response:
            response.raise_for_status()
            if response.content_type == "application/json":
                return await response.json()
            text = await response.text()
            raise ValueError(f"Unexpected response type: {response.content_type} -> {text}")

    async def get_account_balances(self) -> list[Dict[str, str]]:
        payload = await self._request("GET", "/api/v1/account", signed=True)
        balances = payload.get("balances", [])
        if not isinstance(balances, list):
            return []
        return balances

    async def get_price(self, symbol: str) -> Optional[Decimal]:
        symbol = symbol.upper()
        cached = self.price_cache.get(symbol)
        now = time.monotonic()
        if cached and now - cached[1] <= self.PRICE_CACHE_TTL:
            return cached[0]
        data = await self._request("GET", "/api/v1/ticker/price", params={"symbol": symbol})
        price = data.get("price") if isinstance(data, dict) else None
        if price is None:
            return None
        value = _decimal(price)
        self.price_cache[symbol] = (value, now)
        return value

    async def _pair_price(self, base: str, quote: str) -> Optional[Decimal]:
        if base == quote:
            return Decimal("1")
        direct_symbol = f"{base}{quote}"
        price = await self.get_price(direct_symbol)
        if price is not None:
            return price
        inverse_symbol = f"{quote}{base}"
        inverse_price = await self.get_price(inverse_symbol)
        if inverse_price is None or inverse_price <= 0:
            return None
        return Decimal("1") / inverse_price

    async def find_conversion_rate(self, asset: str, base_asset: str) -> Optional[Decimal]:
        asset = asset.upper()
        base_asset = base_asset.upper()
        if asset == base_asset:
            return Decimal("1")

        direct_price = await self._pair_price(asset, base_asset)
        if direct_price is not None:
            return direct_price

        bridge_assets = [base_asset] + [q for q in SPOT_STABLE_QUOTES if q not in {asset, base_asset}]
        for middle in bridge_assets:
            if middle == asset:
                continue
            first_leg = await self._pair_price(asset, middle)
            if first_leg is None:
                continue
            if middle == base_asset:
                return first_leg
            second_leg = await self._pair_price(middle, base_asset)
            if second_leg is None:
                continue
            return first_leg * second_leg
        return None

    async def fetch_snapshot(self) -> Dict[str, object]:
        balances = await self.get_account_balances()
        assets: list[Dict[str, object]] = []
        total_base = Decimal("0")
        total_usd = Decimal("0")

        for balance in balances:
            asset = balance.get("asset", "").upper()
            if not asset:
                continue
            free_amt = _decimal(balance.get("free"))
            locked_amt = _decimal(balance.get("locked"))
            total_amt = free_amt + locked_amt

            if not self.show_zero and total_amt <= self.min_total:
                continue

            entry: Dict[str, object] = {
                "asset": asset,
                "free": free_amt,
                "locked": locked_amt,
                "total": total_amt,
                "price": None,
                "base_value": None,
                "usd_value": None,
            }

            if total_amt > 0:
                conversion = await self.find_conversion_rate(asset, self.base_asset)
                if conversion is not None:
                    base_value = total_amt * conversion
                    usd_value = base_value * self.usd_rate
                    entry.update({
                        "price": conversion,
                        "base_value": base_value,
                        "usd_value": usd_value,
                    })
                    total_base += base_value
                    total_usd += usd_value

            assets.append(entry)

        assets.sort(key=lambda item: item["asset"])  # type: ignore[index]
        return {
            "base_asset": self.base_asset,
            "usd_rate": self.usd_rate,
            "total_base": total_base,
            "total_usd": total_usd,
            "assets": assets,
        }

class TerminalDashboard:
    """Maintains shared state for the combined account/order/price dashboard."""

    def __init__(
        self,
        credentials: Dict[str, Optional[str]],
        stop_event: asyncio.Event,
        refresh_interval: int = REST_REFRESH_INTERVAL,
        spot_fetcher: Optional[SpotBalanceFetcher] = None,
    ) -> None:
        self.credentials = credentials
        self.stop_event = stop_event
        self.refresh_interval = refresh_interval
        self.spot_fetcher = spot_fetcher

        self.balances: Dict[str, Dict[str, str]] = {
            asset: {"wallet_balance": "0", "cross_wallet_balance": "0", "last_change": "0"}
            for asset in STABLE_ASSETS
        }
        self.positions: Dict[str, Dict[str, float]] = {}
        self.active_orders: Dict[str, Dict[str, object]] = {}
        self.order_mid_snapshots: Dict[str, Dict[str, object]] = {}
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
        self._first_render = True
        self._last_book_render = 0.0
        self._book_render_interval = 0.3
        self.spot_snapshot: Dict[str, object] = {
            "base_asset": SPOT_BASE_ASSET,
            "usd_rate": SPOT_USD_RATE,
            "total_base": Decimal("0"),
            "total_usd": Decimal("0"),
            "assets": [],
        }
        self.spot_last_update: Optional[datetime] = None

        self._load_realized_pnl()

    def _track_symbol(self, symbol: str) -> None:
        symbol = symbol.upper()
        if symbol and symbol not in self.mark_symbols:
            self.mark_symbols.add(symbol)
            self.mark_stream_event.set()

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

    # ------------------------------------------------------------------
    # Snapshot helpers
    # ------------------------------------------------------------------
    def ensure_stable(self) -> None:
        for asset in STABLE_ASSETS:
            self.balances.setdefault(
                asset,
                {"wallet_balance": "0", "cross_wallet_balance": "0", "last_change": "0"},
            )

    def update_spot_snapshot(self, snapshot: Dict[str, object]) -> None:
        self.spot_snapshot = snapshot
        self.spot_last_update = datetime.now()
        self.last_reason = "SPOT SNAPSHOT"


    def _refresh_mark_symbols(self) -> None:
        position_symbols = {
            symbol for symbol, pos in self.positions.items()
            if isinstance(pos, dict) and pos.get("amount")
        }
        active_symbols = {
            info.get("symbol", "").upper()
            for info in self.active_orders.values()
            if isinstance(info, dict) and info.get("symbol")
        }
        snapshot_symbols = {sym.upper() for sym in self.order_mid_snapshots.keys() if sym}
        symbols = (
            {sym for sym in position_symbols if sym}
            | {sym for sym in active_symbols if sym}
            | snapshot_symbols
        )
        if symbols != self.mark_symbols:
            self.mark_symbols = symbols
            self.mark_stream_event.set()
        self._first_render = True

    @staticmethod
    def _order_keys(order: Dict[str, object]) -> list[str]:
        keys: list[str] = []
        order_id = order.get("i") or order.get("orderId")
        client_id = order.get("C") or order.get("c") or order.get("clientOrderId")
        if order_id not in (None, ""):
            keys.append(f"id:{order_id}")
        if client_id not in (None, ""):
            keys.append(f"client:{client_id}")
        return keys

    def _record_order_mid_snapshot(self, data: Dict[str, object]) -> None:
        symbol = (data.get("symbol") or "").upper()
        if not symbol:
            return
        recorded_at = time.monotonic()
        snapshot = self.order_mid_snapshots.get(symbol, {}).copy()
        snapshot.update(
            {
                "symbol": symbol,
                "time": data.get("time") or snapshot.get("time") or datetime.now().strftime("%H:%M:%S"),
                "status": data.get("status") or snapshot.get("status"),
                "recorded_at": recorded_at,
            }
        )
        self.order_mid_snapshots[symbol] = snapshot
        self._track_symbol(symbol)

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
            logging.getLogger("TerminalDashboard").warning(f"Failed to load realized PnL file: {exc}")
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
            logging.getLogger("TerminalDashboard").warning(f"Failed to persist realized PnL: {exc}")

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

    def _update_active_orders(self, order: Dict[str, object], data: Dict[str, object]) -> None:
        status = data.get("status")
        self._record_order_mid_snapshot(data)
        keys = set(self._order_keys(order))
        if not keys:
            return

        if status in ACTIVE_ORDER_STATUSES:
            existing = None
            for key in keys:
                existing = self.active_orders.get(key)
                if existing:
                    break
            if existing is None:
                existing = {"keys": set()}
            existing_keys = set(existing.get("keys", set()))
            merged_keys = existing_keys | keys
            existing.update(data)
            existing["keys"] = merged_keys
            for key in merged_keys:
                self.active_orders[key] = existing
            return

        # Order no longer active; remove any aliases for it.
        existing = None
        for key in keys:
            existing = self.active_orders.get(key)
            if existing:
                break
        if not existing:
            return
        aliases = set(existing.get("keys", set())) or keys
        for key in aliases:
            self.active_orders.pop(key, None)

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
    def update_from_snapshot(self, data: Dict[str, object]) -> None:
        for balance in data.get("assets", []):
            asset = balance.get("asset")
            if not asset:
                continue
            self.balances[asset] = {
                "wallet_balance": balance.get("walletBalance", "0"),
                "cross_wallet_balance": balance.get("crossWalletBalance", "0"),
                "last_change": balance.get("lastChangeBalance", "0"),
            }

        self.positions.clear()
        for position in data.get("positions", []):
            amount = to_float(position.get("positionAmt"))
            if amount == 0:
                continue
            symbol = position.get("symbol", "N/A").upper()
            raw_unrealized = (
                position.get("unRealizedProfit")
                or position.get("unrealizedProfit")
                or position.get("unrealizedPnl")
            )
            self.positions[symbol] = {
                "amount": amount,
                "entry": to_float(position.get("entryPrice")),
                "unrealized": to_float(raw_unrealized),
                "side": position.get("positionSide", "BOTH"),
            }
            self._recalc_unrealized(symbol)

        self.ensure_stable()
        self.latest_snapshot_time = datetime.now()
        self.margin_alerts.clear()
        self.last_reason = "REST SNAPSHOT"
        self._refresh_mark_symbols()

    def handle_account_update(self, payload: Dict[str, object], event_time: int = 0) -> None:
        self.last_reason = payload.get("m", "ACCOUNT_UPDATE")
        self.account_update_count += 1
        timestamp = (
            datetime.fromtimestamp(event_time / 1000).strftime("%H:%M:%S.%f")[:-3] if event_time else "--"
        )
        self.last_event_time = timestamp

        for balance in payload.get("B", []):
            asset = balance.get("a")
            if not asset:
                continue
            self.balances[asset] = {
                "wallet_balance": balance.get("wb", "0"),
                "cross_wallet_balance": balance.get("cw", "0"),
                "last_change": balance.get("bc", "0"),
            }

        for position in payload.get("P", []):
            symbol = position.get("s", "N/A").upper()
            amount = to_float(position.get("pa"))
            if amount == 0:
                self.positions.pop(symbol, None)
                continue
            raw_unrealized = (
                position.get("up")
                or position.get("unRealizedProfit")
                or position.get("unrealizedProfit")
                or position.get("unrealizedPnl")
            )
            self.positions[symbol] = {
                "amount": amount,
                "entry": to_float(position.get("ep")),
                "unrealized": to_float(raw_unrealized),
                "side": position.get("ps", "BOTH"),
            }
            self._recalc_unrealized(symbol)

        self.ensure_stable()
        self.latest_snapshot_time = datetime.now()
        self._refresh_mark_symbols()

    def handle_order_update(self, order: Dict[str, object]) -> None:
        event_time = order.get("T") or order.get("O") or 0
        timestamp = (
            datetime.fromtimestamp(event_time / 1000).strftime("%H:%M:%S.%f")[:-3] if event_time else "--"
        )
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
            "last_fill_qty": to_float(order.get("l")),
            "last_fill_price": to_float(order.get("L")),
            "realized": to_float(order.get("rp")),
            "order_id": order.get("i") or order.get("orderId"),
            "client_id": order.get("C") or order.get("c") or order.get("clientOrderId"),
        }
        self._track_symbol(symbol)
        self.order_events.insert(0, entry)
        del self.order_events[MAX_ORDER_EVENTS:]
        self._record_realized_pnl(entry)

        active_payload = {
            "symbol": symbol,
            "side": entry["side"],
            "status": entry["status"],
            "qty": entry["qty"],
            "filled": entry["filled"],
            "price": entry["price"],
            "avg": entry["avg"],
            "type": order.get("o", "N/A"),
            "time": timestamp,
            "client_order_id": order.get("C") or order.get("c") or order.get("clientOrderId"),
            "order_id": order.get("i") or order.get("orderId"),
        }
        self._update_active_orders(order, active_payload)

        self.order_update_count += 1
        if order.get("x") == "TRADE":
            self.trade_count += 1
        self.last_event_time = timestamp
        self._refresh_mark_symbols()

    def handle_margin_call(self, payload: Dict[str, object], event_time: int = 0) -> None:
        timestamp = (
            datetime.fromtimestamp(event_time / 1000).strftime("%H:%M:%S.%f")[:-3] if event_time else "--"
        )
        self.last_event_time = timestamp
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
            bal = to_float(self.balances.get(asset, {}).get("wallet_balance"))
            stable_total += bal
            stable_lines.append(f"  {asset}: {bal:,.4f} {asset}")
        other_balances = []
        for asset, info in sorted(self.balances.items()):
            if asset in STABLE_ASSETS:
                continue
            amount = to_float(info.get("wallet_balance"))
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

        header = colorize("=== ASTER TERMINAL DASHBOARD ===", CYAN + BOLD if USE_COLOR else CYAN)
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
        lines.append(f"  Total Stablecoins: {stable_total:,.4f} USD")
        if other_balances:
            lines.append("")
            lines.append(colorize("Other Balances:", BOLD))
            lines.extend(other_balances)

        lines.append("")
        lines.append(colorize("Spot Wallet:", BOLD))
        base_asset = str(self.spot_snapshot.get("base_asset", SPOT_BASE_ASSET)).upper()
        total_base = self.spot_snapshot.get("total_base")
        total_usd = self.spot_snapshot.get("total_usd")
        usd_rate = self.spot_snapshot.get("usd_rate", Decimal("1"))
        lines.append(f"  Total {base_asset}: {_format_decimal(total_base, 4)} {base_asset}")
        lines.append(f"  Total USD: {_format_decimal(total_usd, 4)} USD (rate {_format_decimal(usd_rate, 4)})")
        updated_text = self.spot_last_update.strftime('%H:%M:%S') if self.spot_last_update else '--'
        lines.append(f"  Updated: {updated_text}")
        lines.append(colorize("  Assets:", BOLD))

        spot_assets = []
        if self.spot_snapshot:
            raw_assets = self.spot_snapshot.get("assets", [])
            if isinstance(raw_assets, list):
                spot_assets = list(raw_assets[:SPOT_DISPLAY_ROWS])

        padded_assets = spot_assets + [None] * (SPOT_DISPLAY_ROWS - len(spot_assets))

        for entry in padded_assets:
            if entry:
                asset = entry.get("asset", "-")
                free_amt = entry.get("free")
                locked_amt = entry.get("locked")
                total_amt = entry.get("total")
                base_value = entry.get("base_value")
                usd_value = entry.get("usd_value")
                price = entry.get("price")
                price_label = _format_decimal(price, 3)
                base_label = _format_decimal(base_value, 4)
                usd_label = _format_decimal(usd_value, 2)
                line = (
                    f"  {asset:<6} free {_format_decimal(free_amt, 6):>12} | locked {_format_decimal(locked_amt, 6):>12} |"
                    f" total {_format_decimal(total_amt, 6):>12} | px {price_label:>13} |"
                    f" {base_label:>12} {base_asset:<4} | {usd_label:>12} USD"
                )
            else:
                line = (
                    f"  {'--':<6} free {'--':>12} | locked {'--':>12} |"
                    f" total {'--':>12} | px {'--':>13} | {'--':>12} {base_asset:<4} | {'--':>12} USD"
                )
            lines.append(line)

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
                        funding_plain = f"{funding_val:.4f}%".rjust(10)
                        if USE_COLOR:
                            funding_color = GREEN if funding_val >= 0 else RED
                            funding_display = colorize(funding_plain, funding_color)
                        else:
                            funding_display = funding_plain
                amount_plain = f"{amount:>12.4f}"
                pnl_value = pos.get('unrealized', 0.0)
                pnl_plain = f"{pnl_value:>14.2f}"
                entry_price = pos.get('entry')
                entry_display = f"{entry_price:>12.3f}" if entry_price is not None else '--'.rjust(12)
                quote_ref = mid_val if mid_val is not None else mark_val if mark_val is not None else entry_price
                quote_value = None
                quote_plain = '--'.rjust(14)
                if quote_ref is not None:
                    quote_value = amount * quote_ref
                    quote_plain = f"{quote_value:>14.3f}"
                if USE_COLOR:
                    amount_color = GREEN if amount >= 0 else RED
                    amount_text = colorize(amount_plain, amount_color)
                    side_color = GREEN if amount > 0 else RED
                    side_cell = colorize(f"{side:<6}", side_color)
                    quote_text = colorize(quote_plain, GREEN if quote_value is not None and quote_value >= 0 else RED) if quote_value is not None else quote_plain
                    pnl_color = GREEN if pnl_value >= 0 else RED
                    pnl_text = colorize(pnl_plain, pnl_color)
                else:
                    amount_text = amount_plain
                    side_cell = f"{side:<6}"
                    quote_text = quote_plain
                    pnl_text = pnl_plain
                mark_cell = f"{mark_display:>12}"
                lines.append(
                    f"{symbol:<10}{side_cell}{amount_text}{entry_display}"
                    f"{mark_cell}{mid_display}{quote_text}{pnl_text} {funding_display}"
                )
        else:
            lines.append(colorize('  None', DIM))

        lines.append("")
        lines.append(colorize("Active Order Mid Prices:", BOLD))
        mid_entries = list(self.order_mid_snapshots.values())
        if mid_entries:
            mid_entries.sort(key=lambda item: item.get("recorded_at", 0), reverse=True)
            for snapshot in mid_entries:
                symbol = snapshot.get("symbol", "N/A")
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
        if USE_COLOR:
            total_color = GREEN if self.realized_pnl_total >= 0 else RED
            total_line = colorize(total_line, total_color)
        lines.append(total_line)
        if self.realized_pnl_history:
            for record in self.realized_pnl_history:
                pnl_value = record.get("pnl", 0.0)
                line = f"  {record.get('time', '--'):<8} {record.get('symbol', 'N/A'):<10} {pnl_value:+.4f} USD"
                if USE_COLOR:
                    pnl_color = GREEN if pnl_value >= 0 else RED
                    line = colorize(line, pnl_color)
                lines.append(line)
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
                    pnl_label = f"{realized:+.4f} USD"
                    if USE_COLOR:
                        pnl_color = GREEN if realized >= 0 else RED
                        pnl_label = colorize(pnl_label, pnl_color)
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
                    pct_str = f"{pct:+.2f}%"
                    if USE_COLOR:
                        pct_color = GREEN if pct <= 0 else RED
                        pct_str = colorize(pct_str, pct_color)
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
    async def periodic_refresh(self) -> None:
        while not self.stop_event.is_set():
            try:
                async with ApiClient(
                    self.credentials["api_user"],
                    self.credentials["api_signer"],
                    self.credentials["api_private_key"],
                ) as client:
                    snapshot = await client.signed_request("GET", "/fapi/v3/account", {})
                self.update_from_snapshot(snapshot)
                self.render("REST REFRESH")
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
                summary = self._summarize_exception(exc)
                logging.getLogger("TerminalDashboard").warning("Refresh error: %s", exc)
                self.last_reason = f"Refresh error ({summary})"
                self.render("REFRESH ERROR")
            except Exception as exc:
                logging.getLogger("TerminalDashboard").error("Unexpected refresh error: %s", exc, exc_info=True)
                self.last_reason = f"Refresh error ({self._summarize_exception(exc)})"
                self.render("REFRESH ERROR")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.refresh_interval)
            except asyncio.TimeoutError:
                continue
        self.last_reason = "Refresh stopped"

    async def spot_balance_worker(self) -> None:
        if not self.spot_fetcher:
            return
        try:
            while not self.stop_event.is_set():
                try:
                    snapshot = await self.spot_fetcher.fetch_snapshot()
                    self.update_spot_snapshot(snapshot)
                    self.render("SPOT SNAPSHOT")
                except asyncio.CancelledError:
                    raise
                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
                    summary = self._summarize_exception(exc)
                    logging.getLogger("TerminalDashboard").warning("Spot error: %s", exc)
                    self.last_reason = f"Spot error ({summary})"
                    self.render("SPOT ERROR")
                except Exception as exc:
                    logging.getLogger("TerminalDashboard").error("Unexpected spot error: %s", exc, exc_info=True)
                    self.last_reason = f"Spot error ({self._summarize_exception(exc)})"
                    self.render("SPOT ERROR")
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=SPOT_REFRESH_INTERVAL)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise
        finally:
            await self.spot_fetcher.aclose()

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
                            info.update({
                                "mark": to_float(payload.get("p")),
                                "index": to_float(payload.get("i")),
                                "funding": to_float(payload.get("r")) * 100,
                                "time": payload.get("E"),
                            })
                            self._recalc_unrealized(symbol)
                            self.render("MARK PRICE")
                            continue
                        if event_type != "bookTicker":
                            continue
                        bid = to_float(payload.get("b"))
                        ask = to_float(payload.get("a"))
                        updated = False
                        if bid > 0:
                            info["bid"] = bid
                            updated = True
                        if ask > 0:
                            info["ask"] = ask
                            updated = True
                        mid = None
                        if bid > 0 and ask > 0:
                            mid = (bid + ask) / 2
                        elif ask > 0:
                            mid = ask
                        elif bid > 0:
                            mid = bid
                        if mid is not None:
                            info["mid"] = mid
                            updated = True
                        if updated:
                            info["book_time"] = payload.get("E")
                            now = time.monotonic()
                            if now - self._last_book_render >= self._book_render_interval:
                                self._last_book_render = now
                                self.render("BOOK TICKER")
            except ConnectionClosedOK:
                self.last_reason = "Mark stream closed"
                self.render("MARK STREAM CLOSED")
                await asyncio.sleep(MARK_STREAM_RETRY)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, json.JSONDecodeError, websockets.WebSocketException) as exc:
                summary = self._summarize_exception(exc)
                logging.getLogger("TerminalDashboard").warning("Mark stream error: %s", exc)
                self.last_reason = f"Mark stream error ({summary})"
                self.render("MARK STREAM ERROR")
                await asyncio.sleep(MARK_STREAM_RETRY)
            except Exception as exc:
                logging.getLogger("TerminalDashboard").error("Unexpected mark stream error: %s", exc, exc_info=True)
                self.last_reason = f"Mark stream error ({self._summarize_exception(exc)})"
                self.render("MARK STREAM ERROR")
                await asyncio.sleep(MARK_STREAM_RETRY)
        self.last_reason = "Mark stream stopped"

    async def stream(self, ws_url: str) -> None:
        try:
            async with websockets.connect(ws_url) as ws:
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
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, json.JSONDecodeError, websockets.WebSocketException) as exc:
            if not self.stop_event.is_set():
                summary = self._summarize_exception(exc)
                logging.getLogger("TerminalDashboard").warning("Stream error: %s", exc)
                self.last_reason = f"Stream error ({summary})"
                self.render("STREAM ERROR")
        except Exception as exc:
            if not self.stop_event.is_set():
                logging.getLogger("TerminalDashboard").error("Unexpected stream error: %s", exc, exc_info=True)
                self.last_reason = f"Stream error ({self._summarize_exception(exc)})"
                self.render("STREAM ERROR")


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

    load_dotenv()

    api_user = os.getenv("API_USER")
    api_signer = os.getenv("API_SIGNER")
    api_private_key = os.getenv("API_PRIVATE_KEY")
    spot_api_key = os.getenv("SPOT_API_KEY")
    spot_api_secret = os.getenv("SPOT_API_SECRET")

    if not all([api_user, api_signer, api_private_key]):
        print("ERROR: Missing required environment variables")
        print("Required: API_USER, API_SIGNER, API_PRIVATE_KEY")
        return

    credentials = {
        "api_user": api_user,
        "api_signer": api_signer,
        "api_private_key": api_private_key,
    }

    spot_fetcher = None
    if spot_api_key and spot_api_secret:
        spot_fetcher = SpotBalanceFetcher(
            spot_api_key,
            spot_api_secret,
            base_asset=SPOT_BASE_ASSET,
            usd_rate=SPOT_USD_RATE,
            min_total=SPOT_MIN_TOTAL,
            show_zero=False,
        )

    dashboard = TerminalDashboard(
        credentials,
        stop_event,
        refresh_interval=args.refresh_interval,
        spot_fetcher=spot_fetcher,
    )

    async with ApiClient(api_user, api_signer, api_private_key) as client:
        snapshot = await client.signed_request("GET", "/fapi/v3/account", {})
        dashboard.update_from_snapshot(snapshot)
        response = await client.create_listen_key()
        listen_key = response.get("listenKey")
        if not listen_key:
            print("ERROR: Failed to retrieve listenKey from API response")
            return

    ws_url = f"wss://fstream.asterdex.com/ws/{listen_key}"

    refresh_task = asyncio.create_task(dashboard.periodic_refresh())
    mark_task = asyncio.create_task(dashboard.mark_price_listener())
    stream_task = asyncio.create_task(dashboard.stream(ws_url))
    spot_task = asyncio.create_task(dashboard.spot_balance_worker())

    tasks = {refresh_task, mark_task, stream_task, spot_task}
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
