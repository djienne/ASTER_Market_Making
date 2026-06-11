import os
import asyncio
import argparse
import logging
import orjson
import websockets
import json
import signal
import time
import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Optional
import requests
from api_client import ApiClient
from logging_config import setup_root_logging
from utils import configured_symbol, load_project_env
from vol_obi import VolObiCalculator

load_project_env()


def env_flag(name, default):
    """Parse a boolean environment flag with a sane default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}

# --- Configuration ---
# STRATEGY
DEFAULT_SYMBOL = configured_symbol()
DEFAULT_LEVERAGE = 1
DEFAULT_BALANCE_FRACTION = 0.2  # Use a fraction of tracked wallet balance for each order

# VOL+OBI STRATEGY (port of the lighter_MM vol_obi quoting model)
OBI_WINDOW_STEPS = 6000                       # Rolling window for vol/imbalance stats
OBI_STEP_NS = 100_000_000                     # Binance diff-depth cadence (100ms)
OBI_VOL_TO_HALF_SPREAD = float(os.getenv("OBI_VOL_TO_HALF_SPREAD", "42.0"))
# NOTE: lighter_MM production config used 42.0 on the same 100ms signal cadence;
# the vol_obi.py code default is 0.8. This gain is the primary tuning knob.
OBI_MIN_HALF_SPREAD_BPS = float(os.getenv("OBI_MIN_HALF_SPREAD_BPS", "4.0"))
OBI_C1_TICKS = float(os.getenv("OBI_C1_TICKS", "120.0"))  # alpha → fair shift, in ticks per sigma
OBI_SKEW = float(os.getenv("OBI_SKEW", "1.5"))            # inventory skew gain
OBI_LOOKING_DEPTH = 0.025                     # Imbalance band: +/- 2.5% around Binance mid
OBI_MIN_WARMUP_SAMPLES = 100                  # Samples required before quoting
MAX_POSITION_SAFETY_FACTOR = 0.9              # Headroom under the leverage-derived position cap
BALANCE_EPSILON_USD = 0.01                    # Epsilon for USD balance comparisons

# TIMING (in seconds)
ORDER_REFRESH_INTERVAL = 60     # Safety lifetime for a working order before a forced refresh, in seconds.
RETRY_ON_ERROR_INTERVAL = 30    # How long to wait after a major error before retrying.
PRICE_REPORT_INTERVAL = 60      # How often to report current prices and spread to terminal.
BALANCE_REPORT_INTERVAL = 60    # How often to report account balance to terminal.
POSITION_SYNC_TIMEOUT = 2.0     # How long to wait for a position snapshot after a fill.
STARTUP_CLEANUP_TIMEOUT = 20.0  # How long to wait for the initial cancel-all cleanup.
CANCEL_CONFIRM_TIMEOUT = 5.0    # How long to wait for a terminal update after canceling a timed-out order.
WEBSOCKET_MAX_CONNECTION_AGE = 23 * 60 * 60  # Rotate websocket connections before the documented 24h server limit.
SHUTDOWN_ACTIVE_ORDER_GRACE_TIMEOUT = 8.0
SHUTDOWN_CANCEL_ALL_TIMEOUT = 20.0
SHUTDOWN_CANCEL_ALL_RETRIES = 2

# ORDER REUSE SETTINGS
DEFAULT_PRICE_CHANGE_THRESHOLD_BPS = 5.0  # Minimum price move required before replacing an order
DEFAULT_PRICE_CHANGE_THRESHOLD = DEFAULT_PRICE_CHANGE_THRESHOLD_BPS / 10000.0
OPENING_CAPITAL_BUFFER_MULTIPLIER = 1.25  # Safety headroom above the exchange minimum for opening orders.
ORDER_FAILURE_WINDOW_SECONDS = 60.0
ORDER_FAILURE_LIMIT = 3
OPENING_CIRCUIT_BREAKER_COOLDOWN = 120.0

# BINANCE FEED PLUMBING (drives the Vol+OBI signal)
BINANCE_OBI_BOOK_RETAIN_PCT = 0.03
BINANCE_OBI_STALE_TIMEOUT_SECONDS = 5.0
BINANCE_OBI_TRIM_INTERVAL_SECONDS = 1.0
BINANCE_OBI_TRIM_INTERVAL_UPDATES = 10
BINANCE_OBI_BAND_REBUILD_BPS = 1.0

# ORDER CANCELLATION
ORDER_REPLACE_MODE = os.getenv("ORDER_REPLACE_MODE", "fast").strip().lower()
FAST_ORDER_REPLACE = ORDER_REPLACE_MODE == "fast"
OPEN_ORDER_WATCHDOG_INTERVAL = 15.0
OPEN_ORDER_WATCHDOG_CANCEL_ALL = True
OPEN_ORDER_WATCHDOG_STALE_GRACE = 5.0  # Grace before clearing tracking for an order missing on the exchange.
QUOTE_REFRESH_PREFILTER_BPS = 2.0

# LOGGING
LOG_FILE = 'market_maker.log'
RELEASE_MODE = env_flag("RELEASE_MODE", True)  # When True, suppress all non-error logs and prints

MIN_ORDER_INTERVAL = 1.0  # Minimum seconds between order reconcile bursts
POSITION_SIZE_EPSILON = 1e-12


class BinanceOrderBookSyncError(Exception):
    """Raised when the Binance local book must be resynchronized."""


@dataclass(frozen=True)
class AsterTopOfBookSnapshot:
    bid_price: float
    ask_price: float
    mid_price: float
    updated_at: float


@dataclass(frozen=True)
class VolObiSnapshot:
    """Immutable view of the Vol+OBI signal published by the Binance feed."""
    warmed_up: bool = False
    volatility: float = 0.0
    alpha: float = 0.0
    sample_count: int = 0
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_updated: Optional[float] = None
    ws_connected: bool = False


@dataclass(frozen=True)
class PendingTerminalOrder:
    side: str
    reduce_only: bool
    position_update_seq_before_fill: int
    order_label: str
    cancel_requested_at: float


@dataclass(frozen=True)
class SideQuote:
    """Desired resting order for one side of the book."""
    side: str
    price: float
    quantity: float
    reduce_only: bool = False
    order_notional: float = 0.0


@dataclass(frozen=True)
class QuoteSetCommand:
    """The latest desired full state of both sides for the order manager.

    One command always carries the complete desired quote set so the
    latest-wins maxsize-1 handoff queue can never evict half of an update.
    """
    kind: str                       # "quote_set" | "cancel_all"
    bid: Optional[SideQuote] = None
    ask: Optional[SideQuote] = None
    trigger: str = ""


def publish_vol_obi_snapshot(state):
    """Mirror mutable Vol+OBI signal fields into a single immutable snapshot."""
    calc = state.vol_obi_calc
    state.vol_obi_snapshot = VolObiSnapshot(
        warmed_up=bool(calc is not None and calc.warmed_up),
        volatility=float(calc.volatility) if calc is not None else 0.0,
        alpha=float(calc.alpha) if calc is not None else 0.0,
        sample_count=int(calc.total_samples) if calc is not None else 0,
        best_bid=state.binance_best_bid,
        best_ask=state.binance_best_ask,
        last_updated=state.binance_alpha_last_updated,
        ws_connected=bool(state.binance_alpha_ws_connected),
    )


def resolve_symbol(cli_symbol=None):
    """Resolve the active symbol from CLI input or the single runtime config source."""
    symbol = cli_symbol or configured_symbol(DEFAULT_SYMBOL)
    return symbol.upper()


class RuntimeContext:
    """Holds runtime-only state such as timing and shutdown signals."""
    def __init__(self, symbol, clock=None):
        self.symbol = resolve_symbol(symbol)
        self.shutdown_requested = False
        self.price_last_updated = None
        self.last_order_time = 0.0
        self._clock = clock

    def now(self):
        if self._clock is not None:
            return self._clock()

        try:
            return asyncio.get_running_loop().time()
        except RuntimeError:
            return time.monotonic()

    def request_shutdown(self):
        self.shutdown_requested = True


def setup_logging(file_log_level):
    """Configure non-blocking logging (console INFO, file at the given level).

    Log records are drained by a background QueueListener thread so the
    asyncio event loop never blocks on disk/console I/O (hot-path safety).
    """
    log_level = getattr(logging, file_log_level.upper(), logging.DEBUG)
    setup_root_logging(log_file=LOG_FILE, release_mode=RELEASE_MODE, file_log_level=log_level)


@dataclass
class SideOrderState:
    """Mutable tracking for the resting order on one side of the book."""
    order_id: Optional[int] = None
    price: Optional[float] = None
    quantity: Optional[float] = None
    reduce_only: bool = False
    placed_at: Optional[float] = None


class StrategyState:
    """A simple class to hold the shared state of the strategy."""
    def __init__(self):
        self.bid_price = None
        self.ask_price = None
        self.mid_price = None
        self.position_size = 0.0
        # Per-side resting order tracking (two-sided quoting)
        self.side_orders = {'BUY': SideOrderState(), 'SELL': SideOrderState()}
        # Account balance tracking
        self.account_balance = None  # Total USDF + USDT + USDC balance
        self.balance_last_updated = None
        self.balance_listen_key = None
        self.usdf_balance = 0.0
        self.usdt_balance = 0.0
        self.usdc_balance = 0.0
        # Queue for order updates from WebSocket
        self.order_updates = asyncio.Queue()
        # Latest-wins handoff from quote engine to order manager
        self.order_commands = asyncio.Queue(maxsize=1)
        self.quote_refresh_event = asyncio.Event()
        self.aster_top_of_book_snapshot = None
        # WebSocket connection health flags
        self.price_ws_connected = False
        self.user_data_ws_connected = False
        self.symbol_filters = None
        # Position snapshots are the source of truth for inventory state
        self.position_update_seq = 0
        self.order_failure_timestamps = deque()
        self.opening_circuit_breaker_until = 0.0
        # Vol+OBI strategy signal (fed by the Binance depth stream)
        self.vol_obi_calc = None  # VolObiCalculator, created in main() once tick_size is known
        self.vol_obi_snapshot = VolObiSnapshot()
        self.binance_last_refresh_alpha = None
        # Binance local order book state
        self.binance_bid_book = {}
        self.binance_ask_book = {}
        self.binance_last_update_id = None
        self.binance_alpha_ws_connected = False
        self.binance_alpha_last_updated = None
        self.binance_best_bid = None
        self.binance_best_ask = None
        self.binance_book_last_trim_at = 0.0
        self.binance_book_updates_since_trim = 0
        self.binance_band_mid_price = None
        self.binance_band_lower_bound = None
        self.binance_band_upper_bound = None
        self.binance_band_bid_qty = 0.0
        self.binance_band_ask_qty = 0.0
        self.pending_terminal_orders = {}


def get_position_close_side(position_size):
    """Return the side required to reduce the current position."""
    if position_size > 0:
        return 'SELL'
    if position_size < 0:
        return 'BUY'
    return None


def has_open_position_size(position_size):
    """Return True when the tracked position is materially non-zero."""
    return abs(position_size) > POSITION_SIZE_EPSILON


def has_open_position(state):
    """Return True when the strategy is carrying any non-zero inventory."""
    return has_open_position_size(state.position_size)


def clear_side_order(state, side):
    """Clear the tracked resting order for one side."""
    state.side_orders[side] = SideOrderState()


def has_live_orders(state):
    """Return True when any side has a tracked resting order."""
    return any(side_state.order_id is not None for side_state in state.side_orders.values())


def get_tracked_side_for_order(state, order_id):
    """Return the side ('BUY'/'SELL') tracking the given order id, or None."""
    if order_id is None:
        return None
    for side, side_state in state.side_orders.items():
        if side_state.order_id == order_id:
            return side
    return None


def get_position_notional_usd(position_size, reference_price):
    """Estimate the USD notional of a position from a reference price."""
    if reference_price is None or reference_price <= 0:
        return 0.0
    return abs(position_size * reference_price)


def apply_position_snapshot(state, position_size):
    """Store the latest position size without erasing residual inventory."""
    state.position_size = position_size
    state.position_update_seq += 1


def extract_position_snapshot(position_data, reference_price=None):
    """Normalize an exchange position payload into size and notional values."""
    raw_size = position_data.get('positionAmt')
    if raw_size is None:
        raw_size = position_data.get('pa', 0.0)

    position_size = float(raw_size or 0.0)

    raw_notional = position_data.get('notional')
    if raw_notional is not None:
        return position_size, abs(float(raw_notional))

    entry_price = float(position_data.get('ep', position_data.get('entryPrice', 0.0)) or 0.0)
    reference = reference_price if reference_price and reference_price > 0 else entry_price
    return position_size, get_position_notional_usd(position_size, reference)


def sync_state_from_position_data(state, position_data, reference_price=None):
    """Apply an exchange position payload to local state."""
    position_size, notional_value = extract_position_snapshot(position_data, reference_price=reference_price)
    apply_position_snapshot(state, position_size)
    return position_size, notional_value


def request_quote_refresh(state):
    """Wake the quote engine to recompute the desired working order."""
    state.quote_refresh_event.set()


def publish_latest_order_command(state, command):
    """Push the latest order-manager command, replacing any stale pending one."""
    while True:
        try:
            state.order_commands.get_nowait()
        except asyncio.QueueEmpty:
            break

    state.order_commands.put_nowait(command)


def drain_latest_order_command(state, initial_command):
    """Collapse queued commands and return only the newest one."""
    latest_command = initial_command
    while True:
        try:
            latest_command = state.order_commands.get_nowait()
        except asyncio.QueueEmpty:
            return latest_command


def publish_cancel_all_if_live(state, trigger):
    """Ask the order manager to pull all working orders if any side is live."""
    if has_live_orders(state):
        publish_latest_order_command(state, QuoteSetCommand(kind="cancel_all", trigger=trigger))


def round_price_to_tick(price, tick_size, side):
    """Round prices to a passive tick for the given side."""
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")

    scaled = price / tick_size
    if side == 'BUY':
        rounded = math.floor(scaled + 1e-12) * tick_size
    elif side == 'SELL':
        rounded = math.ceil(scaled - 1e-12) * tick_size
    else:
        raise ValueError(f"Unsupported side for price rounding: {side}")

    return rounded


def _binance_ws_symbol(symbol):
    """Return the lowercase Binance websocket symbol."""
    return (symbol or "").lower()


def _binance_depth_stream_url(symbol):
    """Return the public Binance diff-book stream URL for the symbol."""
    return f"wss://fstream.binance.com/ws/{_binance_ws_symbol(symbol)}@depth@100ms"


def _binance_depth_snapshot_url(symbol):
    """Return the Binance REST depth snapshot URL for the symbol."""
    return f"https://fapi.binance.com/fapi/v1/depth?symbol={(symbol or '').upper()}&limit=1000"


def clear_binance_alpha_state(state):
    """Reset all in-memory Binance book and Vol+OBI state to avoid stale reuse."""
    state.binance_bid_book.clear()
    state.binance_ask_book.clear()
    state.binance_last_update_id = None
    state.binance_alpha_last_updated = None
    state.binance_best_bid = None
    state.binance_best_ask = None
    state.binance_book_last_trim_at = 0.0
    state.binance_book_updates_since_trim = 0
    state.binance_band_mid_price = None
    state.binance_band_lower_bound = None
    state.binance_band_upper_bound = None
    state.binance_band_bid_qty = 0.0
    state.binance_band_ask_qty = 0.0
    state.binance_last_refresh_alpha = None
    # A stale signal must never survive a reconnect: warmup restarts by design.
    if state.vol_obi_calc is not None:
        state.vol_obi_calc.reset()
    publish_vol_obi_snapshot(state)


def _refresh_binance_best_prices(state):
    """Refresh cached best prices from the current local Binance book."""
    state.binance_best_bid = max(state.binance_bid_book) if state.binance_bid_book else None
    state.binance_best_ask = min(state.binance_ask_book) if state.binance_ask_book else None


def _price_is_inside_binance_band(price, lower_bound, upper_bound, is_bid):
    """Return True when a price contributes to the active OBI band for its side."""
    if lower_bound is None or upper_bound is None:
        return False
    if is_bid:
        return price >= lower_bound
    return price <= upper_bound


def _apply_book_updates(book, updates, current_best_price, is_bid, lower_bound=None, upper_bound=None):
    """Apply absolute-quantity Binance depth updates into a local side book."""
    best_price = current_best_price
    best_invalidated = False
    band_delta = 0.0
    for price_raw, qty_raw in updates:
        price = float(price_raw)
        qty = max(float(qty_raw), 0.0)
        previous_qty = float(book.get(price, 0.0) or 0.0)
        if _price_is_inside_binance_band(price, lower_bound, upper_bound, is_bid):
            band_delta += qty - previous_qty

        if qty <= 0.0:
            removed = book.pop(price, None)
            if removed is not None and best_price is not None and price == best_price:
                best_invalidated = True
        else:
            book[price] = qty
            if best_price is None:
                best_price = price
            elif is_bid and price > best_price:
                best_price = price
            elif not is_bid and price < best_price:
                best_price = price

    if best_invalidated:
        if book:
            best_price = max(book) if is_bid else min(book)
        else:
            best_price = None

    return best_price, band_delta


def _trim_binance_books(state):
    """Keep the local Binance book bounded around the current mid price."""
    if not state.binance_bid_book or not state.binance_ask_book:
        return

    if state.binance_best_bid is None or state.binance_best_ask is None:
        _refresh_binance_best_prices(state)
    best_bid = state.binance_best_bid
    best_ask = state.binance_best_ask
    if best_bid is None or best_ask is None:
        return

    mid_price = (best_bid + best_ask) / 2.0
    lower_bound = mid_price * (1.0 - BINANCE_OBI_BOOK_RETAIN_PCT)
    upper_bound = mid_price * (1.0 + BINANCE_OBI_BOOK_RETAIN_PCT)

    stale_bids = [price for price in state.binance_bid_book if price < lower_bound]
    stale_asks = [price for price in state.binance_ask_book if price > upper_bound]
    for price in stale_bids:
        state.binance_bid_book.pop(price, None)
    for price in stale_asks:
        state.binance_ask_book.pop(price, None)


def _rebuild_binance_band_totals(state):
    """Rebuild the cached OBI depth totals from the bounded local book."""
    if state.binance_best_bid is None or state.binance_best_ask is None:
        _refresh_binance_best_prices(state)
    best_bid = state.binance_best_bid
    best_ask = state.binance_best_ask
    if best_bid is None or best_ask is None or best_bid <= 0.0 or best_ask <= 0.0 or best_bid >= best_ask:
        state.binance_band_mid_price = None
        state.binance_band_lower_bound = None
        state.binance_band_upper_bound = None
        state.binance_band_bid_qty = 0.0
        state.binance_band_ask_qty = 0.0
        return

    mid_price = (best_bid + best_ask) / 2.0
    lower_bound = mid_price * (1.0 - OBI_LOOKING_DEPTH)
    upper_bound = mid_price * (1.0 + OBI_LOOKING_DEPTH)

    bid_qty = 0.0
    for price, qty in state.binance_bid_book.items():
        if price >= lower_bound:
            bid_qty += qty

    ask_qty = 0.0
    for price, qty in state.binance_ask_book.items():
        if price <= upper_bound:
            ask_qty += qty

    state.binance_band_mid_price = mid_price
    state.binance_band_lower_bound = lower_bound
    state.binance_band_upper_bound = upper_bound
    state.binance_band_bid_qty = bid_qty
    state.binance_band_ask_qty = ask_qty


def _binance_band_requires_rebuild(state):
    """Return True when the current OBI totals must be rebuilt from the local book."""
    if state.binance_best_bid is None or state.binance_best_ask is None:
        return True
    if state.binance_band_mid_price is None:
        return True

    current_mid = (state.binance_best_bid + state.binance_best_ask) / 2.0
    if current_mid <= 0.0:
        return True

    drift_bps = abs(current_mid - state.binance_band_mid_price) / current_mid * 10000.0
    return drift_bps >= BINANCE_OBI_BAND_REBUILD_BPS


def compute_binance_band_imbalance(state):
    """Compute the raw Binance book imbalance (bid qty - ask qty) within the band."""
    if not state.binance_bid_book or not state.binance_ask_book:
        return None

    if _binance_band_requires_rebuild(state):
        _rebuild_binance_band_totals(state)

    if state.binance_band_mid_price is None:
        return None

    return state.binance_band_bid_qty - state.binance_band_ask_qty


def is_vol_obi_live(state, runtime):
    """Return True when the Vol+OBI signal is warmed up, connected, and fresh.

    With two resting orders the bot must pull quotes when the signal feed
    dies, so staleness is part of liveness.
    """
    snapshot = state.vol_obi_snapshot
    if not snapshot.warmed_up or not snapshot.ws_connected:
        return False
    if snapshot.last_updated is None:
        return False
    return (runtime.now() - snapshot.last_updated) <= BINANCE_OBI_STALE_TIMEOUT_SECONDS


def vol_obi_status_text(state):
    """Return a compact Vol+OBI status string for logs and reporters."""
    snapshot = state.vol_obi_snapshot
    if snapshot.warmed_up:
        return f"Vol+OBI vol=${snapshot.volatility:.4f}/s alpha={snapshot.alpha:+.2f}"
    if snapshot.ws_connected:
        return f"Vol+OBI warming samples={snapshot.sample_count}/{OBI_MIN_WARMUP_SAMPLES}"
    return "Vol+OBI feed unavailable"


def round_quantity_to_step(quantity, step_size):
    """Round quantity down to the nearest valid multiple of step_size."""
    if step_size <= 0:
        raise ValueError("step_size must be positive")

    if quantity <= 0:
        return 0.0

    quantity_dec = Decimal(str(quantity))
    step_dec = Decimal(str(step_size))
    steps = (quantity_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
    rounded = steps * step_dec
    return float(rounded)


class AsterTopOfBookFeed:
    """Publish immutable Aster top-of-book snapshots and prefilter quote refreshes."""

    def publish(self, state, runtime, quote_engine, best_bid, best_ask):
        mid_price = (best_bid + best_ask) / 2.0
        snapshot = AsterTopOfBookSnapshot(
            bid_price=best_bid,
            ask_price=best_ask,
            mid_price=mid_price,
            updated_at=runtime.now(),
        )
        previous_snapshot = state.aster_top_of_book_snapshot
        state.aster_top_of_book_snapshot = snapshot
        state.bid_price = best_bid
        state.ask_price = best_ask
        state.mid_price = mid_price
        runtime.price_last_updated = snapshot.updated_at
        return quote_engine.should_refresh_from_top_of_book(state, previous_snapshot, snapshot)


class BinanceAlphaEngine:
    """Maintain Binance order book state and feed the Vol+OBI calculator."""

    def clear(self, state):
        clear_binance_alpha_state(state)

    def initialize_local_book(self, state, snapshot):
        _initialize_binance_local_book(state, snapshot)
        publish_vol_obi_snapshot(state)

    def apply_depth_event(self, state, event, require_prev_match=True):
        return _apply_binance_depth_event(state, event, require_prev_match=require_prev_match)

    def update_metrics(self, state, runtime):
        _update_binance_alpha_metrics(state, runtime)


class QuoteEngine:
    """Own all quote decisions off immutable feed snapshots."""

    def estimate_quote_center(self, state, book_snapshot=None):
        snapshot = book_snapshot or state.aster_top_of_book_snapshot
        if snapshot is None:
            return None

        mid_price = snapshot.mid_price
        calc = state.vol_obi_calc
        vol_obi = state.vol_obi_snapshot
        if calc is not None and vol_obi.warmed_up:
            return mid_price + calc.c1 * vol_obi.alpha

        return mid_price

    def should_refresh_from_top_of_book(self, state, previous_snapshot, new_snapshot):
        if previous_snapshot is None or new_snapshot is None:
            return True

        previous_center = self.estimate_quote_center(state, book_snapshot=previous_snapshot)
        new_center = self.estimate_quote_center(state, book_snapshot=new_snapshot)
        if previous_center is None or new_center is None:
            return True

        tick_size = float((state.symbol_filters or {}).get("tick_size", 0.0) or 0.0)
        center_threshold = new_center * (QUOTE_REFRESH_PREFILTER_BPS / 10000.0)
        if tick_size > 0.0:
            center_threshold = max(center_threshold, tick_size)
        return abs(new_center - previous_center) >= center_threshold

    def build_quote_set(self, state, symbol_filters, runtime):
        return build_quote_set(state, symbol_filters, runtime)

    async def run(self, state, client, symbol, runtime):
        log = logging.getLogger('MarketMakerLoop')
        if state.symbol_filters is None:
            log.info(f"Fetching trading rules for {symbol}...")
            state.symbol_filters = await client.get_symbol_filters(symbol)
            log.info(f"Filters loaded: {state.symbol_filters}")
        symbol_filters = state.symbol_filters
        request_quote_refresh(state)

        while not runtime.shutdown_requested:
            try:
                await state.quote_refresh_event.wait()
                state.quote_refresh_event.clear()

                while True:
                    if runtime.shutdown_requested:
                        break

                    if symbol_filters.get("status", "TRADING") != "TRADING":
                        publish_cancel_all_if_live(
                            state,
                            f"Symbol status {symbol_filters.get('status', 'UNKNOWN')}",
                        )
                        break

                    if not state.price_ws_connected or not state.user_data_ws_connected:
                        publish_cancel_all_if_live(state, "WebSocket disconnection")
                        break

                    if not is_price_data_valid(state, runtime) or not is_balance_data_valid(state):
                        break

                    quote_set, diagnostics = self.build_quote_set(state, symbol_filters, runtime)
                    if quote_set is None:
                        publish_cancel_all_if_live(
                            state,
                            f"Quotes unavailable: {diagnostics['reason']}",
                        )
                        break

                    if quote_set_requires_update(state, quote_set):
                        publish_latest_order_command(state, quote_set)

                    if not state.quote_refresh_event.is_set():
                        break
                    state.quote_refresh_event.clear()

            except asyncio.CancelledError:
                log.info("Quote engine cancelled.")
                break
            except Exception as exc:
                log.error(f"An error occurred in the quote engine: {exc}", exc_info=True)
                await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)


class OrderExecutor:
    """Own exchange-side actions, including replace policy and cold-path watchdogs."""

    def __init__(self, fast_replace=FAST_ORDER_REPLACE):
        self.fast_replace = bool(fast_replace)

    async def place_order(self, state, client, symbol, runtime, log, quote):
        return await place_side_order(
            state,
            client,
            symbol,
            runtime,
            log,
            quote,
            symbol_filters=state.symbol_filters,
        )

    async def run(self, state, client, symbol, runtime):
        return await order_manager_loop_impl(state, client, symbol, runtime, executor=self)

    async def watch_open_orders(self, state, client, symbol, runtime):
        log = logging.getLogger("OrderWatchdog")

        while not runtime.shutdown_requested:
            try:
                await asyncio.sleep(OPEN_ORDER_WATCHDOG_INTERVAL)
                if runtime.shutdown_requested:
                    break

                open_orders = await client.get_open_orders(symbol)
                open_order_ids = {order.get("orderId") for order in open_orders}

                # Clear local tracking for orders the exchange no longer has.
                # Grace window avoids racing a freshly placed order whose
                # GET /openOrders snapshot hasn't propagated yet.
                for side, side_state in state.side_orders.items():
                    if side_state.order_id is None or side_state.order_id in open_order_ids:
                        continue
                    age = (
                        runtime.now() - side_state.placed_at
                        if side_state.placed_at is not None
                        else float("inf")
                    )
                    if age >= OPEN_ORDER_WATCHDOG_STALE_GRACE:
                        log.warning(
                            "Tracked %s order %s missing from exchange for %.1fs; clearing stale tracking.",
                            side, side_state.order_id, age,
                        )
                        state.pending_terminal_orders.pop(side_state.order_id, None)
                        clear_side_order(state, side)
                        request_quote_refresh(state)

                tracked_ids = {
                    side_state.order_id
                    for side_state in state.side_orders.values()
                    if side_state.order_id is not None
                }
                tracked_ids.update(state.pending_terminal_orders.keys())
                untracked = [
                    order_id for order_id in open_order_ids if order_id not in tracked_ids
                ]
                if not untracked:
                    continue

                log.error(f"Detected untracked open orders for {symbol}: {untracked}")
                if OPEN_ORDER_WATCHDOG_CANCEL_ALL:
                    await client.cancel_all_orders(symbol)
                    log.error(f"Cancelled all open orders for {symbol} after watchdog detected untracked orders.")
                    for side in list(state.side_orders.keys()):
                        clear_side_order(state, side)
                    state.pending_terminal_orders.clear()
                else:
                    for order_id in untracked:
                        await client.cancel_order(symbol, order_id)
                    log.error(f"Cancelled untracked open orders for {symbol}; keeping tracked orders {sorted(tracked_ids)}.")

                request_quote_refresh(state)

            except asyncio.CancelledError:
                log.info("Order watchdog cancelled.")
                break
            except Exception as exc:
                log.error(f"Order watchdog error: {exc}", exc_info=True)

async def cancel_side_order(state, client, symbol, log, side, reason, clear_tracking_on_success=True):
    """Cancel the tracked order on one side and optionally clear local tracking on success."""
    side_state = state.side_orders[side]
    if side_state.order_id is None:
        return True

    order_id_to_cancel = side_state.order_id
    try:
        await client.cancel_order(symbol, order_id_to_cancel)
    except Exception as cancel_error:
        log.error(f"{reason}: failed to cancel {side} order {order_id_to_cancel}: {cancel_error}")
        return False

    if clear_tracking_on_success:
        clear_side_order(state, side)
    log.info(f"{reason}: cancelled {side} order {order_id_to_cancel}.")
    return True


async def wait_for_position_sync(state, previous_seq, timeout):
    """Wait for the next position snapshot to arrive."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while state.position_update_seq <= previous_seq:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(0.05, remaining))

    return True


async def reconcile_fill_with_position(state, client, symbol, log, previous_position_seq, fill_context):
    """Refresh tracked inventory after a fill using position snapshots as the source of truth."""
    if await wait_for_position_sync(state, previous_position_seq, POSITION_SYNC_TIMEOUT):
        return True

    log.warning(f"{fill_context}: no position snapshot arrived within {POSITION_SYNC_TIMEOUT:.1f}s; falling back to REST sync.")
    positions = await client.get_position_risk(symbol)
    if positions:
        sync_state_from_position_data(state, positions[0], reference_price=state.mid_price)
    else:
        apply_position_snapshot(state, 0.0)

    return False


async def websocket_price_updater(state, symbol, runtime, top_of_book_feed=None, quote_engine=None):
    """[MODIFIED] WebSocket-based price updater with exponential backoff and stale connection detection."""
    log = logging.getLogger('WebSocketPriceUpdater')
    top_of_book_feed = top_of_book_feed or AsterTopOfBookFeed()
    quote_engine = quote_engine or QuoteEngine()

    websocket_url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@depth5"
    reconnect_delay = 5  # Initial delay
    max_reconnect_delay = 60 # Maximum wait time

    while not runtime.shutdown_requested:
        try:
            log.info(f"Connecting to WebSocket: {websocket_url}")
            state.price_ws_connected = False # Mark as disconnected while attempting
            request_quote_refresh(state)

            async with websockets.connect(websocket_url, ping_interval=20, ping_timeout=10) as websocket:
                log.info(f"WebSocket connected for {symbol} depth stream")
                state.price_ws_connected = True # Mark as connected
                request_quote_refresh(state)
                reconnect_delay = 5  # Reset reconnect delay on successful connection
                connected_at = runtime.now()
                last_message_time = runtime.now()

                while not runtime.shutdown_requested:
                    try:
                        if runtime.now() - connected_at >= WEBSOCKET_MAX_CONNECTION_AGE:
                            log.info("Price WebSocket reached its max safe lifetime. Reconnecting proactively.")
                            break

                        # [MODIFIED] Wait for a message with a timeout to detect stale connections
                        message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                        last_message_time = runtime.now()

                        try:
                            data = orjson.loads(message)

                            if data.get('e') == 'depthUpdate' and ('b' in data and 'a' in data):
                                bids = data.get('b', [])
                                asks = data.get('a', [])

                                if bids and asks:
                                    best_bid = float(bids[0][0])
                                    best_ask = float(asks[0][0])
                                    if best_bid != state.bid_price or best_ask != state.ask_price:
                                        should_refresh = top_of_book_feed.publish(
                                            state,
                                            runtime,
                                            quote_engine,
                                            best_bid,
                                            best_ask,
                                        )
                                        if should_refresh:
                                            request_quote_refresh(state)
                                        log.debug(f"Updated prices for {symbol}: Bid={best_bid}, Ask={best_ask}, Mid={state.mid_price:.4f}")

                        except json.JSONDecodeError:
                            log.warning("Failed to decode WebSocket message")
                        except Exception as e:
                            log.error(f"Error processing WebSocket message: {e}")
                    
                    # [ADDED] Stale connection detection logic
                    except asyncio.TimeoutError:
                        time_since_last_msg = runtime.now() - last_message_time
                        if time_since_last_msg > 60:
                            log.warning(f"No price messages received for {time_since_last_msg:.1f}s. Connection may be stale. Reconnecting...")
                            break # Exit inner loop to force reconnection
                        else:
                            log.debug(f"Price WebSocket recv timed out ({time_since_last_msg:.1f}s since last message), but connection seems alive.")
                            continue # Continue waiting for messages

        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.InvalidState) as e:
            log.warning(f"Price WebSocket connection issue: {e}")
        except Exception as e:
            log.error(f"Price WebSocket error: {e}")
        finally:
            state.price_ws_connected = False # Mark as disconnected on any error/exit
            request_quote_refresh(state)

        if not runtime.shutdown_requested:
            log.info(f"Reconnecting to price WebSocket in {reconnect_delay:.1f}s...")
            await asyncio.sleep(reconnect_delay)
            # [MODIFIED] Implement exponential backoff
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    log.info("WebSocket price updater shutting down")


def _update_binance_alpha_metrics(state, runtime):
    """HOT PATH: feed the Vol+OBI calculator from the latest local Binance book.

    Runs once per 100ms diff-depth event. Everything here is O(1) float math
    against the incrementally maintained band totals — no REST, no disk, no
    book re-summation (the band is rebuilt only on >=1bps mid drift).
    """
    now = runtime.now()
    should_trim = (
        state.binance_book_updates_since_trim >= BINANCE_OBI_TRIM_INTERVAL_UPDATES
        or (now - state.binance_book_last_trim_at) >= BINANCE_OBI_TRIM_INTERVAL_SECONDS
    )
    if should_trim:
        _trim_binance_books(state)
        state.binance_book_last_trim_at = now
        state.binance_book_updates_since_trim = 0

    raw_imbalance = compute_binance_band_imbalance(state)
    if raw_imbalance is None:
        publish_vol_obi_snapshot(state)
        return

    calc = state.vol_obi_calc
    if calc is not None:
        calc.on_sample(state.binance_band_mid_price, raw_imbalance)

    state.binance_alpha_last_updated = now
    publish_vol_obi_snapshot(state)


async def _fetch_binance_depth_snapshot(symbol):
    """Fetch a Binance futures REST depth snapshot in a worker thread."""
    url = _binance_depth_snapshot_url(symbol)

    def _do_request():
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    return await asyncio.to_thread(_do_request)


def _initialize_binance_local_book(state, snapshot):
    """Reset and seed the local Binance book from a REST snapshot."""
    clear_binance_alpha_state(state)
    state.binance_last_update_id = int(snapshot["lastUpdateId"])
    state.binance_best_bid, _ = _apply_book_updates(
        state.binance_bid_book,
        snapshot.get("bids", []),
        current_best_price=None,
        is_bid=True,
    )
    state.binance_best_ask, _ = _apply_book_updates(
        state.binance_ask_book,
        snapshot.get("asks", []),
        current_best_price=None,
        is_bid=False,
    )
    _trim_binance_books(state)
    _rebuild_binance_band_totals(state)


def _extract_binance_depth_event(message):
    """Normalize raw or combined Binance websocket payloads into a depth event."""
    payload = message.get("data", message)
    if not isinstance(payload, dict):
        return None
    if "b" not in payload or "a" not in payload:
        return None
    return payload


def _apply_binance_depth_event(state, event, require_prev_match=True):
    """Apply a Binance diff-depth event to the local book, raising on sync errors."""
    final_update_id = int(event["u"])
    first_update_id = int(event["U"])
    previous_final_update_id = event.get("pu")

    if state.binance_last_update_id is None:
        raise BinanceOrderBookSyncError("Binance local book is not initialized")

    if require_prev_match:
        if previous_final_update_id is None or int(previous_final_update_id) != int(state.binance_last_update_id):
            raise BinanceOrderBookSyncError("Binance depth sequence gap detected")
    elif not (first_update_id <= int(state.binance_last_update_id) <= final_update_id):
        raise BinanceOrderBookSyncError("Initial Binance buffered event does not overlap the snapshot")

    if final_update_id < int(state.binance_last_update_id):
        return False

    previous_band_mid = state.binance_band_mid_price
    previous_band_lower = state.binance_band_lower_bound
    previous_band_upper = state.binance_band_upper_bound

    state.binance_best_bid, bid_band_delta = _apply_book_updates(
        state.binance_bid_book,
        event.get("b", []),
        current_best_price=state.binance_best_bid,
        is_bid=True,
        lower_bound=previous_band_lower,
        upper_bound=previous_band_upper,
    )
    state.binance_best_ask, ask_band_delta = _apply_book_updates(
        state.binance_ask_book,
        event.get("a", []),
        current_best_price=state.binance_best_ask,
        is_bid=False,
        lower_bound=previous_band_lower,
        upper_bound=previous_band_upper,
    )
    state.binance_last_update_id = final_update_id
    state.binance_book_updates_since_trim += 1

    if (
        state.binance_best_bid is None
        or state.binance_best_ask is None
        or state.binance_best_bid >= state.binance_best_ask
    ):
        _refresh_binance_best_prices(state)
        if (
            state.binance_best_bid is None
            or state.binance_best_ask is None
            or state.binance_best_bid >= state.binance_best_ask
        ):
            raise BinanceOrderBookSyncError("Binance local book became crossed or empty")

    if previous_band_mid is not None and not _binance_band_requires_rebuild(state):
        state.binance_band_bid_qty += bid_band_delta
        state.binance_band_ask_qty += ask_band_delta
    else:
        _rebuild_binance_band_totals(state)

    return True


def _vol_obi_alpha_refresh_required(state, calc):
    """Return True when alpha moved the fair price enough to warrant a requote."""
    last_alpha = state.binance_last_refresh_alpha
    if last_alpha is None:
        return True

    fair_shift_abs = abs(calc.c1 * (calc.alpha - last_alpha))
    reference_mid = state.mid_price or state.binance_band_mid_price or 0.0
    threshold = reference_mid * (QUOTE_REFRESH_PREFILTER_BPS / 10000.0)
    tick_size = float((state.symbol_filters or {}).get("tick_size", 0.0) or 0.0)
    if tick_size > 0.0:
        threshold = max(threshold, tick_size)
    return threshold > 0.0 and fair_shift_abs >= threshold


async def binance_orderbook_imbalance_updater(state, symbol, runtime, alpha_engine=None):
    """Maintain a bounded local Binance futures book feeding the Vol+OBI signal."""
    log = logging.getLogger("BinanceOBIUpdater")
    alpha_engine = alpha_engine or BinanceAlphaEngine()
    websocket_url = _binance_depth_stream_url(symbol)
    reconnect_delay = 5.0
    max_reconnect_delay = 60.0

    while not runtime.shutdown_requested:
        try:
            log.info(f"Connecting to Binance OBI stream: {websocket_url}")
            state.binance_alpha_ws_connected = False
            alpha_engine.clear(state)
            request_quote_refresh(state)

            async with websockets.connect(websocket_url, ping_interval=20, ping_timeout=10) as websocket:
                state.binance_alpha_ws_connected = True
                publish_vol_obi_snapshot(state)
                request_quote_refresh(state)
                reconnect_delay = 5.0
                log.info(f"Binance OBI connected for {symbol} via diff-depth @100ms")

                snapshot_task = asyncio.create_task(_fetch_binance_depth_snapshot(symbol))
                buffered_events = []

                while not snapshot_task.done() and not runtime.shutdown_requested:
                    message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                    event = _extract_binance_depth_event(orjson.loads(message))
                    if event is not None:
                        buffered_events.append(event)

                snapshot = await snapshot_task
                alpha_engine.initialize_local_book(state, snapshot)

                buffered_events = [event for event in buffered_events if int(event["u"]) >= int(state.binance_last_update_id)]
                start_index = None
                for idx, event in enumerate(buffered_events):
                    if int(event["U"]) <= int(state.binance_last_update_id) <= int(event["u"]):
                        start_index = idx
                        break
                if start_index is None and buffered_events:
                    raise BinanceOrderBookSyncError("Could not align Binance buffered events with snapshot")

                for idx, event in enumerate(buffered_events[start_index or 0:]):
                    alpha_engine.apply_depth_event(state, event, require_prev_match=(idx != 0))
                    alpha_engine.update_metrics(state, runtime)

                last_message_time = runtime.now()

                while not runtime.shutdown_requested:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=BINANCE_OBI_STALE_TIMEOUT_SECONDS)
                    except asyncio.TimeoutError:
                        raise BinanceOrderBookSyncError("Binance OBI stream became stale")

                    last_message_time = runtime.now()
                    event = _extract_binance_depth_event(orjson.loads(message))
                    if event is None:
                        continue

                    alpha_engine.apply_depth_event(state, event)
                    calc = state.vol_obi_calc
                    previous_warmed = calc.warmed_up if calc is not None else False
                    alpha_engine.update_metrics(state, runtime)

                    if calc is not None and calc.warmed_up:
                        if not previous_warmed:
                            log.info(
                                "Vol+OBI warmed up after %d samples: vol=$%.4f/s alpha=%+.2f",
                                calc.total_samples, calc.volatility, calc.alpha,
                            )
                            state.binance_last_refresh_alpha = calc.alpha
                            request_quote_refresh(state)
                        elif _vol_obi_alpha_refresh_required(state, calc):
                            state.binance_last_refresh_alpha = calc.alpha
                            request_quote_refresh(state)

                    if runtime.now() - last_message_time > BINANCE_OBI_STALE_TIMEOUT_SECONDS:
                        raise BinanceOrderBookSyncError("Binance OBI stream stale threshold exceeded")

        except BinanceOrderBookSyncError as exc:
            log.warning(f"{exc}. Reinitializing Binance local book.")
        except asyncio.CancelledError:
            log.info("Binance OBI updater cancelled.")
            break
        except Exception as exc:
            log.error(f"Binance OBI updater error: {exc}", exc_info=True)
        finally:
            state.binance_alpha_ws_connected = False
            alpha_engine.clear(state)
            request_quote_refresh(state)

        if not runtime.shutdown_requested:
            log.info(f"Reconnecting to Binance OBI in {reconnect_delay:.1f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    log.info("Binance OBI updater shutting down")

def is_price_data_valid(state, runtime):
    """Check if the price data is valid and recent."""
    if state.mid_price is None or runtime.price_last_updated is None:
        return False

    # Check if price data is recent (within 30 seconds)
    current_time = runtime.now()
    if current_time - runtime.price_last_updated > 30:
        return False

    return True


def is_balance_data_valid(state):
    """Check if the balance data is valid and recent."""
    if state.account_balance is None or state.balance_last_updated is None:
        return False

    return True


async def keepalive_balance_listen_key(state, client, runtime):
    """Periodically send keepalive for balance listen key."""
    log = logging.getLogger('BalanceKeepalive')

    while not runtime.shutdown_requested and state.balance_listen_key:
        try:
            # Sleep for 10 minutes (listen key expires in 60 minutes)
            await asyncio.sleep(600)

            if runtime.shutdown_requested or not state.balance_listen_key:
                break

            log.info("Sending keepalive for balance listen key...")
            await client.keepalive_listen_key()
            log.info("Balance listen key keepalive sent successfully")

        except asyncio.CancelledError:
            log.info("Balance keepalive task cancelled.")
            break
        except Exception as e:
            log.error(f"Failed to send balance listen key keepalive: {e}")

    log.info("Balance keepalive task shutting down")


async def websocket_user_data_updater(state, client, symbol, runtime):
    """[MODIFIED] WebSocket-based user data updater for account and order updates."""
    log = logging.getLogger('UserDataUpdater')
    reconnect_delay = 5
    max_reconnect_delay = 60 # Maximum wait time between reconnection attempts
    keepalive_task = None

    while not runtime.shutdown_requested:
        try:
            log.info("Getting listen key for user data stream...")
            state.user_data_ws_connected = False # Mark as disconnected
            request_quote_refresh(state)

            response = await client.create_listen_key()
            state.balance_listen_key = response['listenKey']
            log.info(f"User data listen key obtained: {state.balance_listen_key[:20]}...")

            keepalive_task = asyncio.create_task(keepalive_balance_listen_key(state, client, runtime))

            ws_url = f"wss://fstream.asterdex.com/ws/{state.balance_listen_key}"
            log.info(f"Connecting to user data WebSocket: {ws_url}")

            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as websocket:
                log.info("User data WebSocket connected!")
                state.user_data_ws_connected = True # Mark as connected
                request_quote_refresh(state)
                reconnect_delay = 5  # Reset reconnect delay on successful connection
                connected_at = runtime.now()

                while not runtime.shutdown_requested:
                    try:
                        if runtime.now() - connected_at >= WEBSOCKET_MAX_CONNECTION_AGE:
                            log.info("User data WebSocket reached its max safe lifetime. Reconnecting proactively.")
                            break

                        # Wait for a message with a timeout to detect stale connections
                        message = await asyncio.wait_for(websocket.recv(), timeout=30.0)

                        try:
                            data = orjson.loads(message)
                            event_type = data.get('e')

                            if event_type == 'ACCOUNT_UPDATE':
                                account_data = data.get('a', {})
                                balances = account_data.get('B', [])
                                for balance in balances:
                                    if balance.get('a') == 'USDF':
                                        state.usdf_balance = float(balance.get('wb', '0'))
                                    elif balance.get('a') == 'USDT':
                                        state.usdt_balance = float(balance.get('wb', '0'))
                                    elif balance.get('a') == 'USDC':
                                        state.usdc_balance = float(balance.get('wb', '0'))

                                state.account_balance = state.usdf_balance + state.usdt_balance + state.usdc_balance
                                state.balance_last_updated = runtime.now()
                                log.info(f"Balance updated: USDF={state.usdf_balance:.4f}, USDT={state.usdt_balance:.4f}, USDC={state.usdc_balance:.4f}, Total=${state.account_balance:.4f}")
                                request_quote_refresh(state)

                                # Also check for position updates in the same event
                                positions = account_data.get('P', [])
                                for position in positions:
                                    if position.get('s') == symbol:
                                        reference_price = state.mid_price if state.mid_price and state.mid_price > 0 else float(position.get('ep', '0') or 0.0)
                                        previous_position_size = state.position_size
                                        new_position_size, notional_value = sync_state_from_position_data(
                                            state,
                                            position,
                                            reference_price=reference_price,
                                        )

                                        if abs(previous_position_size - new_position_size) > 1e-9:
                                            log.info(f"Real-time position update for {symbol}: size changed from {previous_position_size:.6f} to {new_position_size:.6f} (${notional_value:.2f})")
                                        request_quote_refresh(state)

                            
                            elif event_type == 'ORDER_TRADE_UPDATE':
                                order_data = data.get('o', {})
                                log.debug(f"Queueing order update for {order_data.get('i')}: status {order_data.get('X')}")
                                await state.order_updates.put(data)

                            elif event_type == 'listenKeyExpired':
                                log.warning("User data listen key expired! Reconnecting...")
                                break # Exit inner loop to get a new key

                        except json.JSONDecodeError:
                            log.warning("Failed to decode user data WebSocket message")
                        except Exception as e:
                            log.error(f"Error processing user data message: {e}", exc_info=True)

                    except asyncio.TimeoutError:
                        log.debug("No user data events received in 30.0s; keeping the WebSocket open.")
                        continue

        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.InvalidState) as e:
            log.warning(f"User data WebSocket connection issue: {e}")
        except Exception as e:
            log.error(f"An unexpected error occurred in user data updater: {e}", exc_info=True)
        finally:
            state.user_data_ws_connected = False # Mark as disconnected on any error/exit
            request_quote_refresh(state)
            if keepalive_task and not keepalive_task.done():
                keepalive_task.cancel()
                try:
                    await keepalive_task
                except asyncio.CancelledError:
                    pass # Expected cancellation

        if not runtime.shutdown_requested:
            log.info(f"Reconnecting to user data WebSocket in {reconnect_delay:.1f}s...")
            await asyncio.sleep(reconnect_delay)
            # Exponential backoff
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    log.info("User data updater shutting down")


async def balance_reporter(state, runtime):
    """Periodically reports current account balance (only when not in release mode)."""
    log = logging.getLogger('BalanceReporter')

    # Only run balance reporter if not in release mode
    if RELEASE_MODE:
        log.info("Balance reporter disabled in release mode")
        return

    while not runtime.shutdown_requested:
        try:
            await asyncio.sleep(BALANCE_REPORT_INTERVAL)  # Report every 30 seconds

            if not runtime.shutdown_requested and is_balance_data_valid(state):
                log.info(f"Account Balance: USDF={state.usdf_balance:.4f}, USDT={state.usdt_balance:.4f}, USDC={state.usdc_balance:.4f}, Total=${state.account_balance:.4f}")

        except Exception as e:
            log.error(f"Error in balance reporter: {e}")

    log.info("Balance reporter shutting down")


async def price_reporter(state, symbol, runtime):
    """Periodically reports current mid-price and bid-ask spread."""
    log = logging.getLogger('PriceReporter')

    while not runtime.shutdown_requested:
        try:
            await asyncio.sleep(PRICE_REPORT_INTERVAL)

            if not runtime.shutdown_requested and is_price_data_valid(state, runtime):
                bid_ask_spread = state.ask_price - state.bid_price
                spread_percentage = (bid_ask_spread / state.mid_price) * 100 if state.mid_price > 0 else 0

                balance_info = ""
                if is_balance_data_valid(state):
                    balance_info = f" | Balance: ${state.account_balance:.2f}"

                alpha_info = f" | {vol_obi_status_text(state)}"
                log.info(
                    f"{symbol} | Mid-Price: ${state.mid_price:.4f} | Bid-Ask Spread: {spread_percentage:.3f}% "
                    f"| Bid: ${state.bid_price:.4f} | Ask: ${state.ask_price:.4f}{balance_info}{alpha_info}"
                )

        except Exception as e:
            log.error(f"Error in price reporter: {e}")

    log.info("Price reporter shutting down")


async def wait_for_startup_inputs(state, symbol, runtime):
    """Block startup until the Vol+OBI signal has warmed up."""
    log = logging.getLogger('StartupInputs')
    wait_seconds = 5
    last_status = None

    while not runtime.shutdown_requested:
        vol_obi_ready = is_vol_obi_live(state, runtime)
        if vol_obi_ready != last_status:
            if vol_obi_ready:
                log.info(f"Startup inputs ready for {symbol}: Vol+OBI signal warmed up.")
            else:
                log.info(
                    f"Waiting for startup inputs for {symbol}: {vol_obi_status_text(state)}. "
                    f"Retrying every {wait_seconds}s."
                )
            last_status = vol_obi_ready

        if vol_obi_ready:
            return True

        await asyncio.sleep(wait_seconds)

    return False


def should_reuse_side(side_state, new_price, threshold=DEFAULT_PRICE_CHANGE_THRESHOLD):
    """Check if the resting order on one side can be reused for the new price.

    Price-only comparison: desired quantity wobbles with every balance tick
    and partial fill, so comparing quantities would churn quotes pointlessly.
    """
    if side_state.order_id is None or side_state.price is None or side_state.price <= 0:
        return False

    price_change_pct = abs(new_price - side_state.price) / side_state.price
    return price_change_pct < threshold


def quote_set_requires_update(state, command):
    """Return True when the desired quote set differs from the live orders."""
    for side, quote in (('BUY', command.bid), ('SELL', command.ask)):
        side_state = state.side_orders[side]
        if quote is None:
            if side_state.order_id is not None:
                return True
            continue
        if side_state.order_id is None:
            return True
        if side_state.reduce_only != quote.reduce_only:
            return True
        if not should_reuse_side(side_state, quote.price):
            return True
    return False


def record_opening_order_failure(state, runtime):
    """Track exchange-side opening-order failures and trip a cooldown breaker when they cluster."""
    now = runtime.now()
    state.order_failure_timestamps.append(now)
    while state.order_failure_timestamps and now - state.order_failure_timestamps[0] > ORDER_FAILURE_WINDOW_SECONDS:
        state.order_failure_timestamps.popleft()

    if len(state.order_failure_timestamps) >= ORDER_FAILURE_LIMIT:
        state.opening_circuit_breaker_until = now + OPENING_CIRCUIT_BREAKER_COOLDOWN


def reset_opening_order_failures(state):
    """Clear the recent opening-order failure window after a healthy opening-order lifecycle event."""
    state.order_failure_timestamps.clear()
    state.opening_circuit_breaker_until = 0.0


def is_opening_circuit_breaker_active(state, runtime):
    """Return True while new opening quotes are paused due to repeated recent failures."""
    return runtime.now() < state.opening_circuit_breaker_until


def get_min_open_order_notional(symbol_filters, reference_price):
    """Estimate the minimum viable opening-order notional from exchange filters and price."""
    min_notional = float(symbol_filters.get('min_notional', 0.0) or 0.0)
    min_qty = float(symbol_filters.get('min_qty', 0.0) or 0.0)
    price = max(float(reference_price or 0.0), 0.0)
    return max(min_notional, min_qty * price)


def get_required_opening_balance(symbol_filters, reference_price):
    """Compute the minimum tracked wallet balance needed for an opening quote to clear exchange limits."""
    min_open_order_notional = get_min_open_order_notional(symbol_filters, reference_price)
    safe_min_open_notional = min_open_order_notional * OPENING_CAPITAL_BUFFER_MULTIPLIER
    if DEFAULT_BALANCE_FRACTION <= 0:
        return float("inf")
    return safe_min_open_notional / DEFAULT_BALANCE_FRACTION


def compute_max_position_usd(state):
    """Compute the dynamic max position notional from live capital and leverage.

    Mirrors lighter_MM's _dynamic_max_position_dollar with one level per side:
    reserve margin for both resting orders, then keep a safety margin.
    """
    balance = state.account_balance
    if balance is None or balance <= 0.0:
        return 0.0

    order_value = balance * DEFAULT_BALANCE_FRACTION
    raw = balance * DEFAULT_LEVERAGE - 2.0 * order_value
    return max(0.0, raw * MAX_POSITION_SAFETY_FACTOR)


def prepare_order_candidate(symbol_filters, side, reduce_only, limit_price, quantity_to_trade):
    """Round and validate an order candidate against exchange filters."""
    rounded_price = round_price_to_tick(limit_price, symbol_filters['tick_size'], side)
    rounded_quantity = round_quantity_to_step(quantity_to_trade, symbol_filters['step_size'])
    quantity_value = float(rounded_quantity)
    price_value = float(rounded_price)
    min_qty = symbol_filters['min_qty']
    min_notional = symbol_filters['min_notional']
    order_notional = price_value * quantity_value

    if quantity_value <= 0:
        return {
            "ok": False,
            "reason": "non_positive_quantity",
            "rounded_price": rounded_price,
            "rounded_quantity": rounded_quantity,
        }

    if quantity_value + POSITION_SIZE_EPSILON < min_qty:
        return {
            "ok": False,
            "reason": "min_qty",
            "rounded_price": rounded_price,
            "rounded_quantity": rounded_quantity,
            "min_qty": min_qty,
            "order_kind": "reduce-only" if reduce_only else "opening",
        }

    if order_notional < min_notional:
        return {
            "ok": False,
            "reason": "min_notional",
            "rounded_price": rounded_price,
            "rounded_quantity": rounded_quantity,
            "order_notional": order_notional,
            "min_notional": min_notional,
        }

    return {
        "ok": True,
        "rounded_price": rounded_price,
        "rounded_quantity": rounded_quantity,
        "order_notional": order_notional,
    }


def _build_side_quote(state, symbol_filters, side, price, mid_price, reduce_only):
    """Round, size, and validate one side of the desired quote set."""
    if price is None or price <= 0.0:
        return None

    if reduce_only:
        quantity_to_trade = abs(state.position_size)
    else:
        balance = state.account_balance
        if balance is None or balance <= 0.0:
            return None
        required_balance = get_required_opening_balance(symbol_filters, price)
        if balance + BALANCE_EPSILON_USD < required_balance:
            return None
        quantity_to_trade = (balance * DEFAULT_BALANCE_FRACTION) / mid_price

    order_candidate = prepare_order_candidate(symbol_filters, side, reduce_only, price, quantity_to_trade)
    if not order_candidate["ok"]:
        return None

    return SideQuote(
        side=side,
        price=float(order_candidate["rounded_price"]),
        quantity=float(order_candidate["rounded_quantity"]),
        reduce_only=reduce_only,
        order_notional=order_candidate["order_notional"],
    )


def build_quote_set(state, symbol_filters, runtime):
    """Build the desired two-sided quote set from in-memory state only.

    Pure float math off immutable snapshots (hot path): the Vol+OBI quote,
    position-limit suppression, GTX post-only clamping, and per-side
    validation against exchange filters.
    """
    calc = state.vol_obi_calc
    mid_price = state.mid_price
    if calc is None or mid_price is None or mid_price <= 0.0:
        return None, {"reason": "missing_price"}

    if not is_vol_obi_live(state, runtime):
        return None, {
            "reason": "vol_obi_unavailable",
            "sample_count": state.vol_obi_snapshot.sample_count,
        }

    max_pos = compute_max_position_usd(state)
    if max_pos <= 0.0:
        return None, {"reason": "no_position_capacity"}
    calc.set_max_position_dollar(max_pos)

    bid_price, ask_price = calc.quote(mid_price, state.position_size)
    if bid_price is None or ask_price is None:
        return None, {"reason": "crossed_or_warming_quote"}

    # Hard position limit: suppress the side that would add exposure and
    # flag the surviving side reduce-only so inventory can only shrink.
    position_usd = abs(state.position_size) * mid_price
    reduce_only_side = None
    if position_usd >= max_pos:
        if state.position_size > 0:
            bid_price = None
            reduce_only_side = 'SELL'
        elif state.position_size < 0:
            ask_price = None
            reduce_only_side = 'BUY'

    # Opening circuit breaker: pause exposure-adding quotes, keep reducing.
    if is_opening_circuit_breaker_active(state, runtime):
        close_side = get_position_close_side(state.position_size)
        if close_side == 'SELL':
            bid_price = None
        elif close_side == 'BUY':
            ask_price = None
        else:
            return None, {"reason": "opening_circuit_breaker"}

    # GTX clamp: a post-only order is rejected when it would cross the
    # OPPOSITE side of the book (bid >= best ask / ask <= best bid), which
    # happens when alpha pushes fair through the touch. Quoting inside the
    # spread is legal and stays untouched.
    tick_size = float(symbol_filters.get('tick_size', 0.0) or 0.0)
    best_bid = state.bid_price
    best_ask = state.ask_price
    if bid_price is not None and best_ask is not None and bid_price >= best_ask:
        bid_price = best_ask - tick_size if tick_size > 0.0 else None
    if ask_price is not None and best_bid is not None and ask_price <= best_bid:
        ask_price = best_bid + tick_size if tick_size > 0.0 else None
    if bid_price is not None and ask_price is not None and bid_price >= ask_price:
        # Clamping collapsed the spread: drop the side fair price pushed
        # through the book and keep the passive one.
        if calc.alpha >= 0.0:
            bid_price = None
        else:
            ask_price = None

    bid_quote = _build_side_quote(
        state, symbol_filters, 'BUY', bid_price, mid_price,
        reduce_only=(reduce_only_side == 'BUY'),
    )
    ask_quote = _build_side_quote(
        state, symbol_filters, 'SELL', ask_price, mid_price,
        reduce_only=(reduce_only_side == 'SELL'),
    )
    if bid_quote is None and ask_quote is None:
        return None, {"reason": "no_valid_sides"}

    command = QuoteSetCommand(kind="quote_set", bid=bid_quote, ask=ask_quote, trigger="price")
    return command, {"reason": "ok"}


def classify_order_update(order_data):
    """Classify an order update into terminal/non-terminal and fill/non-fill outcomes."""
    status = order_data.get('X', order_data.get('status'))
    filled_qty = float(order_data.get('z', order_data.get('executedQty', 0.0)) or 0.0)

    if status == 'PARTIALLY_FILLED':
        return {
            "is_terminal": False,
            "treat_as_fill": False,
            "status": status,
            "filled_qty": filled_qty,
        }

    if status == 'FILLED':
        return {
            "is_terminal": True,
            "treat_as_fill": filled_qty > 0,
            "status": status,
            "filled_qty": filled_qty,
        }

    if status in {'CANCELED', 'REJECTED', 'EXPIRED'}:
        return {
            "is_terminal": True,
            "treat_as_fill": filled_qty > 0,
            "status": status,
            "filled_qty": filled_qty,
        }

    return {
        "is_terminal": False,
        "treat_as_fill": False,
        "status": status,
        "filled_qty": filled_qty,
    }


def is_order_reduce_only(order_data):
    """Normalize the exchange reduce-only flag to a boolean."""
    raw_value = order_data.get('R', order_data.get('reduceOnly', False))
    if isinstance(raw_value, str):
        return raw_value.lower() == 'true'
    return bool(raw_value)


async def wait_for_terminal_order_update(order_updates, order_id, timeout, log, context):
    """Wait for a terminal update for the given order id."""
    start_time = asyncio.get_event_loop().time()

    while True:
        remaining_timeout = timeout - (asyncio.get_event_loop().time() - start_time)
        if remaining_timeout <= 0:
            raise asyncio.TimeoutError

        update = await asyncio.wait_for(order_updates.get(), timeout=remaining_timeout)
        if update.get('e') != 'ORDER_TRADE_UPDATE':
            continue

        order_data = update.get('o', {})
        if order_data.get('i') != order_id:
            continue

        terminal_update = classify_order_update(order_data)
        if not terminal_update["is_terminal"]:
            continue

        status = terminal_update["status"]
        filled_qty = terminal_update["filled_qty"]

        log.info(f"{context} order {order_id} reached final state {status}. Filled: {filled_qty}")

        return terminal_update


async def cancel_and_finalize_side_order(state, client, symbol, log, side, reason, order_label):
    """Cancel one side's tracked order and wait for a terminal state before proceeding."""
    side_state = state.side_orders[side]
    if side_state.order_id is None:
        return True

    order_id = side_state.order_id
    position_update_seq_before_fill = state.position_update_seq
    if not await cancel_side_order(
        state,
        client,
        symbol,
        log,
        side,
        reason,
        clear_tracking_on_success=False,
    ):
        return False

    try:
        terminal_update = await wait_for_terminal_order_update(
            state.order_updates,
            order_id,
            CANCEL_CONFIRM_TIMEOUT,
            log,
            f"{order_label} cancel confirmation",
        )
    except asyncio.TimeoutError:
        log.warning(
            f"{order_label} {order_id}: no terminal user-data update after cancel within "
            f"{CANCEL_CONFIRM_TIMEOUT:.1f}s; checking REST order status."
        )
        try:
            order_data = await client.get_order_status(symbol, order_id)
        except Exception as rest_error:
            log.error(f"{order_label} {order_id}: failed to confirm terminal state via REST: {rest_error}")
            return False

        terminal_update = classify_order_update(order_data)
        if not terminal_update["is_terminal"]:
            log.warning(
                f"{order_label} {order_id}: order still reports non-terminal status "
                f"{terminal_update['status']} after cancel. Keeping tracking and pausing."
            )
            return False

        log.info(
            f"{order_label} {order_id}: terminal status {terminal_update['status']} confirmed via REST."
        )

    await handle_terminal_order_update(
        state,
        client,
        symbol,
        log,
        side,
        order_id,
        terminal_update,
        position_update_seq_before_fill,
        order_label,
    )
    return True


async def handle_terminal_order_update(
    state,
    client,
    symbol,
    log,
    side,
    order_id,
    terminal_update,
    position_update_seq_before_fill,
    order_label,
):
    """Finalize a tracked order after it reaches a terminal exchange status."""
    filled_qty = terminal_update["filled_qty"]
    if terminal_update["treat_as_fill"]:
        log.info(f"{order_label} {order_id} filled! Quantity: {filled_qty}")
        synced_via_ws = await reconcile_fill_with_position(
            state,
            client,
            symbol,
            log,
            position_update_seq_before_fill,
            f"{order_label} {order_id}",
        )
        sync_source = "WebSocket" if synced_via_ws else "REST fallback"
        log.info(f"{side or 'UNKNOWN'} fill reconciled via {sync_source}: tracked position size {state.position_size:.6f}")
    else:
        log.info(f"{order_label} {order_id} ended as {terminal_update['status']} without an executed fill.")

    tracked_side = get_tracked_side_for_order(state, order_id)
    if tracked_side is not None:
        clear_side_order(state, tracked_side)
    log.debug(f"Adding 0.01s delay after {order_label.lower()} terminal update")
    await asyncio.sleep(0.01)


async def reconcile_stale_pending_terminal_orders(state, client, symbol, runtime, log):
    """Resolve old canceled/replaced orders asynchronously so fast replace stays off the hot path."""
    stale_order_ids = [
        order_id
        for order_id, pending in state.pending_terminal_orders.items()
        if runtime.now() - pending.cancel_requested_at >= CANCEL_CONFIRM_TIMEOUT
    ]
    for order_id in stale_order_ids:
        pending = state.pending_terminal_orders.get(order_id)
        if pending is None:
            continue

        try:
            order_data = await client.get_order_status(symbol, order_id)
        except Exception as rest_error:
            log.error(f"{pending.order_label} {order_id}: failed to confirm terminal state via REST: {rest_error}")
            continue

        terminal_update = classify_order_update(order_data)
        if not terminal_update["is_terminal"]:
            log.warning(
                f"{pending.order_label} {order_id}: still non-terminal after async REST confirmation "
                f"({terminal_update['status']}). Leaving it for the watchdog."
            )
            continue

        await handle_terminal_order_update(
            state,
            client,
            symbol,
            log,
            pending.side,
            order_id,
            terminal_update,
            pending.position_update_seq_before_fill,
            pending.order_label,
        )
        state.pending_terminal_orders.pop(order_id, None)
        request_quote_refresh(state)


async def place_side_order(state, client, symbol, runtime, log, quote, symbol_filters=None):
    """Submit one side's desired order and update local tracking.

    Pacing happens once per reconcile burst in apply_quote_set, not per
    placement, so the two sides of a quote set go out back-to-back on the
    same price snapshot.
    """
    percentage_diff = 0.0
    if state.mid_price:
        percentage_diff = (quote.price - state.mid_price) / state.mid_price * 100

    filters = symbol_filters or state.symbol_filters or {}
    price_precision = int(filters.get("price_precision", 8))
    quantity_precision = int(filters.get("quantity_precision", 8))
    formatted_price = f"{quote.price:.{price_precision}f}"
    formatted_quantity = f"{quote.quantity:.{quantity_precision}f}"

    log.info(
        f"Placing {quote.side} order: {formatted_quantity} {symbol} @ {formatted_price} "
        f"({percentage_diff:+.4f}% from mid-price, {vol_obi_status_text(state)})"
    )

    placed_order = await client.place_order(
        symbol,
        formatted_price,
        formatted_quantity,
        quote.side,
        quote.reduce_only,
    )
    runtime.last_order_time = runtime.now()
    state.side_orders[quote.side] = SideOrderState(
        order_id=placed_order.get('orderId'),
        price=quote.price,
        quantity=quote.quantity,
        reduce_only=quote.reduce_only,
        placed_at=runtime.last_order_time,
    )
    if not quote.reduce_only:
        reset_opening_order_failures(state)
    log.info(f"{quote.side} order placed successfully: ID={state.side_orders[quote.side].order_id}")
    return placed_order


async def cancel_all_side_orders(state, client, symbol, runtime, log, reason):
    """Cancel every working order with one REST call and park them as pending."""
    if not has_live_orders(state):
        return True

    try:
        await client.cancel_all_orders(symbol)
    except Exception as cancel_error:
        log.error(f"{reason}: failed to cancel all working orders: {cancel_error}")
        return False

    for side, side_state in state.side_orders.items():
        if side_state.order_id is not None:
            state.pending_terminal_orders[side_state.order_id] = PendingTerminalOrder(
                side=side,
                reduce_only=side_state.reduce_only,
                position_update_seq_before_fill=state.position_update_seq,
                order_label="Cancelled quote",
                cancel_requested_at=runtime.now(),
            )
            clear_side_order(state, side)
    log.info(f"{reason}: cancelled all working orders.")
    return True


def _side_needs_replace(state, side, quote):
    """Return True when the live order on a side no longer matches the desired quote."""
    side_state = state.side_orders[side]
    if side_state.order_id is None:
        return False
    if quote is None:
        return True
    if side_state.reduce_only != quote.reduce_only:
        return True
    return not should_reuse_side(side_state, quote.price)


async def apply_quote_set(state, client, symbol, runtime, log, executor, command):
    """Reconcile the desired two-sided quote set with the live exchange orders.

    Cancels run before placements so a large fair-price jump can never leave
    a new bid resting above our own still-live old ask. Pacing is per
    reconcile burst (not per order) so both sides go out on the same prices.
    """
    if command.kind == "cancel_all":
        if not await cancel_all_side_orders(state, client, symbol, runtime, log, command.trigger or "Cancel all"):
            await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
        return

    if command.kind != "quote_set":
        return

    wait_time = MIN_ORDER_INTERVAL - (runtime.now() - runtime.last_order_time)
    if wait_time > 0:
        log.debug(f"Pacing: waiting {wait_time:.3f}s before the next quote reconcile")
        await asyncio.sleep(wait_time)
        # Never act on stale prices after a pacing wait: take the freshest plan.
        command = drain_latest_order_command(state, command)
        if command.kind == "cancel_all":
            if not await cancel_all_side_orders(state, client, symbol, runtime, log, command.trigger or "Cancel all"):
                await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
            return
        if command.kind != "quote_set":
            return

    desired = {'BUY': command.bid, 'SELL': command.ask}

    # Pass 1: cancel sides that disappeared or moved beyond the reuse threshold.
    for side, quote in desired.items():
        if not _side_needs_replace(state, side, quote):
            continue

        if executor.fast_replace:
            side_state = state.side_orders[side]
            order_id = side_state.order_id
            if not await cancel_side_order(
                state,
                client,
                symbol,
                log,
                side,
                command.trigger or "Fast requote replacement",
                clear_tracking_on_success=False,
            ):
                await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                return

            if order_id is not None:
                state.pending_terminal_orders[order_id] = PendingTerminalOrder(
                    side=side,
                    reduce_only=side_state.reduce_only,
                    position_update_seq_before_fill=state.position_update_seq,
                    order_label="Fast requote order",
                    cancel_requested_at=runtime.now(),
                )
            clear_side_order(state, side)
        else:
            if not await cancel_and_finalize_side_order(
                state,
                client,
                symbol,
                log,
                side,
                command.trigger or "Requote replacement",
                "Requote order",
            ):
                await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                return

    if runtime.shutdown_requested:
        return

    # Pass 2: place sides that are wanted but not resting (reused sides skip).
    for side, quote in desired.items():
        if quote is None or state.side_orders[side].order_id is not None:
            continue

        try:
            await executor.place_order(state, client, symbol, runtime, log, quote)
        except Exception:
            if not quote.reduce_only:
                record_opening_order_failure(state, runtime)
            raise


def _oldest_live_order_age(state, runtime):
    """Return the age in seconds of the oldest live resting order, or None."""
    placed_times = [
        side_state.placed_at
        for side_state in state.side_orders.values()
        if side_state.order_id is not None and side_state.placed_at is not None
    ]
    if not placed_times:
        return None
    return runtime.now() - min(placed_times)


async def _handle_tracked_terminal_update(state, client, symbol, runtime, log, received_update):
    """Process an ORDER_TRADE_UPDATE against live and pending tracked orders.

    Returns True when a terminal update for a tracked order was finalized.
    """
    if received_update.get('e') != 'ORDER_TRADE_UPDATE':
        return False

    order_data = received_update.get('o', {})
    order_id = order_data.get('i')

    tracked_side = get_tracked_side_for_order(state, order_id)
    pending_order = state.pending_terminal_orders.get(order_id) if tracked_side is None else None
    if tracked_side is None and pending_order is None:
        return False

    terminal_update = classify_order_update(order_data)
    if not terminal_update["is_terminal"]:
        return False

    order_was_opening = not is_order_reduce_only(order_data)
    if terminal_update["status"] == "REJECTED" and order_was_opening:
        record_opening_order_failure(state, runtime)
    elif terminal_update["status"] == "FILLED" and order_was_opening:
        reset_opening_order_failures(state)

    if pending_order is not None:
        state.pending_terminal_orders.pop(order_id, None)
        side = pending_order.side
        seq_before_fill = pending_order.position_update_seq_before_fill
        order_label = pending_order.order_label
    else:
        side = tracked_side
        seq_before_fill = state.position_update_seq
        order_label = "Order"

    await handle_terminal_order_update(
        state,
        client,
        symbol,
        log,
        side,
        order_id,
        terminal_update,
        seq_before_fill,
        order_label,
    )
    request_quote_refresh(state)
    return True


async def order_manager_loop_impl(state, client, symbol, runtime, executor):
    """Own the working exchange orders (both sides) and react to quote intents."""
    log = logging.getLogger('OrderManager')

    while not runtime.shutdown_requested:
        try:
            await reconcile_stale_pending_terminal_orders(state, client, symbol, runtime, log)

            if not has_live_orders(state):
                if state.pending_terminal_orders:
                    order_update_task = asyncio.create_task(state.order_updates.get())
                    command_task = asyncio.create_task(state.order_commands.get())
                    done, pending = await asyncio.wait(
                        {order_update_task, command_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)

                    if order_update_task in done:
                        await _handle_tracked_terminal_update(
                            state, client, symbol, runtime, log, order_update_task.result()
                        )
                        continue

                    command = command_task.result()
                else:
                    command = await state.order_commands.get()
                command = drain_latest_order_command(state, command)
                await apply_quote_set(state, client, symbol, runtime, log, executor, command)
                continue

            oldest_age = _oldest_live_order_age(state, runtime)
            remaining_timeout = max(0.0, ORDER_REFRESH_INTERVAL - (oldest_age or 0.0))

            order_update_task = asyncio.create_task(state.order_updates.get())
            command_task = asyncio.create_task(state.order_commands.get())
            timeout_task = asyncio.create_task(asyncio.sleep(remaining_timeout))

            done, pending = await asyncio.wait(
                {order_update_task, command_task, timeout_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            received_update = order_update_task.result() if order_update_task in done else None
            received_command = command_task.result() if command_task in done else None
            timed_out = timeout_task in done
            action_taken = False

            if received_update is not None:
                action_taken = await _handle_tracked_terminal_update(
                    state, client, symbol, runtime, log, received_update
                )

            if not action_taken and timed_out:
                expired_sides = [
                    side
                    for side, side_state in state.side_orders.items()
                    if side_state.order_id is not None
                    and side_state.placed_at is not None
                    and runtime.now() - side_state.placed_at >= ORDER_REFRESH_INTERVAL
                ]
                refresh_failed = False
                for side in expired_sides:
                    log.info(
                        f"{side} order {state.side_orders[side].order_id} reached the "
                        f"{ORDER_REFRESH_INTERVAL:.1f}s safety lifetime. Refreshing the quote."
                    )
                    if not await cancel_and_finalize_side_order(
                        state,
                        client,
                        symbol,
                        log,
                        side,
                        "Timed-out order refresh",
                        "Timed-out order",
                    ):
                        refresh_failed = True
                        break

                if refresh_failed:
                    await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                    continue

                request_quote_refresh(state)
                action_taken = True

            if not action_taken and received_command is not None:
                command = drain_latest_order_command(state, received_command)
                await apply_quote_set(state, client, symbol, runtime, log, executor, command)
                action_taken = True

            if not action_taken and received_update is not None:
                continue

        except asyncio.CancelledError:
            log.info("Order manager cancelled.")
            break
        except Exception as exc:
            log.error(f"An error occurred in the order manager: {exc}", exc_info=True)
            for side in list(state.side_orders.keys()):
                if state.side_orders[side].order_id is not None:
                    await cancel_side_order(state, client, symbol, log, side, "Order manager error")
            await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)


async def order_manager_loop(state, client, symbol, runtime, executor=None):
    """Compatibility wrapper around the explicit OrderExecutor component."""
    executor = executor or OrderExecutor()
    return await order_manager_loop_impl(state, client, symbol, runtime, executor)



async def market_making_loop(state, client, symbol, runtime, quote_engine=None):
    """Compatibility wrapper around the explicit QuoteEngine component."""
    quote_engine = quote_engine or QuoteEngine()
    return await quote_engine.run(state, client, symbol, runtime)



async def fetch_initial_balance(state, client, runtime):
    """Fetch initial account balance via REST API."""
    log = logging.getLogger('InitialBalance')

    try:
        log.info("Fetching initial account balance...")
        account_info = await client.signed_request("GET", "/fapi/v3/account", {})
        balances = account_info.get('assets', [])

        for balance in balances:
            asset = balance.get('asset', '')
            wallet_balance = float(balance.get('walletBalance', '0'))

            if asset == 'USDF':
                state.usdf_balance = wallet_balance
                log.info(f"Initial USDF balance: {wallet_balance}")
            elif asset == 'USDT':
                state.usdt_balance = wallet_balance
                log.info(f"Initial USDT balance: {wallet_balance}")
            elif asset == 'USDC':
                state.usdc_balance = wallet_balance
                log.info(f"Initial USDC balance: {wallet_balance}")

        # Calculate total balance
        state.account_balance = state.usdf_balance + state.usdt_balance + state.usdc_balance
        state.balance_last_updated = runtime.now()

        log.info(f"Initial balance loaded: USDF={state.usdf_balance:.4f}, USDT={state.usdt_balance:.4f}, USDC={state.usdc_balance:.4f}, Total=${state.account_balance:.4f}")
        return True

    except Exception as e:
        log.error(f"Failed to fetch initial balance: {e}", exc_info=True)
        return False


async def ensure_clean_startup(client, symbol, timeout=STARTUP_CLEANUP_TIMEOUT):
    """Cancel all open orders for the symbol before trading starts."""
    log = logging.getLogger('StartupCleanup')
    log.info(f"Sending initial cancel all orders for {symbol} to ensure a clean slate.")
    try:
        await asyncio.wait_for(client.cancel_all_orders(symbol), timeout=timeout)
    except asyncio.TimeoutError:
        log.error(
            f"Initial cancel-all for {symbol} timed out after {timeout:.1f}s. "
            "Aborting startup to avoid trading on top of stale orders."
        )
        return False
    except Exception as exc:
        log.error(
            f"Initial cancel-all for {symbol} failed: {exc}. "
            "Aborting startup to avoid trading on top of stale orders."
        )
        return False

    log.info(f"Initial cancel-all for {symbol} completed successfully.")
    return True


async def wait_for_active_order_clear(state, timeout):
    """Wait for local working-order tracking to clear on both sides during shutdown."""
    if state is None or not has_live_orders(state):
        return True

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while has_live_orders(state) and loop.time() < deadline:
        await asyncio.sleep(0.05)
    return not has_live_orders(state)


async def initiate_graceful_order_shutdown(state, runtime, timeout=SHUTDOWN_ACTIVE_ORDER_GRACE_TIMEOUT):
    """Ask the running order manager to cancel the working orders before task teardown."""
    if state is None:
        return True

    runtime.request_shutdown()
    if not has_live_orders(state):
        return True

    live_ids = [
        side_state.order_id
        for side_state in state.side_orders.values()
        if side_state.order_id is not None
    ]
    logging.info(f"Requesting graceful shutdown cancel for working orders {live_ids}.")
    publish_latest_order_command(state, QuoteSetCommand(kind="cancel_all", trigger="Shutdown cleanup"))
    request_quote_refresh(state)
    cleared = await wait_for_active_order_clear(state, timeout)
    if cleared:
        logging.info("Working orders cleared before task shutdown.")
    else:
        logging.warning(
            f"Working orders did not clear within {timeout:.1f}s. "
            "Falling back to direct cancel-all cleanup."
        )
    return cleared


async def cleanup_orders(
    symbol,
    api_user,
    api_signer,
    api_private_key,
    existing_client=None,
    timeout=SHUTDOWN_CANCEL_ALL_TIMEOUT,
):
    """Best-effort final cleanup that cancels any submitted orders before exit."""
    log = logging.getLogger("ShutdownCleanup")

    async def _cancel_with(client_obj):
        await asyncio.wait_for(client_obj.cancel_all_orders(symbol), timeout=timeout)

    for attempt in range(1, SHUTDOWN_CANCEL_ALL_RETRIES + 1):
        try:
            log.info(f"Shutdown cleanup attempt {attempt}/{SHUTDOWN_CANCEL_ALL_RETRIES}: cancelling all orders for {symbol}.")

            if existing_client is not None and getattr(existing_client, "session", None) is not None and not existing_client.session.closed:
                await _cancel_with(existing_client)
            else:
                async with ApiClient(api_user, api_signer, api_private_key, RELEASE_MODE) as cleanup_client:
                    await _cancel_with(cleanup_client)

            log.info("All open orders cancelled. Shutdown complete.")
            return True
        except Exception as exc:
            log.error(f"Shutdown cleanup attempt {attempt} failed: {exc}")
            if attempt < SHUTDOWN_CANCEL_ALL_RETRIES:
                await asyncio.sleep(0.5)

    return False

def build_signal_handler(runtime):
    """Build a signal handler bound to the active runtime context."""
    def signal_handler(signum, frame):
        logging.info(f"Signal {signum} received, initiating shutdown...")
        runtime.request_shutdown()

    return signal_handler

async def main():
    parser = argparse.ArgumentParser(description="A market making bot for Aster Finance.")
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="The symbol to trade. Defaults to SYMBOL in runtime.env.",
    )
    args = parser.parse_args()

    setup_logging("INFO")
    load_project_env()
    args.symbol = resolve_symbol(args.symbol)
    runtime = RuntimeContext(args.symbol)

    logging.info(f"Starting market maker with arguments: {args}")

    API_USER = os.getenv("API_USER")
    API_SIGNER = os.getenv("API_SIGNER")
    API_PRIVATE_KEY = os.getenv("API_PRIVATE_KEY")

    # Set up signal handlers (SIGTERM not available on Windows, use SIGINT as fallback)
    signal_handler = build_signal_handler(runtime)
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)

    client = None
    state = None
    tasks = []
    core_tasks = []
    cleanup_completed = False

    try:
        client = ApiClient(API_USER, API_SIGNER, API_PRIVATE_KEY, RELEASE_MODE)
        state = StrategyState()
        quote_engine = QuoteEngine()
        order_executor = OrderExecutor()
        top_of_book_feed = AsterTopOfBookFeed()
        alpha_engine = BinanceAlphaEngine()

        async with client:
            try:
                if not await ensure_clean_startup(client, args.symbol):
                    return

                # [IMPROVED] Fetch initial account balance with a timeout
                logging.info("Fetching initial account balance...")
                try:
                    balance_success = await asyncio.wait_for(fetch_initial_balance(state, client, runtime), timeout=20.0)
                    if not balance_success:
                        logging.error("Failed to fetch initial balance. Cannot proceed.")
                        return
                except asyncio.TimeoutError:
                    logging.error("Timed out while fetching initial balance. Cannot proceed.")
                    return

                # The Vol+OBI calculator needs the Aster tick size before the
                # Binance feed starts pumping samples into it.
                logging.info(f"Fetching trading rules for {args.symbol}...")
                state.symbol_filters = await client.get_symbol_filters(args.symbol)
                logging.info(f"Filters loaded: {state.symbol_filters}")
                state.vol_obi_calc = VolObiCalculator(
                    tick_size=float(state.symbol_filters["tick_size"]),
                    window_steps=OBI_WINDOW_STEPS,
                    step_ns=OBI_STEP_NS,
                    vol_to_half_spread=OBI_VOL_TO_HALF_SPREAD,
                    min_half_spread_bps=OBI_MIN_HALF_SPREAD_BPS,
                    c1_ticks=OBI_C1_TICKS,
                    skew=OBI_SKEW,
                    looking_depth=OBI_LOOKING_DEPTH,
                    min_warmup_samples=OBI_MIN_WARMUP_SAMPLES,
                    max_position_dollar=0.0,  # set dynamically each quote cycle
                )

                support_tasks = [
                    asyncio.create_task(
                        websocket_price_updater(
                            state,
                            args.symbol,
                            runtime,
                            top_of_book_feed=top_of_book_feed,
                            quote_engine=quote_engine,
                        )
                    ),
                    asyncio.create_task(websocket_user_data_updater(state, client, args.symbol, runtime)),
                    asyncio.create_task(
                        binance_orderbook_imbalance_updater(
                            state,
                            args.symbol,
                            runtime,
                            alpha_engine=alpha_engine,
                        )
                    ),
                ]
                tasks.extend(support_tasks)

                if not await wait_for_startup_inputs(state, args.symbol, runtime):
                    logging.info("Shutdown requested before required startup inputs became available.")
                    return

                try:
                    logging.info(f"Checking for existing position for {args.symbol}...")
                    positions = await client.get_position_risk(args.symbol)
                    logging.debug(f"Position risk response: {positions}")

                    position_found = False
                    if positions:
                        position_size, notional_value = sync_state_from_position_data(
                            state,
                            positions[0],
                            reference_price=state.mid_price,
                        )

                        if has_open_position(state):
                            position_side = "LONG" if position_size > 0 else "SHORT"
                            logging.info(
                                f"Found existing {position_side} position of size {position_size} with notional value "
                                f"${notional_value:.2f}. Inventory skew will work it off."
                            )
                            position_found = True

                    if not position_found:
                        logging.info("No existing position found.")
                        try:
                            logging.info(f"Attempting to set leverage for {args.symbol} to {DEFAULT_LEVERAGE}x.")
                            await client.change_leverage(args.symbol, DEFAULT_LEVERAGE)
                            logging.info(f"Successfully set leverage for {args.symbol} to {DEFAULT_LEVERAGE}x.")
                        except Exception as e:
                            logging.error(f"Failed to set leverage: {e}", exc_info=True)

                except Exception as e:
                    logging.warning(f"Could not check for existing position or set leverage: {e}", exc_info=True)

                # Start all async tasks
                quote_task = asyncio.create_task(market_making_loop(state, client, args.symbol, runtime, quote_engine=quote_engine))
                order_task = asyncio.create_task(order_manager_loop(state, client, args.symbol, runtime, executor=order_executor))
                watchdog_task = asyncio.create_task(order_executor.watch_open_orders(state, client, args.symbol, runtime))
                core_tasks = [quote_task, order_task]
                tasks.extend([
                    asyncio.create_task(balance_reporter(state, runtime)),
                    quote_task,
                    order_task,
                    watchdog_task,
                    asyncio.create_task(price_reporter(state, args.symbol, runtime)),
                ])

                request_quote_refresh(state)

                # Wait for either a core trading task to complete or a shutdown signal
                while not runtime.shutdown_requested and not any(task.done() for task in core_tasks):
                    await asyncio.sleep(0.01)
            finally:
                logging.info("Shutdown initiated. Cleaning up...")
                runtime.request_shutdown()
                try:
                    await initiate_graceful_order_shutdown(state, runtime)
                except Exception as shutdown_exc:
                    logging.error(f"Graceful shutdown pre-cancel failed: {shutdown_exc}", exc_info=True)

                for task in tasks:
                    if not task.done():
                        task.cancel()

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

                await asyncio.shield(
                    cleanup_orders(
                        args.symbol,
                        API_USER,
                        API_SIGNER,
                        API_PRIVATE_KEY,
                        existing_client=client,
                    )
                )
                cleanup_completed = True

    except asyncio.CancelledError:
        logging.info("Main task was cancelled.")
    except Exception as e:
        logging.error(f"An unhandled exception occurred in main: {e}", exc_info=True)
    finally:
        if not cleanup_completed:
            logging.info("Shutdown initiated. Cleaning up...")
            runtime.request_shutdown()
            await asyncio.shield(cleanup_orders(args.symbol, API_USER, API_SIGNER, API_PRIVATE_KEY))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown requested by user (Ctrl+C).")
