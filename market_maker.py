import os
import asyncio
import argparse
import logging
import websockets
import json
import signal
import time
import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Optional
import numpy as np
import requests
from api_client import ApiClient
from utils import configured_symbol, load_project_env, normalize_symbol_base

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
FLIP_MODE = False # True for short-biased (SELL first), False for long-biased (BUY first)
DEFAULT_BUY_SPREAD = 0.006   # 0.6% below mid-price for buy orders
DEFAULT_SELL_SPREAD = 0.006  # 0.6% above mid-price for sell orders
USE_AVELLANEDA_SPREADS = True  # Toggle to pull spreads from Avellaneda parameter files
DEFAULT_LEVERAGE = 1
DEFAULT_BALANCE_FRACTION = 0.2  # Use a fraction of tracked wallet balance for each order
POSITION_THRESHOLD_USD = 15.0  # USD threshold before a position is treated as significant inventory

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

# SUPERTREND INTEGRATION
USE_SUPERTREND_SIGNAL = True  # Toggle to use Supertrend signal for dynamic flip_mode
SUPERTREND_PARAMS_TEMPLATE = "supertrend_params_{}.json"
SUPERTREND_CHECK_INTERVAL = 600 # Seconds between checking the signal file
USE_BINANCE_OBI_ALPHA = True
BINANCE_OBI_LOOKING_DEPTH_PCT = 0.025
BINANCE_OBI_BOOK_RETAIN_PCT = 0.03
BINANCE_OBI_ZSCORE_WINDOW_SECONDS = 600
BINANCE_OBI_WARMUP_SECONDS = 300
BINANCE_OBI_BUFFER_CAPACITY = 8192
BINANCE_OBI_MIN_SAMPLES = 100
BINANCE_OBI_STALE_TIMEOUT_SECONDS = 5.0
BINANCE_OBI_BPS_PER_SIGMA = 5.0
BINANCE_OBI_MAX_SHIFT_BPS = 15.0
BINANCE_OBI_SHIFT_LOG_DELTA_BPS = 1.0
BINANCE_OBI_TRIM_INTERVAL_SECONDS = 1.0
BINANCE_OBI_TRIM_INTERVAL_UPDATES = 10
BINANCE_OBI_BAND_REBUILD_BPS = 1.0

# ORDER CANCELLATION
CANCEL_SPECIFIC_ORDER = True # If True, cancel specific order ID. If False, cancel all orders for the symbol.
ORDER_REPLACE_MODE = os.getenv("ORDER_REPLACE_MODE", "fast").strip().lower()
FAST_ORDER_REPLACE = ORDER_REPLACE_MODE == "fast"
OPEN_ORDER_WATCHDOG_INTERVAL = 15.0
OPEN_ORDER_WATCHDOG_CANCEL_ALL = True
QUOTE_REFRESH_PREFILTER_BPS = 2.0

# LOGGING
LOG_FILE = 'market_maker.log'
RELEASE_MODE = env_flag("RELEASE_MODE", True)  # When True, suppress all non-error logs and prints

MIN_ORDER_INTERVAL = 1.0  # Minimum seconds between order placements
POSITION_SIZE_EPSILON = 1e-12

# Spread configuration
PARAMS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "params")
AVELLANEDA_FILE_PREFIX = "avellaneda_parameters_"
DEFAULT_MIN_AVELLANEDA_SPREAD_BPS = 5.0
DEFAULT_MAX_AVELLANEDA_SPREAD_BPS = 200.0
SPREAD_CACHE_TTL_SECONDS = 10
_SPREAD_CACHE = {}


class BinanceOrderBookSyncError(Exception):
    """Raised when the Binance local book must be resynchronized."""


class RollingZScoreBuffer:
    """Fixed-capacity, time-evicting ring buffer for rolling z-score stats."""

    def __init__(self, capacity: int):
        self.capacity = max(int(capacity), 1)
        self.timestamps_ms = np.zeros(self.capacity, dtype=np.int64)
        self.values = np.zeros(self.capacity, dtype=np.float64)
        self.head = 0
        self.tail = 0
        self.count = 0
        self.sum = 0.0
        self.sum_sq = 0.0

    def clear(self):
        self.head = 0
        self.tail = 0
        self.count = 0
        self.sum = 0.0
        self.sum_sq = 0.0

    def _drop_oldest(self):
        if self.count == 0:
            return
        value = float(self.values[self.head])
        self.sum -= value
        self.sum_sq -= value * value
        self.head = (self.head + 1) % self.capacity
        self.count -= 1

    def evict_older_than(self, cutoff_ms: int):
        while self.count > 0 and int(self.timestamps_ms[self.head]) < int(cutoff_ms):
            self._drop_oldest()

    def append(self, timestamp_ms: int, value: float):
        if self.count == self.capacity:
            self._drop_oldest()

        self.timestamps_ms[self.tail] = int(timestamp_ms)
        self.values[self.tail] = float(value)
        self.tail = (self.tail + 1) % self.capacity
        self.count += 1
        self.sum += float(value)
        self.sum_sq += float(value) * float(value)

    def mean(self):
        if self.count <= 0:
            return None
        return self.sum / self.count

    def std(self):
        if self.count <= 1:
            return None
        mean = self.sum / self.count
        variance = max((self.sum_sq / self.count) - (mean * mean), 0.0)
        return math.sqrt(variance)

    def span_seconds(self):
        if self.count <= 1:
            return 0.0
        last_index = (self.tail - 1) % self.capacity
        return max(0.0, (int(self.timestamps_ms[last_index]) - int(self.timestamps_ms[self.head])) / 1000.0)


@dataclass(frozen=True)
class AsterTopOfBookSnapshot:
    bid_price: float
    ask_price: float
    mid_price: float
    updated_at: float


@dataclass(frozen=True)
class BinanceAlphaSnapshot:
    ready: bool = False
    raw_imbalance: Optional[float] = None
    zscore: Optional[float] = None
    shift_bps: float = 0.0
    warmup_seconds: float = 0.0
    sample_count: int = 0
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_updated: Optional[float] = None
    ws_connected: bool = False


@dataclass(frozen=True)
class PreparedQuoteParams:
    kind: str
    source: str
    buy_spread: float = 0.0
    sell_spread: float = 0.0
    gamma: float = 0.0
    sigma: float = 0.0
    k_buy: float = 0.0
    k_sell: float = 0.0
    time_horizon_days: float = 0.0
    spread_limits_min_bps: float = DEFAULT_MIN_AVELLANEDA_SPREAD_BPS
    spread_limits_max_bps: float = DEFAULT_MAX_AVELLANEDA_SPREAD_BPS


@dataclass(frozen=True)
class PendingTerminalOrder:
    side: str
    reduce_only: bool
    position_update_seq_before_fill: int
    order_label: str
    cancel_requested_at: float


@dataclass(frozen=True)
class OrderCommand:
    """The latest desired action for the order manager."""
    kind: str
    side: str = ""
    reduce_only: bool = False
    price: float = 0.0
    quantity: float = 0.0
    formatted_price: str = ""
    formatted_quantity: str = ""
    order_notional: float = 0.0
    trigger: str = ""


def get_unavailable_quote_params():
    """Return a sentinel payload meaning the bot must not quote yet."""
    return {"source": "unavailable"}


def get_unavailable_prepared_quote_params():
    """Return the immutable unavailable quote snapshot used by the hot path."""
    return PreparedQuoteParams(kind="unavailable", source="unavailable")


def prepare_quote_params_snapshot(params):
    """Validate and normalize quote params once so the hot path stays memory-only."""
    if not isinstance(params, dict):
        return get_unavailable_prepared_quote_params()

    source = str(params.get("source") or "unavailable")
    if source == "unavailable":
        return get_unavailable_prepared_quote_params()

    if source == "default":
        buy_spread = _safe_float(params.get("buy_spread"))
        sell_spread = _safe_float(params.get("sell_spread"))
        if buy_spread is None or sell_spread is None or buy_spread <= 0.0 or sell_spread <= 0.0:
            return get_unavailable_prepared_quote_params()
        return PreparedQuoteParams(
            kind="default",
            source=source,
            buy_spread=buy_spread,
            sell_spread=sell_spread,
        )

    spread_limits = resolve_avellaneda_spread_limits_bps(params)
    gamma = _safe_float(params.get("gamma"))
    sigma = _safe_float(params.get("sigma"))
    k_buy = _safe_float(params.get("k_buy"))
    k_sell = _safe_float(params.get("k_sell"))
    time_horizon_days = _safe_float(params.get("time_horizon_days"))
    required_positive = (gamma, sigma, k_buy, k_sell, time_horizon_days)
    if any(value is None or value <= 0.0 for value in required_positive):
        return get_unavailable_prepared_quote_params()

    return PreparedQuoteParams(
        kind="avellaneda",
        source=source,
        gamma=gamma,
        sigma=sigma,
        k_buy=k_buy,
        k_sell=k_sell,
        time_horizon_days=time_horizon_days,
        spread_limits_min_bps=spread_limits["min"],
        spread_limits_max_bps=spread_limits["max"],
    )


def publish_binance_alpha_snapshot(state):
    """Mirror mutable Binance alpha fields into a single immutable snapshot."""
    state.binance_alpha_snapshot = BinanceAlphaSnapshot(
        ready=bool(state.binance_alpha_ready),
        raw_imbalance=state.binance_alpha_raw_imbalance,
        zscore=state.binance_alpha_zscore,
        shift_bps=float(state.binance_alpha_shift_bps or 0.0),
        warmup_seconds=float(state.binance_alpha_warmup_seconds or 0.0),
        sample_count=int(state.binance_alpha_buffer.count),
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
    """Configures logging to both console (INFO) and file (specified level)."""
    log_level = getattr(logging, file_log_level.upper(), logging.DEBUG)
    logger = logging.getLogger()  # Get root logger

    if RELEASE_MODE:
        logger.setLevel(logging.ERROR)  # Only errors in release mode
    else:
        logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler - only add if not in release mode or for errors
    if not RELEASE_MODE:
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        # In release mode, only log errors to file
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Console handler - only add if not in release mode or for errors
    if not RELEASE_MODE:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    else:
        # In release mode, only show errors on console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)


class StrategyState:
    """A simple class to hold the shared state of the strategy."""
    def __init__(self, flip_mode=False):
        self.bid_price = None
        self.ask_price = None
        self.mid_price = None
        self.active_order_id = None
        self.position_size = 0.0
        # Mode can be 'BUY' or 'SELL'
        self.flip_mode = flip_mode
        self.mode = 'SELL' if self.flip_mode else 'BUY'
        # Track last order details for reuse logic
        self.last_order_price = None
        self.last_order_side = None
        self.last_order_quantity = None
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
        self.quote_params = (
            {"buy_spread": DEFAULT_BUY_SPREAD, "sell_spread": DEFAULT_SELL_SPREAD, "source": "default"}
            if not USE_AVELLANEDA_SPREADS
            else get_unavailable_quote_params()
        )
        self.prepared_quote_params = PreparedQuoteParams(
            kind="default",
            source="default",
            buy_spread=DEFAULT_BUY_SPREAD,
            sell_spread=DEFAULT_SELL_SPREAD,
        ) if not USE_AVELLANEDA_SPREADS else PreparedQuoteParams(kind="unavailable", source="unavailable")
        # WebSocket connection health flags
        self.price_ws_connected = False
        self.user_data_ws_connected = False
        self.symbol_filters = None
        # Supertrend signal
        self.supertrend_signal = None # Can be 1 (up) or -1 (down)
        # Position snapshots are the source of truth for inventory state
        self.position_update_seq = 0
        self.active_order_started_at = None
        self.order_failure_timestamps = deque()
        self.opening_circuit_breaker_until = 0.0
        # Binance order book imbalance alpha state
        self.binance_bid_book = {}
        self.binance_ask_book = {}
        self.binance_last_update_id = None
        self.binance_alpha_buffer = RollingZScoreBuffer(BINANCE_OBI_BUFFER_CAPACITY)
        self.binance_alpha_ws_connected = False
        self.binance_alpha_ready = False
        self.binance_alpha_last_updated = None
        self.binance_alpha_raw_imbalance = None
        self.binance_alpha_zscore = None
        self.binance_alpha_shift_bps = 0.0
        self.binance_alpha_warmup_seconds = 0.0
        self.binance_best_bid = None
        self.binance_best_ask = None
        self.binance_last_logged_shift_bps = None
        self.binance_alpha_last_status = None
        self.binance_alpha_snapshot = BinanceAlphaSnapshot()
        self.binance_book_last_trim_at = 0.0
        self.binance_book_updates_since_trim = 0
        self.binance_band_mid_price = None
        self.binance_band_lower_bound = None
        self.binance_band_upper_bound = None
        self.binance_band_bid_qty = 0.0
        self.binance_band_ask_qty = 0.0
        self.pending_terminal_orders = {}


def get_strategy_modes(flip_mode):
    """Return the opening and closing order sides for the current bias."""
    opening_mode = 'SELL' if flip_mode else 'BUY'
    closing_mode = 'BUY' if flip_mode else 'SELL'
    return opening_mode, closing_mode


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


def has_significant_position(state, position_notional=None):
    """Return True when inventory is non-zero and large enough to require an explicit close."""
    if position_notional is None:
        position_notional = get_position_notional_usd(state.position_size, state.mid_price)

    return has_open_position(state) and is_position_significant(position_notional)


def get_target_mode(state, position_notional=None):
    """Return the desired trading side given current bias and tracked inventory."""
    opening_mode, _ = get_strategy_modes(state.flip_mode)

    if position_notional is None:
        position_notional = get_position_notional_usd(state.position_size, state.mid_price)

    close_side = get_position_close_side(state.position_size)
    if close_side and is_position_significant(position_notional):
        return close_side

    return opening_mode


def clear_order_tracking(state):
    """Clear the active order id and reuse-tracking fields."""
    state.active_order_id = None
    state.last_order_price = None
    state.last_order_side = None
    state.last_order_quantity = None
    state.active_order_started_at = None


def get_position_notional_usd(position_size, reference_price):
    """Estimate the USD notional of a position from a reference price."""
    if reference_price is None or reference_price <= 0:
        return 0.0
    return abs(position_size * reference_price)


def is_position_significant(position_notional):
    """Return True when the position is large enough to force closing mode."""
    return position_notional >= POSITION_THRESHOLD_USD


def apply_position_snapshot(state, position_size, position_notional=None):
    """Store the latest position size and align the mode without erasing residual inventory."""
    state.position_size = position_size
    state.position_update_seq += 1
    return sync_mode_with_position(state, position_notional=position_notional)


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
    previous_mode = state.mode
    position_size, notional_value = extract_position_snapshot(position_data, reference_price=reference_price)
    apply_position_snapshot(state, position_size, position_notional=notional_value)
    return position_size, notional_value, previous_mode, state.mode


def sync_mode_with_position(state, position_notional=None):
    """Keep the strategy mode aligned with the current bias and inventory threshold."""
    state.mode = get_target_mode(state, position_notional=position_notional)
    return state.mode


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


def get_supertrend_params_path(symbol):
    """Return the normalized Supertrend params file path for a trading symbol."""
    filename_symbol = normalize_symbol_base(symbol)
    return os.path.join(PARAMS_DIR, SUPERTREND_PARAMS_TEMPLATE.format(filename_symbol))


def load_supertrend_signal(symbol):
    """Load the latest Supertrend signal from disk."""
    params_file = get_supertrend_params_path(symbol)
    if not os.path.exists(params_file):
        raise FileNotFoundError(params_file)

    with open(params_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    signal = data.get('current_signal', {}).get('trend')
    if signal not in [1, -1]:
        raise ValueError(f"Invalid signal '{signal}' in {params_file}")

    return signal, params_file


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
    """Reset all in-memory Binance alpha state to avoid stale reuse or memory growth."""
    state.binance_bid_book.clear()
    state.binance_ask_book.clear()
    state.binance_last_update_id = None
    state.binance_alpha_buffer.clear()
    state.binance_alpha_ready = False
    state.binance_alpha_last_updated = None
    state.binance_alpha_raw_imbalance = None
    state.binance_alpha_zscore = None
    state.binance_alpha_shift_bps = 0.0
    state.binance_alpha_warmup_seconds = 0.0
    state.binance_best_bid = None
    state.binance_best_ask = None
    state.binance_last_logged_shift_bps = None
    state.binance_alpha_last_status = None
    state.binance_book_last_trim_at = 0.0
    state.binance_book_updates_since_trim = 0
    state.binance_band_mid_price = None
    state.binance_band_lower_bound = None
    state.binance_band_upper_bound = None
    state.binance_band_bid_qty = 0.0
    state.binance_band_ask_qty = 0.0
    publish_binance_alpha_snapshot(state)


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
    lower_bound = mid_price * (1.0 - BINANCE_OBI_LOOKING_DEPTH_PCT)
    upper_bound = mid_price * (1.0 + BINANCE_OBI_LOOKING_DEPTH_PCT)

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


def calculate_binance_orderbook_imbalance(state):
    """Compute normalized Binance book imbalance within +/- looking depth around mid."""
    if not state.binance_bid_book or not state.binance_ask_book:
        return None

    if _binance_band_requires_rebuild(state):
        _rebuild_binance_band_totals(state)

    if state.binance_band_mid_price is None:
        return None

    total_qty = state.binance_band_bid_qty + state.binance_band_ask_qty
    if total_qty <= 0.0:
        return None

    return (state.binance_band_bid_qty - state.binance_band_ask_qty) / total_qty


def calculate_binance_alpha_shift_bps(zscore):
    """Map a z-score to a capped quote shift in basis points."""
    if zscore is None or not np.isfinite(zscore):
        return 0.0
    shift_bps = float(zscore) * BINANCE_OBI_BPS_PER_SIGMA
    return max(-BINANCE_OBI_MAX_SHIFT_BPS, min(BINANCE_OBI_MAX_SHIFT_BPS, shift_bps))


def is_binance_alpha_live(state):
    """Return True when Binance alpha is warmed up and ready for opening quotes."""
    if not USE_BINANCE_OBI_ALPHA:
        return True
    return bool(state.binance_alpha_snapshot.ready or state.binance_alpha_ready)


def binance_alpha_status_text(state):
    """Return a compact status string for logs and reporters."""
    snapshot = state.binance_alpha_snapshot
    if not USE_BINANCE_OBI_ALPHA:
        return "Binance OBI disabled"
    if snapshot.ready is False and state.binance_alpha_ready:
        snapshot = BinanceAlphaSnapshot(
            ready=True,
            zscore=state.binance_alpha_zscore,
            shift_bps=state.binance_alpha_shift_bps,
            warmup_seconds=state.binance_alpha_warmup_seconds,
            sample_count=state.binance_alpha_buffer.count,
            best_bid=state.binance_best_bid,
            best_ask=state.binance_best_ask,
            last_updated=state.binance_alpha_last_updated,
            ws_connected=state.binance_alpha_ws_connected,
        )
    if snapshot.ready:
        zscore = snapshot.zscore if snapshot.zscore is not None else 0.0
        return f"Binance OBI z={zscore:+.2f} shift={snapshot.shift_bps:+.1f}bps"
    if snapshot.ws_connected:
        return (
            f"Binance OBI warming {snapshot.warmup_seconds:.0f}s/"
            f"{BINANCE_OBI_WARMUP_SECONDS}s samples={snapshot.sample_count}"
        )
    return "Binance OBI unavailable"

    return rounded


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
    """Maintain Binance order book state and publish immutable alpha snapshots."""

    def clear(self, state):
        clear_binance_alpha_state(state)

    def initialize_local_book(self, state, snapshot):
        _initialize_binance_local_book(state, snapshot)
        publish_binance_alpha_snapshot(state)

    def apply_depth_event(self, state, event, require_prev_match=True):
        return _apply_binance_depth_event(state, event, require_prev_match=require_prev_match)

    def update_metrics(self, state, runtime):
        _update_binance_alpha_metrics(state, runtime)


class QuoteEngine:
    """Own all quote decisions off immutable feed snapshots and prepared params."""

    def prepare_quote_params(self, params):
        return prepare_quote_params_snapshot(params)

    def get_prepared_params(self, state):
        return state.prepared_quote_params or get_unavailable_prepared_quote_params()

    def estimate_quote_center(self, state, book_snapshot=None, prepared_params=None):
        snapshot = book_snapshot or state.aster_top_of_book_snapshot
        if snapshot is None:
            return None

        params = prepared_params or self.get_prepared_params(state)
        mid_price = snapshot.mid_price
        alpha_snapshot = state.binance_alpha_snapshot
        alpha_shift_abs = 0.0
        if alpha_snapshot.ready and mid_price and mid_price > 0.0:
            alpha_shift_abs = mid_price * (alpha_snapshot.shift_bps / 10000.0)

        if params.kind == "avellaneda":
            risk_term = params.gamma * ((params.sigma * mid_price) ** 2) * params.time_horizon_days
            return mid_price - state.position_size * risk_term + alpha_shift_abs

        return mid_price + alpha_shift_abs

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

    def build_quote_command(self, state, symbol_filters):
        return build_quote_command(state, symbol_filters)

    async def run(self, state, client, symbol, runtime):
        log = logging.getLogger('MarketMakerLoop')
        log.info(f"Fetching trading rules for {symbol}...")
        symbol_filters = await client.get_symbol_filters(symbol)
        state.symbol_filters = symbol_filters
        log.info(f"Filters loaded: {symbol_filters}")
        request_quote_refresh(state)

        while not runtime.shutdown_requested:
            try:
                await state.quote_refresh_event.wait()
                state.quote_refresh_event.clear()

                while True:
                    if runtime.shutdown_requested:
                        break

                    if symbol_filters.get("status", "TRADING") != "TRADING":
                        if state.active_order_id:
                            publish_latest_order_command(
                                state,
                                OrderCommand(kind="cancel", trigger=f"Symbol status {symbol_filters.get('status', 'UNKNOWN')}"),
                            )
                        break

                    if not state.price_ws_connected or not state.user_data_ws_connected:
                        if state.active_order_id:
                            publish_latest_order_command(
                                state,
                                OrderCommand(kind="cancel", trigger="WebSocket disconnection"),
                            )
                        break

                    if not is_price_data_valid(state, runtime) or not is_balance_data_valid(state):
                        break

                    if is_opening_circuit_breaker_active(state, runtime) and not has_open_position(state):
                        break

                    bias_changed, current_notional = apply_supertrend_bias(state)
                    if bias_changed:
                        trend_name = "DOWNTREND" if state.flip_mode else "UPTREND"
                        log.info(f"Supertrend switched strategy bias to {trend_name} while inventory was below ${current_notional:.2f}.")

                    quote_command, order_candidate = self.build_quote_command(state, symbol_filters)
                    if quote_command is None:
                        reason = order_candidate["reason"]
                        if reason in {"missing_quote_params", "invalid_quote_params", "binance_alpha_unavailable"}:
                            if state.active_order_id:
                                publish_latest_order_command(
                                    state,
                                    OrderCommand(kind="cancel", trigger=f"Quotes unavailable: {reason}"),
                                )
                            break

                        if reason in {"non_positive_quantity", "min_qty", "min_notional"} and state.active_order_id:
                            publish_latest_order_command(
                                state,
                                OrderCommand(kind="cancel", trigger=f"Quote invalid: {reason}"),
                            )
                        break

                    if not should_reuse_order(state, quote_command.price, quote_command.side, quote_command.quantity):
                        publish_latest_order_command(state, quote_command)

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

    async def place_order(self, state, client, symbol, runtime, log, command):
        return await place_order_from_command(
            state,
            client,
            symbol,
            runtime,
            log,
            command,
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
                if len(open_orders) <= 1:
                    continue

                order_ids = [order.get("orderId") for order in open_orders]
                log.error(f"Detected multiple open orders for {symbol}: {order_ids}")
                if OPEN_ORDER_WATCHDOG_CANCEL_ALL:
                    await client.cancel_all_orders(symbol)
                    log.error(f"Cancelled all open orders for {symbol} after watchdog detected duplicates.")
                else:
                    active_order_id = state.active_order_id
                    for order in open_orders:
                        order_id = order.get("orderId")
                        if active_order_id is not None and order_id == active_order_id:
                            continue
                        await client.cancel_order(symbol, order_id)
                    log.error(f"Cancelled duplicate open orders for {symbol}; keeping tracked order {active_order_id}.")

                clear_order_tracking(state)
                state.pending_terminal_orders.clear()
                request_quote_refresh(state)

            except asyncio.CancelledError:
                log.info("Order watchdog cancelled.")
                break
            except Exception as exc:
                log.error(f"Order watchdog error: {exc}", exc_info=True)

async def cancel_active_order(state, client, symbol, log, reason, clear_tracking_on_success=True):
    """Cancel the currently tracked order and optionally clear local tracking on success."""
    if not state.active_order_id:
        return True

    order_id_to_cancel = state.active_order_id
    try:
        if CANCEL_SPECIFIC_ORDER:
            await client.cancel_order(symbol, order_id_to_cancel)
        else:
            await client.cancel_all_orders(symbol)
    except Exception as cancel_error:
        log.error(f"{reason}: failed to cancel active order {order_id_to_cancel}: {cancel_error}")
        return False

    if clear_tracking_on_success:
        clear_order_tracking(state)
    log.info(f"{reason}: cancelled active order {order_id_to_cancel}.")
    return True


def get_close_side_for_trading(state):
    """Return the side needed to flatten any tracked inventory, including small residuals."""
    if not has_open_position(state):
        return None

    return get_position_close_side(state.position_size)


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
    """Refresh strategy mode after a fill using position snapshots as the source of truth."""
    previous_mode = state.mode

    if await wait_for_position_sync(state, previous_position_seq, POSITION_SYNC_TIMEOUT):
        sync_mode_with_position(state)
        new_mode = state.mode
        return True, previous_mode, new_mode

    log.warning(f"{fill_context}: no position snapshot arrived within {POSITION_SYNC_TIMEOUT:.1f}s; falling back to REST sync.")
    positions = await client.get_position_risk(symbol)
    if positions:
        sync_state_from_position_data(state, positions[0], reference_price=state.mid_price)
    else:
        apply_position_snapshot(state, 0.0, position_notional=0.0)

    sync_mode_with_position(state)
    new_mode = state.mode
    return False, previous_mode, new_mode


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
                            data = json.loads(message)

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
    """Refresh the bounded rolling Binance alpha state from the latest local book."""
    now = runtime.now()
    should_trim = (
        state.binance_book_updates_since_trim >= BINANCE_OBI_TRIM_INTERVAL_UPDATES
        or (now - state.binance_book_last_trim_at) >= BINANCE_OBI_TRIM_INTERVAL_SECONDS
    )
    if should_trim:
        _trim_binance_books(state)
        state.binance_book_last_trim_at = now
        state.binance_book_updates_since_trim = 0

    raw_imbalance = calculate_binance_orderbook_imbalance(state)
    if raw_imbalance is None:
        publish_binance_alpha_snapshot(state)
        return

    now_ms = int(now * 1000)
    cutoff_ms = now_ms - (BINANCE_OBI_ZSCORE_WINDOW_SECONDS * 1000)
    state.binance_alpha_buffer.evict_older_than(cutoff_ms)
    state.binance_alpha_buffer.append(now_ms, raw_imbalance)

    state.binance_alpha_last_updated = now
    state.binance_alpha_raw_imbalance = raw_imbalance
    state.binance_alpha_warmup_seconds = state.binance_alpha_buffer.span_seconds()
    std = state.binance_alpha_buffer.std()
    if std is None or std <= 1e-12:
        state.binance_alpha_zscore = None
        state.binance_alpha_shift_bps = 0.0
        state.binance_alpha_ready = False
        publish_binance_alpha_snapshot(state)
        return

    mean = state.binance_alpha_buffer.mean()
    zscore = (raw_imbalance - mean) / std
    state.binance_alpha_zscore = zscore
    state.binance_alpha_shift_bps = calculate_binance_alpha_shift_bps(zscore)
    state.binance_alpha_ready = (
        state.binance_alpha_buffer.count >= BINANCE_OBI_MIN_SAMPLES
        and state.binance_alpha_warmup_seconds >= BINANCE_OBI_WARMUP_SECONDS
    )
    publish_binance_alpha_snapshot(state)


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


async def binance_orderbook_imbalance_updater(state, symbol, runtime, alpha_engine=None):
    """Maintain a bounded local Binance futures book and rolling imbalance alpha."""
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
                publish_binance_alpha_snapshot(state)
                request_quote_refresh(state)
                reconnect_delay = 5.0
                log.info(f"Binance OBI connected for {symbol} via diff-depth @100ms")

                snapshot_task = asyncio.create_task(_fetch_binance_depth_snapshot(symbol))
                buffered_events = []

                while not snapshot_task.done() and not runtime.shutdown_requested:
                    message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                    event = _extract_binance_depth_event(json.loads(message))
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
                last_ready = state.binance_alpha_ready

                while not runtime.shutdown_requested:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=BINANCE_OBI_STALE_TIMEOUT_SECONDS)
                    except asyncio.TimeoutError:
                        raise BinanceOrderBookSyncError("Binance OBI stream became stale")

                    last_message_time = runtime.now()
                    event = _extract_binance_depth_event(json.loads(message))
                    if event is None:
                        continue

                    alpha_engine.apply_depth_event(state, event)
                    previous_ready = state.binance_alpha_ready
                    previous_shift = state.binance_alpha_shift_bps
                    alpha_engine.update_metrics(state, runtime)

                    if state.binance_alpha_ready and not previous_ready:
                        zscore = state.binance_alpha_zscore if state.binance_alpha_zscore is not None else 0.0
                        log.info(
                            f"Binance OBI ready: raw={state.binance_alpha_raw_imbalance:+.4f} "
                            f"z={zscore:+.2f} shift={state.binance_alpha_shift_bps:+.1f} bps "
                            f"window={state.binance_alpha_warmup_seconds:.0f}s samples={state.binance_alpha_buffer.count}"
                        )

                    if state.binance_alpha_ready:
                        should_log_shift = (
                            state.binance_last_logged_shift_bps is None
                            or abs(state.binance_alpha_shift_bps - state.binance_last_logged_shift_bps) >= BINANCE_OBI_SHIFT_LOG_DELTA_BPS
                        )
                        if should_log_shift:
                            zscore = state.binance_alpha_zscore if state.binance_alpha_zscore is not None else 0.0
                            log.info(
                                f"Binance OBI shift update: z={zscore:+.2f}, "
                                f"shift={state.binance_alpha_shift_bps:+.1f} bps"
                            )
                            state.binance_last_logged_shift_bps = state.binance_alpha_shift_bps

                    if (
                        state.binance_alpha_ready != previous_ready
                        or abs(state.binance_alpha_shift_bps - previous_shift) >= BINANCE_OBI_SHIFT_LOG_DELTA_BPS
                    ):
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
                            data = json.loads(message)
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
                                        new_position_size, notional_value, previous_mode, _ = sync_state_from_position_data(
                                            state,
                                            position,
                                            reference_price=reference_price,
                                        )

                                        if abs(previous_position_size - new_position_size) > 1e-9:
                                            log.info(f"Real-time position update for {symbol}: size changed from {previous_position_size:.6f} to {new_position_size:.6f}")

                                        # Update mode based on notional value
                                        if state.mode != previous_mode:
                                            log.info(
                                                f"Position notional from WS (${notional_value:.2f}) changed mode: "
                                                f"{previous_mode} -> {state.mode}."
                                            )
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

                alpha_info = f" | {binance_alpha_status_text(state)}"
                log.info(
                    f"{symbol} | Mid-Price: ${state.mid_price:.4f} | Bid-Ask Spread: {spread_percentage:.3f}% "
                    f"| Bid: ${state.bid_price:.4f} | Ask: ${state.ask_price:.4f}{balance_info}{alpha_info}"
                )

        except Exception as e:
            log.error(f"Error in price reporter: {e}")

    log.info("Price reporter shutting down")


async def initialize_supertrend_signal(state, symbol):
    """Reads the Supertrend signal file once at startup to set the initial state."""
    log = logging.getLogger('SupertrendInitializer')

    try:
        initial_signal, params_file = load_supertrend_signal(symbol)
        state.supertrend_signal = initial_signal
        new_flip_mode = (initial_signal == -1)
        if state.flip_mode != new_flip_mode:
            state.flip_mode = new_flip_mode
            sync_mode_with_position(state)
            log.info(f"Initialized Supertrend signal to: {'UPTREND (+1)' if initial_signal == 1 else 'DOWNTREND (-1)'}")
            log.info(f"Initial strategy bias set by signal: FLIP_MODE -> {state.flip_mode}")
        else:
            log.info(f"Initial Supertrend signal confirms default bias: FLIP_MODE -> {state.flip_mode}")
    except FileNotFoundError:
        params_file = get_supertrend_params_path(symbol)
        log.warning(f"Supertrend params file not found at {params_file}. Using default FLIP_MODE={state.flip_mode}.")
    except ValueError as exc:
        log.warning(f"{exc}. Using default FLIP_MODE={state.flip_mode}.")
    except Exception as e:
        log.error(f"Error initializing Supertrend signal: {e}. Using default FLIP_MODE={state.flip_mode}.")


async def wait_for_startup_inputs(state, symbol, runtime):
    """Block startup until Avellaneda, Supertrend, and Binance alpha are ready."""
    log = logging.getLogger('StartupInputs')
    wait_seconds = 5
    last_status = None

    while not runtime.shutdown_requested:
        avellaneda_ready = (not USE_AVELLANEDA_SPREADS) or state.quote_params.get("source") != "unavailable"
        supertrend_ready = (not USE_SUPERTREND_SIGNAL) or state.supertrend_signal in [1, -1]
        binance_ready = is_binance_alpha_live(state)
        status = (avellaneda_ready, supertrend_ready, binance_ready)
        if status != last_status:
            if avellaneda_ready and supertrend_ready and binance_ready:
                log.info(
                    f"Startup inputs ready for {symbol}: Avellaneda params, Supertrend signal, "
                    "and Binance OBI warmup are all valid."
                )
            else:
                missing = []
                if not avellaneda_ready:
                    missing.append("Avellaneda params")
                if not supertrend_ready:
                    missing.append("Supertrend signal")
                if not binance_ready:
                    missing.append("Binance OBI warmup")
                log.info(
                    f"Waiting for startup inputs for {symbol}: missing {', '.join(missing)}. "
                    f"Retrying every {wait_seconds}s."
                )
            last_status = status

        if avellaneda_ready and supertrend_ready and binance_ready:
            return True

        await asyncio.sleep(wait_seconds)

    return False


async def supertrend_signal_updater(state, symbol, runtime):
    """Periodically reads the Supertrend signal file and updates the strategy state."""
    log = logging.getLogger('SupertrendUpdater')

    while not runtime.shutdown_requested:
        try:
            new_signal, params_file = load_supertrend_signal(symbol)
            if state.supertrend_signal != new_signal:
                state.supertrend_signal = new_signal
                log.info(f"Supertrend signal updated to: {'UPTREND (+1)' if new_signal == 1 else 'DOWNTREND (-1)'}")
                request_quote_refresh(state)

            await asyncio.sleep(SUPERTREND_CHECK_INTERVAL)

        except FileNotFoundError:
            params_file = get_supertrend_params_path(symbol)
            if state.supertrend_signal is None:
                log.warning(f"Supertrend params file not found at {params_file}. Keeping default bias until a valid signal appears.")
            else:
                log.warning(f"Supertrend params file not found at {params_file}. Holding previous signal {state.supertrend_signal}.")
            await asyncio.sleep(SUPERTREND_CHECK_INTERVAL)
        except json.JSONDecodeError:
            params_file = get_supertrend_params_path(symbol)
            if state.supertrend_signal is None:
                log.error(f"Error decoding JSON from {params_file}. Keeping default bias until a valid signal appears.")
            else:
                log.error(f"Error decoding JSON from {params_file}. Holding previous signal {state.supertrend_signal}.")
            await asyncio.sleep(SUPERTREND_CHECK_INTERVAL)
        except ValueError as exc:
            if state.supertrend_signal is None:
                log.warning(f"{exc}. Keeping default bias until a valid signal appears.")
            else:
                log.warning(f"{exc}. Holding previous signal {state.supertrend_signal}.")
            await asyncio.sleep(SUPERTREND_CHECK_INTERVAL)
        except Exception as e:
            if state.supertrend_signal is None:
                log.error(f"An error occurred in the Supertrend signal updater: {e}. Keeping default bias until a valid signal appears.")
            else:
                log.error(f"An error occurred in the Supertrend signal updater: {e}. Holding previous signal {state.supertrend_signal}.")
            await asyncio.sleep(SUPERTREND_CHECK_INTERVAL)
    
    log.info("Supertrend signal updater shutting down.")
def should_reuse_order(state, new_price, new_side, new_quantity, threshold=DEFAULT_PRICE_CHANGE_THRESHOLD):
    """Check if existing order can be reused based on price change threshold."""
    if (state.active_order_id is None or
        state.last_order_price is None or
        state.last_order_side != new_side or
        abs(state.last_order_quantity - new_quantity) > 0.000000000001):  # Different quantity
        return False

    # Calculate price change percentage
    price_change_pct = abs(new_price - state.last_order_price) / state.last_order_price

    # Reuse if price change is below threshold
    return price_change_pct < threshold


def apply_supertrend_bias(state):
    """Update flip_mode from the cached signal when inventory is below the significance threshold."""
    if not USE_SUPERTREND_SIGNAL or state.supertrend_signal is None:
        return False, None

    current_notional = get_position_notional_usd(state.position_size, state.mid_price)
    if current_notional >= POSITION_THRESHOLD_USD:
        return False, current_notional

    new_flip_mode = (state.supertrend_signal == -1)
    if state.flip_mode == new_flip_mode:
        return False, current_notional

    state.flip_mode = new_flip_mode
    sync_mode_with_position(state, position_notional=current_notional)
    return True, current_notional


def get_runtime_quote_params(state):
    """Return the latest cached quoting parameters."""
    if state.quote_params:
        return state.quote_params

    if not USE_AVELLANEDA_SPREADS:
        return {"buy_spread": DEFAULT_BUY_SPREAD, "sell_spread": DEFAULT_SELL_SPREAD, "source": "default"}

    return get_unavailable_quote_params()


def get_runtime_prepared_quote_params(state):
    """Return the prevalidated quote snapshot used by the quote hot path."""
    raw_params = get_runtime_quote_params(state)
    prepared = state.prepared_quote_params
    if prepared is None:
        return prepare_quote_params_snapshot(raw_params)
    if prepared.kind == "unavailable" and raw_params.get("source") != "unavailable":
        return prepare_quote_params_snapshot(raw_params)
    if prepared.source != str(raw_params.get("source") or "unavailable"):
        return prepare_quote_params_snapshot(raw_params)
    return prepared


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


def resolve_avellaneda_spread_limits_bps(params):
    """Return validated Avellaneda spread guardrails from params or runtime defaults."""
    default_limits = {
        "min": DEFAULT_MIN_AVELLANEDA_SPREAD_BPS,
        "max": DEFAULT_MAX_AVELLANEDA_SPREAD_BPS,
    }
    spread_limits = params.get("spread_limits_bps")
    if not isinstance(spread_limits, dict):
        return default_limits

    min_bps = _safe_float(spread_limits.get("min"))
    max_bps = _safe_float(spread_limits.get("max"))
    if min_bps is None or max_bps is None or min_bps < 0.0 or max_bps <= 0.0 or min_bps > max_bps:
        return default_limits

    return {"min": min_bps, "max": max_bps}


def clamp_offset_to_spread_limits(offset, mid_price, spread_limits_bps):
    """Clamp a dynamic Avellaneda quote offset to configured basis-point guardrails."""
    if mid_price <= 0:
        return offset

    raw_bps = (float(offset) / float(mid_price)) * 10000.0
    clamped_bps = min(max(raw_bps, spread_limits_bps["min"]), spread_limits_bps["max"])
    return float(mid_price) * (clamped_bps / 10000.0)


def get_binance_alpha_shift_abs(state, reference_price):
    """Convert the current Binance alpha shift in bps into an absolute price offset."""
    if reference_price is None or reference_price <= 0:
        return 0.0
    alpha_snapshot = state.binance_alpha_snapshot
    alpha_ready = alpha_snapshot.ready or state.binance_alpha_ready
    shift_bps = alpha_snapshot.shift_bps if alpha_snapshot.ready else state.binance_alpha_shift_bps
    if not USE_BINANCE_OBI_ALPHA or not alpha_ready:
        return 0.0
    return float(reference_price) * (shift_bps / 10000.0)


def build_order_plan(state, opening_mode, params):
    """Build the intended side, price, and size for the next quote."""
    if isinstance(params, dict):
        params = prepare_quote_params_snapshot(params)

    close_side = get_close_side_for_trading(state)

    if params.kind == "avellaneda":
        gamma = params.gamma
        sigma = params.sigma
        k_buy = params.k_buy
        k_sell = params.k_sell
        time_horizon = params.time_horizon_days
        position_size = state.position_size
        mid_price = state.mid_price
        spread_limits_bps = {
            "min": params.spread_limits_min_bps,
            "max": params.spread_limits_max_bps,
        }

        risk_term = gamma * ((sigma * mid_price) ** 2) * time_horizon
        reservation_price = mid_price - position_size * risk_term
        alpha_shift_abs = get_binance_alpha_shift_abs(state, mid_price)
        reservation_price += alpha_shift_abs
        ask_offset = (1 / gamma) * math.log1p(gamma / k_buy) + (risk_term / 2.0)
        bid_offset = (1 / gamma) * math.log1p(gamma / k_sell) + (risk_term / 2.0)
        ask_offset = clamp_offset_to_spread_limits(ask_offset, mid_price, spread_limits_bps)
        bid_offset = clamp_offset_to_spread_limits(bid_offset, mid_price, spread_limits_bps)
        ask_price = reservation_price + ask_offset
        bid_price = reservation_price - bid_offset

        if close_side:
            side, reduce_only = close_side, True
            quantity_to_trade = abs(state.position_size)
            limit_price = ask_price if side == 'SELL' else bid_price
        else:
            side, reduce_only = opening_mode, False
            quantity_to_trade = (state.account_balance * DEFAULT_BALANCE_FRACTION) / state.mid_price
            limit_price = bid_price if opening_mode == 'BUY' else ask_price

        used_spread = (ask_price - bid_price) / mid_price if mid_price > 0 else 0
        return {
            "side": side,
            "reduce_only": reduce_only,
            "quantity_to_trade": quantity_to_trade,
            "limit_price": limit_price,
            "used_spread": used_spread,
            "reservation_price": reservation_price,
            "bid_price": bid_price,
            "ask_price": ask_price,
            "binance_alpha_shift_bps": state.binance_alpha_snapshot.shift_bps if state.binance_alpha_snapshot.ready else 0.0,
        }

    if params.kind == "unavailable":
        return None

    buy_spread, sell_spread = params.buy_spread, params.sell_spread
    alpha_shift_abs = get_binance_alpha_shift_abs(state, state.mid_price)
    if close_side:
        side, reduce_only = close_side, True
        quantity_to_trade = abs(state.position_size)
        limit_price = state.mid_price * (1 + sell_spread) if side == 'SELL' else state.mid_price * (1 - buy_spread)
    else:
        side, reduce_only = opening_mode, False
        quantity_to_trade = (state.account_balance * DEFAULT_BALANCE_FRACTION) / state.mid_price
        limit_price = state.mid_price * (1 - buy_spread) if opening_mode == 'BUY' else state.mid_price * (1 + sell_spread)
    limit_price += alpha_shift_abs

    used_spread = sell_spread if side == 'SELL' else buy_spread
    return {
        "side": side,
        "reduce_only": reduce_only,
        "quantity_to_trade": quantity_to_trade,
        "limit_price": limit_price,
        "used_spread": used_spread,
        "binance_alpha_shift_bps": state.binance_alpha_snapshot.shift_bps if state.binance_alpha_snapshot.ready else 0.0,
    }


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


def build_quote_command(state, symbol_filters):
    """Build the current desired order command from in-memory state only."""
    params = get_runtime_prepared_quote_params(state)
    if params.kind == "unavailable":
        return None, {"reason": "missing_quote_params"}

    opening_mode, _ = get_strategy_modes(state.flip_mode)
    order_plan = build_order_plan(state, opening_mode, params)
    if order_plan is None:
        return None, {"reason": "invalid_quote_params"}
    if not order_plan["reduce_only"] and USE_BINANCE_OBI_ALPHA and not is_binance_alpha_live(state):
        return None, {
            "reason": "binance_alpha_unavailable",
            "warmup_seconds": state.binance_alpha_snapshot.warmup_seconds,
            "sample_count": state.binance_alpha_snapshot.sample_count,
        }

    if not order_plan["reduce_only"]:
        required_balance = get_required_opening_balance(symbol_filters, order_plan["limit_price"])
        if state.account_balance is None or state.account_balance + POSITION_SIZE_EPSILON < required_balance:
            return None, {
                "reason": "insufficient_opening_capital",
                "required_balance": required_balance,
                "current_balance": state.account_balance,
            }

    order_candidate = prepare_order_candidate(
        symbol_filters,
        order_plan["side"],
        order_plan["reduce_only"],
        order_plan["limit_price"],
        order_plan["quantity_to_trade"],
    )
    if not order_candidate["ok"]:
        return None, order_candidate

    command = OrderCommand(
        kind="quote",
        side=order_plan["side"],
        reduce_only=order_plan["reduce_only"],
        price=float(order_candidate["rounded_price"]),
        quantity=float(order_candidate["rounded_quantity"]),
        order_notional=order_candidate["order_notional"],
        trigger="price",
    )
    return command, order_candidate


def classify_order_update(order_data, fill_notional_threshold=POSITION_THRESHOLD_USD):
    """Classify an order update into terminal/non-terminal and fill/non-fill outcomes."""
    del fill_notional_threshold  # Retained for compatibility with older callers/tests.

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


def apply_fill_to_state(state, side, filled_qty):
    """Refresh strategy mode after a fill using the latest position snapshot."""
    if filled_qty is not None and filled_qty <= 0:
        return state.mode, state.mode

    previous_mode = state.mode

    sync_mode_with_position(state)
    return previous_mode, state.mode


async def cancel_and_finalize_active_order(state, client, symbol, log, reason, order_label):
    """Cancel the tracked order and wait for a terminal state before proceeding."""
    if not state.active_order_id:
        return True

    order_id = state.active_order_id
    position_update_seq_before_fill = state.position_update_seq
    if not await cancel_active_order(
        state,
        client,
        symbol,
        log,
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
        state.last_order_side,
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
        synced_via_ws, previous_mode, new_mode = await reconcile_fill_with_position(
            state,
            client,
            symbol,
            log,
            position_update_seq_before_fill,
            f"{order_label} {order_id}",
        )
        sync_source = "WebSocket" if synced_via_ws else "REST fallback"
        log.info(f"{side} fill reconciled via {sync_source}: tracked position size {state.position_size:.6f}")
        if new_mode != previous_mode:
            log.info(f"Mode change: {previous_mode} -> {new_mode}")
    else:
        log.info(f"{order_label} {order_id} ended as {terminal_update['status']} without an executed fill.")

    if state.active_order_id == order_id:
        clear_order_tracking(state)
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


async def place_order_from_command(state, client, symbol, runtime, log, command, symbol_filters=None):
    """Submit the desired order command and update local tracking."""
    current_time = runtime.now()
    time_since_last_order = current_time - runtime.last_order_time
    if time_since_last_order < MIN_ORDER_INTERVAL:
        wait_time = MIN_ORDER_INTERVAL - time_since_last_order
        log.info(f"Rate limiting: waiting {wait_time:.3f}s before placing order")
        await asyncio.sleep(wait_time)
        if not state.active_order_id and command.kind == "quote":
            command = drain_latest_order_command(state, command)
            if command.kind != "quote":
                return None

    percentage_diff = 0.0
    if state.mid_price:
        percentage_diff = (command.price - state.mid_price) / state.mid_price * 100

    filters = symbol_filters or state.symbol_filters or {}
    price_precision = int(filters.get("price_precision", 8))
    quantity_precision = int(filters.get("quantity_precision", 8))
    formatted_price = f"{command.price:.{price_precision}f}"
    formatted_quantity = f"{command.quantity:.{quantity_precision}f}"

    log.info(
        f"Placing {command.side} order: {formatted_quantity} {symbol} @ {formatted_price} "
        f"({percentage_diff:+.4f}% from mid-price, Binance shift {state.binance_alpha_snapshot.shift_bps:+.1f} bps)"
    )

    active_order = await client.place_order(
        symbol,
        formatted_price,
        formatted_quantity,
        command.side,
        command.reduce_only,
    )
    runtime.last_order_time = runtime.now()
    state.active_order_id = active_order.get('orderId')
    state.active_order_started_at = runtime.last_order_time
    state.last_order_price = command.price
    state.last_order_side = command.side
    state.last_order_quantity = command.quantity
    if not command.reduce_only:
        reset_opening_order_failures(state)
    log.info(f"Order placed successfully: ID={state.active_order_id}")
    return active_order


async def avellaneda_params_updater(state, symbol, runtime):
    """Refresh quote parameters out of band so the quote engine stays memory-only."""
    log = logging.getLogger('AvellanedaParamsUpdater')
    quote_engine = QuoteEngine()

    while not runtime.shutdown_requested:
        try:
            params = get_avellaneda_params(symbol)
            prepared_params = quote_engine.prepare_quote_params(params)
            if params != state.quote_params or prepared_params != state.prepared_quote_params:
                state.quote_params = params
                state.prepared_quote_params = prepared_params
                log.info(f"Updated quoting parameters for {symbol} from {params['source']}")
                request_quote_refresh(state)
        except Exception as exc:
            log.error(f"Failed to refresh quoting parameters for {symbol}: {exc}")

        await asyncio.sleep(SPREAD_CACHE_TTL_SECONDS)


async def order_manager_loop_impl(state, client, symbol, runtime, executor):
    """Own the active exchange order and react to quote intents immediately."""
    log = logging.getLogger('OrderManager')

    while not runtime.shutdown_requested:
        try:
            await reconcile_stale_pending_terminal_orders(state, client, symbol, runtime, log)

            if not state.active_order_id:
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
                        received_update = order_update_task.result()
                        order_data = received_update.get('o', {})
                        order_id = order_data.get('i')
                        if received_update.get('e') == 'ORDER_TRADE_UPDATE' and order_id in state.pending_terminal_orders:
                            terminal_update = classify_order_update(order_data)
                            if terminal_update["is_terminal"]:
                                pending_order = state.pending_terminal_orders.pop(order_id)
                                order_was_opening = not is_order_reduce_only(order_data)
                                if terminal_update["status"] == "REJECTED" and order_was_opening:
                                    record_opening_order_failure(state, runtime)
                                elif terminal_update["status"] == "FILLED" and order_was_opening:
                                    reset_opening_order_failures(state)
                                await handle_terminal_order_update(
                                    state,
                                    client,
                                    symbol,
                                    log,
                                    pending_order.side,
                                    order_id,
                                    terminal_update,
                                    pending_order.position_update_seq_before_fill,
                                    pending_order.order_label,
                                )
                                request_quote_refresh(state)
                        continue

                    command = command_task.result()
                else:
                    command = await state.order_commands.get()
                command = drain_latest_order_command(state, command)
                if command.kind != "quote":
                    continue

                try:
                    placed = await executor.place_order(state, client, symbol, runtime, log, command)
                except Exception:
                    if not command.reduce_only:
                        record_opening_order_failure(state, runtime)
                    raise

                if placed is None:
                    continue
                continue

            order_age = runtime.now() - (state.active_order_started_at or runtime.now())
            remaining_timeout = max(0.0, ORDER_REFRESH_INTERVAL - order_age)

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
                order_data = received_update.get('o', {})
                order_id = order_data.get('i')
                if received_update.get('e') == 'ORDER_TRADE_UPDATE' and order_id == state.active_order_id:
                    terminal_update = classify_order_update(order_data)
                    if terminal_update["is_terminal"]:
                        order_was_opening = not is_order_reduce_only(order_data)
                        if terminal_update["status"] == "REJECTED" and order_was_opening:
                            record_opening_order_failure(state, runtime)
                        elif terminal_update["status"] == "FILLED" and order_was_opening:
                            reset_opening_order_failures(state)

                        order_id = state.active_order_id
                        position_update_seq_before_fill = state.position_update_seq
                        await handle_terminal_order_update(
                            state,
                            client,
                            symbol,
                            log,
                            state.last_order_side,
                            order_id,
                            terminal_update,
                            position_update_seq_before_fill,
                            "Order",
                        )
                        request_quote_refresh(state)
                        action_taken = True
                elif received_update.get('e') == 'ORDER_TRADE_UPDATE' and order_id in state.pending_terminal_orders:
                    terminal_update = classify_order_update(order_data)
                    if terminal_update["is_terminal"]:
                        pending = state.pending_terminal_orders.pop(order_id)
                        order_was_opening = not is_order_reduce_only(order_data)
                        if terminal_update["status"] == "REJECTED" and order_was_opening:
                            record_opening_order_failure(state, runtime)
                        elif terminal_update["status"] == "FILLED" and order_was_opening:
                            reset_opening_order_failures(state)

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
                        request_quote_refresh(state)
                        action_taken = True

            if not action_taken and timed_out:
                log.info(
                    f"Order {state.active_order_id} reached the {ORDER_REFRESH_INTERVAL:.1f}s safety lifetime. "
                    "Refreshing the quote."
                )
                if not await cancel_and_finalize_active_order(
                    state,
                    client,
                    symbol,
                    log,
                    "Timed-out order refresh",
                    "Timed-out order",
                ):
                    await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                    continue

                request_quote_refresh(state)
                action_taken = True

            if not action_taken and received_command is not None:
                command = drain_latest_order_command(state, received_command)
                if command.kind == "cancel":
                    if not await cancel_and_finalize_active_order(
                        state,
                        client,
                        symbol,
                        log,
                        command.trigger or "Quote engine cancel",
                        "Active order",
                    ):
                        await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                        continue
                    request_quote_refresh(state)
                    action_taken = True
                elif should_reuse_order(state, command.price, command.side, command.quantity):
                    action_taken = True
                else:
                    if executor.fast_replace:
                        order_id = state.active_order_id
                        if not await cancel_active_order(
                            state,
                            client,
                            symbol,
                            log,
                            command.trigger or "Fast requote replacement",
                            clear_tracking_on_success=False,
                        ):
                            await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                            continue

                        if order_id is not None:
                            state.pending_terminal_orders[order_id] = PendingTerminalOrder(
                                side=state.last_order_side or command.side,
                                reduce_only=False,
                                position_update_seq_before_fill=state.position_update_seq,
                                order_label="Fast requote order",
                                cancel_requested_at=runtime.now(),
                            )
                        clear_order_tracking(state)
                    else:
                        if not await cancel_and_finalize_active_order(
                            state,
                            client,
                            symbol,
                            log,
                            command.trigger or "Requote replacement",
                            "Requote order",
                        ):
                            await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                            continue

                    if runtime.shutdown_requested:
                        break

                    command = drain_latest_order_command(state, command)
                    if command.kind == "quote":
                        try:
                            placed = await executor.place_order(state, client, symbol, runtime, log, command)
                        except Exception:
                            if not command.reduce_only:
                                record_opening_order_failure(state, runtime)
                            raise

                        if placed is None:
                            continue
                    else:
                        request_quote_refresh(state)
                    action_taken = True

            if not action_taken and received_update is not None:
                continue

        except asyncio.CancelledError:
            log.info("Order manager cancelled.")
            break
        except Exception as exc:
            log.error(f"An error occurred in the order manager: {exc}", exc_info=True)
            if state.active_order_id:
                await cancel_active_order(state, client, symbol, log, "Order manager error")
            await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)


async def order_manager_loop(state, client, symbol, runtime, executor=None):
    """Compatibility wrapper around the explicit OrderExecutor component."""
    executor = executor or OrderExecutor()
    return await order_manager_loop_impl(state, client, symbol, runtime, executor)


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parameter_file_candidates(symbol):
    symbol = (symbol or "").upper()
    candidates = []

    def add(candidate):
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    add(symbol)
    add(normalize_symbol_base(symbol))

    return candidates





def _load_avellaneda_params(symbol):
    log = logging.getLogger('AvellanedaLoader')
    for candidate in _parameter_file_candidates(symbol):
        file_path = os.path.join(PARAMS_DIR, f"{AVELLANEDA_FILE_PREFIX}{candidate}.json")
        if not os.path.isfile(file_path):
            continue

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except Exception as exc:
            log.warning(f"Failed to load {file_path}: {exc}")
            continue
        
        # Extract all necessary parameters
        optimal_params = payload.get("optimal_parameters", {})
        market_data = payload.get("market_data", {})
        legacy_k = _safe_float(market_data.get("k"))
        
        params = {
            "gamma": _safe_float(optimal_params.get("gamma")),
            "time_horizon_days": _safe_float(optimal_params.get("time_horizon_days")),
            "sigma": _safe_float(market_data.get("sigma")),
            "A_buy": _safe_float(market_data.get("A_buy")),
            "A_sell": _safe_float(market_data.get("A_sell")),
            "k_buy": _safe_float(market_data.get("k_buy")) or legacy_k,
            "k_sell": _safe_float(market_data.get("k_sell")) or legacy_k,
            "spread_limits_bps": resolve_avellaneda_spread_limits_bps(payload),
            "source_path": file_path
        }
        
        required_positive = ["gamma", "time_horizon_days", "sigma", "k_buy", "k_sell"]
        if not all(params[key] is not None and params[key] > 0 for key in required_positive):
            log.warning(f"File {file_path} is missing one or more positive Avellaneda parameters. Skipping.")
            continue

        return params

    return None


def get_avellaneda_params(symbol):
    """
    Abstracted function to retrieve Avellaneda-Stoikov parameters.
    Handles caching and blocks quoting until valid params exist when dynamic spreads are enabled.
    :return: A dictionary of parameters.
    """
    if not USE_AVELLANEDA_SPREADS:
        return {"buy_spread": DEFAULT_BUY_SPREAD, "sell_spread": DEFAULT_SELL_SPREAD, "source": "default"}

    symbol_key = (symbol or "").upper() or DEFAULT_SYMBOL
    now = time.time()
    cached_entry = _SPREAD_CACHE.get(symbol_key)
    if cached_entry and cached_entry.get("expires_at", 0) > now:
        return cached_entry["params"]

    log = logging.getLogger('AvellanedaLoader')
    params = _load_avellaneda_params(symbol_key)

    if params:
        # If we have full params, we don't need default spreads
        params["source"] = os.path.basename(params["source_path"])
        _SPREAD_CACHE[symbol_key] = {"params": params, "expires_at": now + SPREAD_CACHE_TTL_SECONDS}
        log.info(f"Loaded Avellaneda parameters for {symbol_key} from {params['source']}")
        return params
    else:
        unavailable_params = get_unavailable_quote_params()
        _SPREAD_CACHE[symbol_key] = {"params": unavailable_params, "expires_at": now + SPREAD_CACHE_TTL_SECONDS}
        log.warning(
            f"No valid Avellaneda parameter file found for {symbol_key}. "
            "Quoting is disabled until enough historical data has been processed."
        )
        return unavailable_params


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
    """Wait for local active-order tracking to clear during shutdown."""
    if state is None or not state.active_order_id:
        return True

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while state.active_order_id and loop.time() < deadline:
        await asyncio.sleep(0.05)
    return state.active_order_id is None


async def initiate_graceful_order_shutdown(state, runtime, timeout=SHUTDOWN_ACTIVE_ORDER_GRACE_TIMEOUT):
    """Ask the running order manager to cancel the working order before task teardown."""
    if state is None:
        return True

    runtime.request_shutdown()
    if not state.active_order_id:
        return True

    logging.info(f"Requesting graceful shutdown cancel for active order {state.active_order_id}.")
    publish_latest_order_command(state, OrderCommand(kind="cancel", trigger="Shutdown cleanup"))
    request_quote_refresh(state)
    cleared = await wait_for_active_order_clear(state, timeout)
    if cleared:
        logging.info("Active order cleared before task shutdown.")
    else:
        logging.warning(
            f"Active order {state.active_order_id} did not clear within {timeout:.1f}s. "
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
    logging.info(f"FLIP_MODE is set to: {FLIP_MODE}")

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
        state = StrategyState(flip_mode=FLIP_MODE)
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

                params_task = asyncio.create_task(avellaneda_params_updater(state, args.symbol, runtime))
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
                    params_task,
                ]
                if USE_SUPERTREND_SIGNAL:
                    support_tasks.append(asyncio.create_task(supertrend_signal_updater(state, args.symbol, runtime)))
                if USE_BINANCE_OBI_ALPHA:
                    support_tasks.append(
                        asyncio.create_task(
                            binance_orderbook_imbalance_updater(
                                state,
                                args.symbol,
                                runtime,
                                alpha_engine=alpha_engine,
                            )
                        )
                    )
                tasks.extend(support_tasks)

                if not await wait_for_startup_inputs(state, args.symbol, runtime):
                    logging.info("Shutdown requested before required startup inputs became available.")
                    return

                # Initialize Supertrend signal before checking positions or starting loops
                if USE_SUPERTREND_SIGNAL:
                    await initialize_supertrend_signal(state, args.symbol)

                try:
                    logging.info(f"Checking for existing position for {args.symbol}...")
                    positions = await client.get_position_risk(args.symbol)
                    logging.debug(f"Position risk response: {positions}")

                    position_found = False
                    if positions:
                        position_size, notional_value, _, _ = sync_state_from_position_data(
                            state,
                            positions[0],
                            reference_price=state.mid_price,
                        )

                        if has_open_position(state):
                            position_side = "LONG" if position_size > 0 else "SHORT"
                            logging.info(f"Found existing {position_side} position of size {position_size} with notional value ${notional_value:.2f}.")
                            if has_significant_position(state, notional_value):
                                logging.info(f"Starting in {state.mode} mode to close position.")
                            else:
                                logging.info("Position is below the significance threshold, but the bot will still flatten it before opening new inventory.")
                            position_found = True

                    if not position_found:
                        logging.info("No existing position found.")
                        try:
                            logging.info(f"Attempting to set leverage for {args.symbol} to {DEFAULT_LEVERAGE}x.")
                            await client.change_leverage(args.symbol, DEFAULT_LEVERAGE)
                            logging.info(f"Successfully set leverage for {args.symbol} to {DEFAULT_LEVERAGE}x.")
                        except Exception as e:
                            logging.error(f"Failed to set leverage: {e}", exc_info=True)

                        opening_mode, _ = get_strategy_modes(state.flip_mode)
                        state.mode = opening_mode
                        logging.info(f"Starting in default {opening_mode} mode.")

                except Exception as e:
                    logging.warning(f"Could not check for existing position or set leverage, starting in default {state.mode} mode: {e}", exc_info=True)

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
