import os
import asyncio
import argparse
import logging
import websockets
import json
import signal
import time
from decimal import Decimal, ROUND_DOWN
import numpy as np
from dotenv import load_dotenv
from api_client import ApiClient

# --- Configuration ---
# STRATEGY
DEFAULT_SYMBOL = "BTCUSDT"
FLIP_MODE = False # True for short-biased (SELL first), False for long-biased (BUY first)
DEFAULT_BUY_SPREAD = 0.006   # 0.6% below mid-price for buy orders
DEFAULT_SELL_SPREAD = 0.006  # 0.6% above mid-price for sell orders
USE_AVELLANEDA_SPREADS = True  # Toggle to pull spreads from Avellaneda parameter files
DEFAULT_LEVERAGE = 1
DEFAULT_BALANCE_FRACTION = 0.2  # Use a fraction of tracked wallet balance for each order
POSITION_THRESHOLD_USD = 15.0  # USD threshold before a position is treated as significant inventory

# TIMING (in seconds)
ORDER_REFRESH_INTERVAL = 30     # How long to wait before cancelling an unfilled order, in seconds.
RETRY_ON_ERROR_INTERVAL = 30    # How long to wait after a major error before retrying.
PRICE_REPORT_INTERVAL = 60      # How often to report current prices and spread to terminal.
BALANCE_REPORT_INTERVAL = 60    # How often to report account balance to terminal.
POSITION_SYNC_TIMEOUT = 2.0     # How long to wait for a position snapshot after a fill.

# ORDER REUSE SETTINGS
DEFAULT_PRICE_CHANGE_THRESHOLD = 0.0001  # 1 bp minimum price change to cancel and replace order

# SUPERTREND INTEGRATION
USE_SUPERTREND_SIGNAL = True  # Toggle to use Supertrend signal for dynamic flip_mode
SUPERTREND_PARAMS_TEMPLATE = "supertrend_params_{}.json"
SUPERTREND_CHECK_INTERVAL = 600 # Seconds between checking the signal file

# ORDER CANCELLATION
CANCEL_SPECIFIC_ORDER = True # If True, cancel specific order ID. If False, cancel all orders for the symbol.

# LOGGING
LOG_FILE = 'market_maker.log'
RELEASE_MODE = True  # When True, suppress all non-error logs and prints

MIN_ORDER_INTERVAL = 1.0  # Minimum seconds between order placements
POSITION_SIZE_EPSILON = 1e-12

# Spread configuration
PARAMS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "params")
AVELLANEDA_FILE_PREFIX = "avellaneda_parameters_"
SPREAD_MIN_THRESHOLD = 0.00005  # 0.005%
SPREAD_MAX_THRESHOLD = 0.02     # 2%
SPREAD_CACHE_TTL_SECONDS = 10
_SPREAD_CACHE = {}


def resolve_symbol(cli_symbol=None):
    """Resolve the active symbol from CLI input, env var, or the hardcoded default."""
    symbol = cli_symbol or os.getenv("SYMBOL") or DEFAULT_SYMBOL
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
        # WebSocket connection health flags
        self.price_ws_connected = False
        self.user_data_ws_connected = False
        # Supertrend signal
        self.supertrend_signal = None # Can be 1 (up) or -1 (down)
        # Position snapshots are the source of truth for inventory state
        self.position_update_seq = 0


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


def round_price_to_tick(price, tick_size, side):
    """Round prices to a passive tick for the given side."""
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")

    scaled = price / tick_size
    if side == 'BUY':
        rounded = np.floor(scaled + 1e-12) * tick_size
    elif side == 'SELL':
        rounded = np.ceil(scaled - 1e-12) * tick_size
    else:
        raise ValueError(f"Unsupported side for price rounding: {side}")

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


async def cancel_active_order(state, client, symbol, log, reason):
    """Cancel the currently tracked order and only clear local tracking on success."""
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


async def websocket_price_updater(state, symbol, runtime):
    """[MODIFIED] WebSocket-based price updater with exponential backoff and stale connection detection."""
    log = logging.getLogger('WebSocketPriceUpdater')

    websocket_url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@depth5"
    reconnect_delay = 5  # Initial delay
    max_reconnect_delay = 60 # Maximum wait time

    while not runtime.shutdown_requested:
        try:
            log.info(f"Connecting to WebSocket: {websocket_url}")
            state.price_ws_connected = False # Mark as disconnected while attempting

            async with websockets.connect(websocket_url, ping_interval=20, ping_timeout=10) as websocket:
                log.info(f"WebSocket connected for {symbol} depth stream")
                state.price_ws_connected = True # Mark as connected
                reconnect_delay = 5  # Reset reconnect delay on successful connection
                last_message_time = runtime.now()

                while not runtime.shutdown_requested:
                    try:
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
                                    mid_price = (best_bid + best_ask) / 2

                                    state.bid_price = best_bid
                                    state.ask_price = best_ask
                                    state.mid_price = mid_price
                                    runtime.price_last_updated = runtime.now()

                                    log.debug(f"Updated prices for {symbol}: Bid={best_bid}, Ask={best_ask}, Mid={mid_price:.4f}")

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

        if not runtime.shutdown_requested:
            log.info(f"Reconnecting to price WebSocket in {reconnect_delay:.1f}s...")
            await asyncio.sleep(reconnect_delay)
            # [MODIFIED] Implement exponential backoff
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    log.info("WebSocket price updater shutting down")

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
    apiv1_public = os.getenv('APIV1_PUBLIC_KEY')
    apiv1_private = os.getenv('APIV1_PRIVATE_KEY')

    while not runtime.shutdown_requested and state.balance_listen_key:
        try:
            # Sleep for 10 minutes (listen key expires in 60 minutes)
            await asyncio.sleep(600)

            if runtime.shutdown_requested or not state.balance_listen_key:
                break

            log.info("Sending keepalive for balance listen key...")
            await client.signed_request(
                "PUT", "/fapi/v1/listenKey", {},
                use_binance_auth=True,
                api_key=apiv1_public,
                api_secret=apiv1_private
            )
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
            apiv1_public = os.getenv('APIV1_PUBLIC_KEY')
            apiv1_private = os.getenv('APIV1_PRIVATE_KEY')

            if not all([apiv1_public, apiv1_private]):
                log.error("Missing APIV1_PUBLIC_KEY or APIV1_PRIVATE_KEY for user data stream.")
                await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                continue

            response = await client.signed_request(
                "POST", "/fapi/v1/listenKey", {},
                use_binance_auth=True,
                api_key=apiv1_public,
                api_secret=apiv1_private
            )
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
                reconnect_delay = 5  # Reset reconnect delay on successful connection

                while not runtime.shutdown_requested:
                    try:
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

                log.info(f"{symbol} | Mid-Price: ${state.mid_price:.4f} | Bid-Ask Spread: {spread_percentage:.3f}% | Bid: ${state.bid_price:.4f} | Ask: ${state.ask_price:.4f}{balance_info}")

        except Exception as e:
            log.error(f"Error in price reporter: {e}")

    log.info("Price reporter shutting down")


async def initialize_supertrend_signal(state, symbol):
    """Reads the Supertrend signal file once at startup to set the initial state."""
    log = logging.getLogger('SupertrendInitializer')
    
    # Determine the symbol for the filename (e.g., BTC from BTCUSDT)
    filename_symbol = symbol[:-4] if symbol.endswith('USDT') else symbol
    params_file = os.path.join(PARAMS_DIR, SUPERTREND_PARAMS_TEMPLATE.format(filename_symbol))

    try:
        if os.path.exists(params_file):
            with open(params_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            initial_signal = data.get('current_signal', {}).get('trend')
            
            if initial_signal in [1, -1]:
                state.supertrend_signal = initial_signal
                # Update flip_mode based on the initial signal
                # Downtrend (-1) -> flip_mode = True (short-biased)
                # Uptrend (+1) -> flip_mode = False (long-biased)
                new_flip_mode = (initial_signal == -1)
                if state.flip_mode != new_flip_mode:
                    state.flip_mode = new_flip_mode
                    sync_mode_with_position(state)
                    log.info(f"Initialized Supertrend signal to: {'UPTREND (+1)' if initial_signal == 1 else 'DOWNTREND (-1)'}")
                    log.info(f"Initial strategy bias set by signal: FLIP_MODE -> {state.flip_mode}")
                else:
                    log.info(f"Initial Supertrend signal confirms default bias: FLIP_MODE -> {state.flip_mode}")
            else:
                log.warning(f"Invalid initial signal '{initial_signal}' in {params_file}. Using default FLIP_MODE={state.flip_mode}.")
        else:
            log.warning(f"Supertrend params file not found at {params_file}. Using default FLIP_MODE={state.flip_mode}.")
    except Exception as e:
        log.error(f"Error initializing Supertrend signal: {e}. Using default FLIP_MODE={state.flip_mode}.")


async def supertrend_signal_updater(state, symbol, runtime):
    """Periodically reads the Supertrend signal file and updates the strategy state."""
    log = logging.getLogger('SupertrendUpdater')
    
    # Determine the symbol for the filename (e.g., BTC from BTCUSDT)
    filename_symbol = symbol[:-4] if symbol.endswith('USDT') else symbol
    params_file = os.path.join(PARAMS_DIR, SUPERTREND_PARAMS_TEMPLATE.format(filename_symbol))

    while not runtime.shutdown_requested:
        try:
            if os.path.exists(params_file):
                with open(params_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                new_signal = data.get('current_signal', {}).get('trend')
                
                if new_signal in [1, -1]:
                    if state.supertrend_signal != new_signal:
                        state.supertrend_signal = new_signal
                        log.info(f"Supertrend signal updated to: {'UPTREND (+1)' if new_signal == 1 else 'DOWNTREND (-1)'}")
                else:
                    log.warning(f"Invalid signal '{new_signal}' in {params_file}. Defaulting to UPTREND (+1).")
                    state.supertrend_signal = 1 # Default to uptrend on invalid signal
            else:
                if state.supertrend_signal != 1: # Only log if it's a change
                    log.warning(f"Supertrend params file not found at {params_file}. Defaulting to UPTREND (+1).")
                    state.supertrend_signal = 1 # Default to uptrend if file not found

            await asyncio.sleep(SUPERTREND_CHECK_INTERVAL)

        except json.JSONDecodeError:
            log.error(f"Error decoding JSON from {params_file}. Defaulting to UPTREND (+1).")
            state.supertrend_signal = 1
            await asyncio.sleep(SUPERTREND_CHECK_INTERVAL)
        except Exception as e:
            log.error(f"An error occurred in the Supertrend signal updater: {e}. Defaulting to UPTREND (+1).")
            state.supertrend_signal = 1
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


def classify_order_update(order_data, fill_notional_threshold=POSITION_THRESHOLD_USD):
    """Classify an order update into terminal/non-terminal and fill/non-fill outcomes."""
    status = order_data.get('X')
    filled_qty = float(order_data.get('z', 0.0))

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

    clear_order_tracking(state)
    log.debug(f"Adding 0.01s delay after {order_label.lower()} terminal update")
    await asyncio.sleep(0.01)


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
    for suffix in ("USDT", "USDC", "USDF", "USD1", "USD"):
        if symbol.endswith(suffix) and len(symbol) > len(suffix):
            add(symbol[:-len(suffix)])

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
    Handles caching and falls back to default spreads if needed.
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
        # Fallback to default spreads if file not found or invalid
        default_params = {"buy_spread": DEFAULT_BUY_SPREAD, "sell_spread": DEFAULT_SELL_SPREAD, "source": "default"}
        _SPREAD_CACHE[symbol_key] = {"params": default_params, "expires_at": now + SPREAD_CACHE_TTL_SECONDS}
        log.info(f"No Avellaneda parameter file found for {symbol_key}; using default spreads.")
        return default_params


async def market_making_loop(state, client, symbol, runtime):
    """The main market making logic loop."""
    log = logging.getLogger('MarketMakerLoop')
    log.info(f"Fetching trading rules for {symbol}...")
    symbol_filters = await client.get_symbol_filters(symbol)
    log.info(f"Filters loaded: {symbol_filters}")

    while not runtime.shutdown_requested:
        try:
            opening_mode, _ = get_strategy_modes(state.flip_mode)

            # We do not trade without both WebSockets because price freshness and order-state
            # monitoring are both safety-critical.
            if not state.price_ws_connected or not state.user_data_ws_connected:
                ws_status = f"Price_WS_Connected={state.price_ws_connected}, User_Data_WS_Connected={state.user_data_ws_connected}"
                log.warning(f"A WebSocket is disconnected ({ws_status}). Pausing trading logic.")

                if state.active_order_id:
                    await cancel_active_order(state, client, symbol, log, "WebSocket disconnection")
                 
                await asyncio.sleep(1) # Wait before checking again
                continue

            # --- Supertrend Signal Integration ---
            if USE_SUPERTREND_SIGNAL and state.supertrend_signal is not None:
                # Check if there is no significant open position
                current_notional = abs(state.position_size * state.mid_price) if state.mid_price else 0
                if current_notional < POSITION_THRESHOLD_USD:
                    # Downtrend signal (-1) means we should be short-biased (SELL first) -> flip_mode = True
                    # Uptrend signal (+1) means we should be long-biased (BUY first) -> flip_mode = False
                    new_flip_mode = (state.supertrend_signal == -1)
                    
                    if state.flip_mode != new_flip_mode:
                        log.info(f"Supertrend signal changed to {'DOWNTREND' if new_flip_mode else 'UPTREND'}.")
                        log.info(f"Position is flat. Adjusting strategy bias: FLIP_MODE -> {new_flip_mode}")
                        state.flip_mode = new_flip_mode
                        opening_mode, _ = get_strategy_modes(state.flip_mode)
                        sync_mode_with_position(state, position_notional=current_notional)
                else:
                    log.debug(f"Supertrend signal is {'DOWNTREND' if state.supertrend_signal == -1 else 'UPTREND'}, but position is open (${current_notional:.2f}). Holding current strategy bias.")
            log.debug(f"Relying on WebSocket for position state. Current size: {state.position_size:.6f}, Mode: {state.mode}")

            # --- Secondary checks for fresh data ---
            if not is_price_data_valid(state, runtime):
                log.info("Waiting for valid price data from WebSocket...")
                await asyncio.sleep(2)
                continue

            if not is_balance_data_valid(state):
                log.info("Waiting for valid balance data from WebSocket...")
                await asyncio.sleep(2)
                continue

            current_position_notional = get_position_notional_usd(state.position_size, state.mid_price)
            close_side = get_close_side_for_trading(state)
            if close_side and not has_significant_position(state, current_position_notional):
                log.debug(
                    f"Residual position detected ({state.position_size:.8f}, ${current_position_notional:.2f}). "
                    "Flattening it before opening new inventory."
                )

            # --- Determine Strategy and Parameters ---
            params = get_avellaneda_params(symbol)

            # Use full Avellaneda-Stoikov logic if params are from a file
            if params["source"] != "default":
                # Unpack parameters
                gamma = params["gamma"]
                sigma = params["sigma"]
                k_buy = params["k_buy"]
                k_sell = params["k_sell"]
                T = params["time_horizon_days"]
                q, s = state.position_size, state.mid_price

                risk_term = gamma * ((sigma * s)**2) * T
                reservation_price = s - q * risk_term
                ask_offset = (1 / gamma) * np.log(1 + (gamma / k_buy)) + (risk_term / 2.0)
                bid_offset = (1 / gamma) * np.log(1 + (gamma / k_sell)) + (risk_term / 2.0)
                ask_price = reservation_price + ask_offset
                bid_price = reservation_price - bid_offset
                log.info(
                    f"Avellaneda quotes: r={reservation_price:.4f}, bid={bid_price:.4f}, "
                    f"ask={ask_price:.4f}, k_buy={k_buy:.6f}, k_sell={k_sell:.6f}"
                )

                if close_side:
                    side, reduce_only = close_side, True
                    quantity_to_trade = abs(state.position_size)
                    limit_price = ask_price if side == 'SELL' else bid_price
                else: # Opening mode
                    side, reduce_only = opening_mode, False
                    quantity_to_trade = (state.account_balance * DEFAULT_BALANCE_FRACTION) / state.mid_price
                    limit_price = bid_price if opening_mode == 'BUY' else ask_price
                
                used_spread = (ask_price - bid_price) / s if s > 0 else 0

            # Fallback to simple spread logic
            else:
                buy_spread, sell_spread = params["buy_spread"], params["sell_spread"]
                log.info(f"Using default spreads: buy={buy_spread:.4%}, sell={sell_spread:.4%}")

                if close_side:
                    side, reduce_only = close_side, True
                    quantity_to_trade = abs(state.position_size)
                    limit_price = state.mid_price * (1 + sell_spread) if side == 'SELL' else state.mid_price * (1 - buy_spread)
                else:  # Opening mode
                    side, reduce_only = opening_mode, False
                    quantity_to_trade = (state.account_balance * DEFAULT_BALANCE_FRACTION) / state.mid_price
                    limit_price = state.mid_price * (1 - buy_spread) if opening_mode == 'BUY' else state.mid_price * (1 + sell_spread)
                
                used_spread = sell_spread if side == 'SELL' else buy_spread

            log.info(f"Calculated order parameters: side={side}, quantity={quantity_to_trade:.8f}, price={limit_price:.8f}, reduce_only={reduce_only}")
            log.debug(f"Market data: mid_price={state.mid_price:.8f}, bid={state.bid_price:.8f}, ask={state.ask_price:.8f}, using_spread={used_spread:.6f}")

            # --- Adjust order to conform to exchange filters ---
            log.debug(f"Symbol filters: {symbol_filters}")
            rounded_price = round_price_to_tick(limit_price, symbol_filters['tick_size'], side)
            formatted_price = f"{rounded_price:.{symbol_filters['price_precision']}f}"
            log.debug(f"Price adjustment: {limit_price:.8f} -> {rounded_price:.8f} -> {formatted_price}")

            rounded_quantity = round_quantity_to_step(quantity_to_trade, symbol_filters['step_size'])
            formatted_quantity = f"{rounded_quantity:.{symbol_filters['quantity_precision']}f}"
            log.info(f"Adjusted order: price={formatted_price}, quantity={formatted_quantity}")
            log.debug(f"Quantity adjustment: {quantity_to_trade:.8f} -> {rounded_quantity:.8f} -> {formatted_quantity}")

            if float(formatted_quantity) <= 0:
                log.warning(f"Calculated quantity is zero or negative: {formatted_quantity}. Skipping cycle.")
                await asyncio.sleep(ORDER_REFRESH_INTERVAL)
                continue

            min_qty = symbol_filters['min_qty']
            if float(formatted_quantity) + POSITION_SIZE_EPSILON < min_qty:
                order_kind = "reduce-only" if reduce_only else "opening"
                log.warning(
                    f"Order quantity too small: {formatted_quantity} < {min_qty:.{symbol_filters['quantity_precision']}f} "
                    f"(minQty) for {order_kind} order. Skipping cycle."
                )
                await asyncio.sleep(ORDER_REFRESH_INTERVAL)
                continue

            order_notional = float(formatted_price) * float(formatted_quantity)
            min_notional = symbol_filters['min_notional']
            if order_notional < min_notional:
                log.warning(f"Order notional too small: ${order_notional:.2f} < ${min_notional:.2f} (min required). Skipping cycle.")
                log.debug(f"Notional calculation: {formatted_price} * {formatted_quantity} = ${order_notional:.2f}")
                await asyncio.sleep(ORDER_REFRESH_INTERVAL)
                continue

            log.debug(f"Order validation passed: notional=${order_notional:.2f} >= ${min_notional:.2f}")

            # --- Check if we can reuse existing order ---
            if should_reuse_order(state, float(formatted_price), side, float(formatted_quantity)):
                price_change_pct = abs(float(formatted_price) - state.last_order_price) / state.last_order_price * 100
                log.info(f"Reusing existing order {state.active_order_id}: price change {price_change_pct:.4f}% < {DEFAULT_PRICE_CHANGE_THRESHOLD*100:.2f}% threshold")

                # Continue monitoring the existing order
                try:
                    log.debug(f"Continuing to monitor existing order {state.active_order_id} via WebSocket with timeout {ORDER_REFRESH_INTERVAL}s")
                    order_id = state.active_order_id
                    position_update_seq_before_fill = state.position_update_seq
                    terminal_update = await wait_for_terminal_order_update(
                        state.order_updates,
                        order_id,
                        ORDER_REFRESH_INTERVAL,
                        log,
                        "Monitored",
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
                        "Reused order",
                    )

                except asyncio.TimeoutError:
                    log.info(f"Reused order {state.active_order_id} not filled within {ORDER_REFRESH_INTERVAL}s. Will evaluate for replacement in next cycle.")
                    await asyncio.sleep(0.01)

                continue  # Skip to next iteration

            # --- Rate Limiting Protection ---
            current_time = runtime.now()
            time_since_last_order = current_time - runtime.last_order_time

            if time_since_last_order < MIN_ORDER_INTERVAL:
                wait_time = MIN_ORDER_INTERVAL - time_since_last_order
                log.info(f"Rate limiting: waiting {wait_time:.1f}s before placing order")
                await asyncio.sleep(wait_time)

            # --- Cancel existing order if we're placing a new one ---
            if state.active_order_id:
                log.info(f"Cancelling existing order {state.active_order_id} to place new order")
                if not await cancel_active_order(state, client, symbol, log, "Order replacement"):
                    await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                    continue

            # --- Place and Monitor Order ---
            percentage_diff = (float(formatted_price) - state.mid_price) / state.mid_price * 100
            log.info(f"Placing {side} order: {formatted_quantity} {symbol} @ {formatted_price} ({percentage_diff:+.4f}% from mid-price)")
            log.info(f"Order details: symbol={symbol}, price={formatted_price}, quantity={formatted_quantity}, side={side}, reduceOnly={reduce_only}")

            try:
                active_order = await client.place_order(symbol, formatted_price, formatted_quantity, side, reduce_only)
                runtime.last_order_time = runtime.now()
                state.active_order_id = active_order.get('orderId')

                # Track order details for reuse logic
                state.last_order_price = float(formatted_price)
                state.last_order_side = side
                state.last_order_quantity = float(formatted_quantity)

                log.info(f"Order placed successfully: ID={state.active_order_id}")
                log.debug(f"Full order response: {active_order}")
            except Exception as order_error:
                log.error(f"Failed to place order: {order_error}")
                log.error(f"Order parameters: symbol={symbol}, price={formatted_price}, quantity={formatted_quantity}, side={side}, reduceOnly={reduce_only}")
                raise

            try:
                log.debug(f"Waiting for WebSocket update for order {state.active_order_id} with timeout {ORDER_REFRESH_INTERVAL}s")
                order_id = state.active_order_id
                position_update_seq_before_fill = state.position_update_seq
                terminal_update = await wait_for_terminal_order_update(
                    state.order_updates,
                    order_id,
                    ORDER_REFRESH_INTERVAL,
                    log,
                    "Placed",
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
                    "Order",
                )

            except asyncio.TimeoutError:
                log.info(f"Order {state.active_order_id} not filled within {ORDER_REFRESH_INTERVAL}s. Cancelling and refreshing.")
                if not await cancel_active_order(state, client, symbol, log, "Timed-out order refresh"):
                    await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)
                    continue

                log.debug("Adding 0.1s delay after order timeout to avoid API rate limits")
                await asyncio.sleep(0.01)

        except asyncio.TimeoutError:
            log.warning("Timeout in main loop. Continuing...")
            await asyncio.sleep(0.01)
        except Exception as e:
            log.error(f"An error occurred in the main loop: {e}", exc_info=True)
            log.error(f"Current state: mode={state.mode}, position_size={state.position_size}, active_order_id={state.active_order_id}")
            log.error(f"Market data: mid_price={state.mid_price}, bid={state.bid_price}, ask={state.ask_price}")

            # Try to cancel any outstanding orders
            if state.active_order_id:
                await cancel_active_order(state, client, symbol, log, "Error handling")

            log.info(f"Waiting for {RETRY_ON_ERROR_INTERVAL} seconds before retrying...")
            await asyncio.sleep(RETRY_ON_ERROR_INTERVAL)



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


async def cleanup_orders(symbol, api_user, api_signer, api_private_key):
    """Cleanup function to cancel all orders"""
    try:
        logging.info(f"Performing final cleanup: Cancelling all orders for {symbol}.")
        async with ApiClient(api_user, api_signer, api_private_key, RELEASE_MODE) as cleanup_client:
            await cleanup_client.cancel_all_orders(symbol)
        logging.info("All open orders cancelled. Shutdown complete.")
    except Exception as e:
        logging.error(f"Error during final order cancellation: {e}")

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
        help="The symbol to trade. Defaults to SYMBOL in .env or BTCUSDT.",
    )
    args = parser.parse_args()

    setup_logging("INFO")
    load_dotenv()
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
    tasks = []

    try:
        client = ApiClient(API_USER, API_SIGNER, API_PRIVATE_KEY, RELEASE_MODE)
        state = StrategyState(flip_mode=FLIP_MODE)

        async with client:
            try:
                logging.info(f"Sending initial cancel all orders for {args.symbol} to ensure a clean slate.")
                await client.cancel_all_orders(args.symbol)
            except Exception as e:
                logging.warning(f"Failed to send initial cancel all orders, proceeding anyway: {e}")

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
            mm_task = asyncio.create_task(market_making_loop(state, client, args.symbol, runtime))
            tasks = [
                asyncio.create_task(websocket_price_updater(state, args.symbol, runtime)),
                asyncio.create_task(websocket_user_data_updater(state, client, args.symbol, runtime)),
                asyncio.create_task(balance_reporter(state, runtime)),
                mm_task,
                asyncio.create_task(price_reporter(state, args.symbol, runtime)),
            ]
            if USE_SUPERTREND_SIGNAL:
                tasks.append(asyncio.create_task(supertrend_signal_updater(state, args.symbol, runtime)))

            # Wait for either the market making task to complete or shutdown signal
            while not runtime.shutdown_requested and not mm_task.done():
                await asyncio.sleep(0.01)

    except asyncio.CancelledError:
        logging.info("Main task was cancelled.")
    except Exception as e:
        logging.error(f"An unhandled exception occurred in main: {e}", exc_info=True)
    finally:
        logging.info("Shutdown initiated. Cleaning up...")
        for task in tasks:
            if not task.done():
                task.cancel()
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Always perform cleanup
        await cleanup_orders(args.symbol, API_USER, API_SIGNER, API_PRIVATE_KEY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown requested by user (Ctrl+C).")
