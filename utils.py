import argparse
import json
import math
import os
import pandas as pd
from pathlib import Path
import numpy as np
import sys

PARAMS_DIR = os.getenv("PARAMS_DIR", "params")
QUOTE_SUFFIXES = ("USDT", "USDC", "USDF", "USD1", "USD")

def _finite_nonneg(x) -> bool:
    """Check if a value is a non-negative, finite number."""
    try:
        v = float(x)
        return math.isfinite(v) and v >= 0.0
    except (ValueError, TypeError):
        return False


def _finite_positive(x) -> bool:
    """Check if a value is a positive, finite number."""
    try:
        v = float(x)
        return math.isfinite(v) and v > 0.0
    except (ValueError, TypeError):
        return False


def normalize_symbol_base(symbol: str) -> str:
    """Strip common stablecoin quote suffixes from a symbol."""
    symbol = (symbol or "").upper()
    for suffix in QUOTE_SUFFIXES:
        if symbol.endswith(suffix) and len(symbol) > len(suffix):
            return symbol[:-len(suffix)]
    return symbol


def quote_symbol_candidates(symbol: str) -> list[str]:
    """Return preferred full-symbol candidates for a base or fully qualified symbol."""
    symbol = (symbol or "").upper()
    if not symbol:
        return []

    base_symbol = normalize_symbol_base(symbol)
    if symbol != base_symbol:
        return [symbol]

    return [f"{base_symbol}{suffix}" for suffix in QUOTE_SUFFIXES]


def resolve_orderbook_symbol(symbol: str, data_root: Path | None = None) -> str:
    """Resolve which full symbol directory to use for local orderbook parquet data."""
    root = Path(data_root) if data_root is not None else Path(__file__).parent.absolute() / 'ASTER_data' / 'orderbook_parquet'
    candidates = quote_symbol_candidates(symbol)
    for candidate in candidates:
        if (root / candidate).is_dir():
            return candidate

    return candidates[0] if candidates else ""


def resolve_trades_csv_path(symbol: str, data_root: Path | None = None) -> tuple[Path, str]:
    """Resolve the local trades CSV path for a base or fully qualified symbol."""
    root = Path(data_root) if data_root is not None else Path(__file__).parent.absolute() / 'ASTER_data'
    candidates = quote_symbol_candidates(symbol)
    for candidate in candidates:
        csv_path = root / f"trades_{candidate}.csv"
        if csv_path.is_file():
            return csv_path, candidate

    fallback_symbol = candidates[0] if candidates else ""
    return root / f"trades_{fallback_symbol}.csv", fallback_symbol


def _has_valid_runtime_avellaneda_fields(params: dict) -> bool:
    """Validate the subset of Avellaneda params required by the runtime loader."""
    optimal_parameters = params.get("optimal_parameters", {})
    market_data = params.get("market_data", {})
    legacy_k = market_data.get("k")

    required_positive = (
        optimal_parameters.get("gamma"),
        optimal_parameters.get("time_horizon_days"),
        market_data.get("sigma"),
        market_data.get("k_buy", legacy_k),
        market_data.get("k_sell", legacy_k),
    )

    return all(_finite_positive(value) for value in required_positive)

def save_avellaneda_params_atomic(params: dict, symbol: str) -> bool:
    """
    Atomically write parameters to a JSON file if they are valid.
    Returns True on success, False otherwise.
    """
    limit_orders = params.get("limit_orders", {})
    da = limit_orders.get("delta_a")
    db = limit_orders.get("delta_b")

    if not (_finite_nonneg(da) and _finite_nonneg(db) and _has_valid_runtime_avellaneda_fields(params)):
        return False

    final_path = os.path.join(PARAMS_DIR, f"avellaneda_parameters_{symbol}.json")
    tmp_path = f"{final_path}.tmp"

    os.makedirs(PARAMS_DIR, exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(params, f, indent=4)
    
    os.replace(tmp_path, final_path)
    return True

def parse_arguments():
    """Parse command-line arguments for the script."""
    parser = argparse.ArgumentParser(description='Calculate Avellaneda-Stoikov market making parameters')
    default_ticker = normalize_symbol_base(os.getenv("SYMBOL", "BTCUSDT"))
    parser.add_argument('ticker', nargs='?', default=default_ticker, help=f'Ticker symbol (default: {default_ticker})')
    parser.add_argument('--minutes', type=int, default=5, help='Frequency in minutes for parameter recalculation (default: 5)')
    return parser.parse_args()

def get_fallback_tick_size(ticker: str) -> float:
    """Get a default tick size for a given ticker symbol."""
    tick_sizes = {
        'BTC': 0.1, 'ETH': 0.1, 'SOL': 0.01,
        'WLFI': 0.0001, 'PAXG': 0.01, 'ASTER': 0.0001, 'BNB': 0.1
    }
    return tick_sizes.get(ticker, 0.01)

def load_trades_data(csv_path: str) -> pd.DataFrame:
    """Load and preprocess trades data from a CSV file."""
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['unix_timestamp_ms'], unit='ms')
    df = df.set_index('datetime')
    return df


def select_recent_orderbook_parquet_files(parquet_files, lookback_seconds=None):
    """Select the most recent parquet snapshots relative to the newest timestamped file."""
    timestamped_files = []
    passthrough_files = []
    latest_files = []

    for file_path in parquet_files:
        stem = Path(file_path).stem
        if stem == "_latest":
            latest_files.append(file_path)
            continue
        if stem.isdigit():
            timestamped_files.append((int(stem), file_path))
            continue
        passthrough_files.append(file_path)

    timestamped_files.sort(key=lambda item: item[0])
    if lookback_seconds is not None and lookback_seconds > 0 and timestamped_files:
        latest_timestamp_ms = timestamped_files[-1][0]
        earliest_timestamp_ms = latest_timestamp_ms - int(lookback_seconds * 1000)
        timestamped_files = [
            (timestamp_ms, file_path)
            for timestamp_ms, file_path in timestamped_files
            if timestamp_ms >= earliest_timestamp_ms
        ]

    return passthrough_files + [file_path for _, file_path in timestamped_files] + latest_files


def load_and_process_orderbook_data(symbol: str, target_volume_usd: float = 1000.0, lookback_seconds=None) -> pd.DataFrame:
    """
    Loads all Parquet order book data for a symbol and calculates the VWAP bid, ask, and mid-price.
    """
    orderbook_root = Path(__file__).parent.absolute() / 'ASTER_data' / 'orderbook_parquet'
    resolved_symbol = resolve_orderbook_symbol(symbol, data_root=orderbook_root)
    data_dir = orderbook_root / resolved_symbol
    if not os.path.isdir(data_dir):
        attempted = ", ".join(quote_symbol_candidates(symbol))
        raise FileNotFoundError(
            f"Order book data directory not found for symbol '{symbol}'. Tried: {attempted}"
        )

    parquet_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.parquet')]
    parquet_files = select_recent_orderbook_parquet_files(parquet_files, lookback_seconds=lookback_seconds)
    if not parquet_files:
        raise ValueError(f"No Parquet files found for symbol: {resolved_symbol}")

    # Read all parquet files, including the staging file, into a single DataFrame
    df = pd.concat([pd.read_parquet(f) for f in parquet_files])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.set_index('datetime')

    # Drop duplicates on hashable columns first
    df.drop_duplicates(subset=['timestamp', 'lastUpdateId'], inplace=True)
    df.sort_index(inplace=True)

    # Parse bids/asks: handle both JSON strings (from older format) and arrays (from parquet)
    def parse_orderbook_side(data):
        """Parse orderbook data - handle both JSON strings and arrays/lists."""
        if isinstance(data, str):
            return json.loads(data)
        elif isinstance(data, (list, np.ndarray)):
            # Convert numpy arrays to regular lists for consistency
            return [list(order) if isinstance(order, np.ndarray) else order for order in data]
        return data

    df['bids'] = df['bids'].apply(parse_orderbook_side)
    df['asks'] = df['asks'].apply(parse_orderbook_side)

    df['vwap_bid'] = df['bids'].apply(lambda x: calculate_vwap(x, target_volume_usd))
    df['vwap_ask'] = df['asks'].apply(lambda x: calculate_vwap(x, target_volume_usd))
    df['mid_price'] = (df['vwap_bid'] + df['vwap_ask']) / 2
    
    # Resample to 1-second frequency and forward-fill to ensure data continuity
    df_resampled = df[['vwap_bid', 'vwap_ask', 'mid_price']].resample('s').ffill()
    
    # Return both the original (but cleaned) and the processed dataframes
    return df, df_resampled

def calculate_vwap(orders, target_volume):
    """Calculates the volume-weighted average price for one side of the order book."""
    quote_volume_seen = 0.0
    base_quantity_filled = 0.0
    for order in orders:
        if isinstance(order, (list, tuple)) and len(order) >= 2:
            price, qty = order
            volume_usd = price * qty

            # If the first level alone exceeds the target, its price is the VWAP
            if quote_volume_seen == 0 and volume_usd >= target_volume:
                return price

            if quote_volume_seen + volume_usd >= target_volume:
                remaining_quote = target_volume - quote_volume_seen
                base_quantity_filled += remaining_quote / price
                quote_volume_seen = target_volume
                break

            base_quantity_filled += qty
            quote_volume_seen += volume_usd

    return quote_volume_seen / base_quantity_filled if base_quantity_filled > 0 else np.nan
