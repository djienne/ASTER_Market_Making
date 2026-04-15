import argparse
import json
import math
import os
import pandas as pd
from pathlib import Path
import numpy as np
import sys

PARAMS_DIR = os.getenv("PARAMS_DIR", "params")

def _finite_nonneg(x) -> bool:
    """Check if a value is a non-negative, finite number."""
    try:
        v = float(x)
        return math.isfinite(v) and v >= 0.0
    except (ValueError, TypeError):
        return False

def save_avellaneda_params_atomic(params: dict, symbol: str) -> bool:
    """
    Atomically write parameters to a JSON file if they are valid.
    Returns True on success, False otherwise.
    """
    limit_orders = params.get("limit_orders", {})
    da = limit_orders.get("delta_a")
    db = limit_orders.get("delta_b")

    if not (_finite_nonneg(da) and _finite_nonneg(db)):
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
    default_ticker = os.getenv("SYMBOL", "BTCUSDT").upper().removesuffix("USDT")
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

def load_and_process_orderbook_data(symbol: str, target_volume_usd: float = 1000.0) -> pd.DataFrame:
    """
    Loads all Parquet order book data for a symbol and calculates the VWAP bid, ask, and mid-price.
    """
    data_dir = os.path.join(Path(__file__).parent.absolute(), 'ASTER_data', 'orderbook_parquet', f'{symbol}USDT')
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"Order book data directory not found for symbol: {symbol}USDT")

    parquet_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.parquet')]
    if not parquet_files:
        raise ValueError(f"No Parquet files found for symbol: {symbol}")

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
