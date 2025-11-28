import argparse
import json
import math
import os
import pandas as pd

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
    with open(tmp_path, "w") as f:
        json.dump(params, f, indent=4)
    
    os.replace(tmp_path, final_path)
    return True

def parse_arguments():
    """Parse command-line arguments for the script."""
    parser = argparse.ArgumentParser(description='Calculate Avellaneda-Stoikov market making parameters')
    parser.add_argument('ticker', nargs='?', default='BNB', help='Ticker symbol (default: BNB)')
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

def load_and_resample_mid_price(csv_path: str) -> pd.DataFrame:
    """Load and preprocess mid-price data from a CSV file."""
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['unix_timestamp_ms'], unit='ms')
    
    if df['datetime'].duplicated().any():
        df.drop_duplicates(subset=['datetime'], keep='last', inplace=True)

    df = df.set_index('datetime')
    df['price_bid'] = df['bid']
    df['price_ask'] = df['ask']
    
    merged = df[['price_bid', 'price_ask']].resample('s').ffill()
    merged['mid_price'] = (merged['price_bid'] + merged['price_ask']) / 2
    merged.dropna(inplace=True)
    return merged
