import numpy as np
import pandas as pd
import sys
import os
from pathlib import Path
import logging

# Import modularized functions
from utils import (
    parse_arguments, get_fallback_tick_size, load_and_process_orderbook_data,
    load_trades_data, save_avellaneda_params_atomic
)
from volatility import calculate_volatility
from intensity import calculate_intensity_params
from backtester import optimize_params

# Configure logging
logging.getLogger('numba').setLevel(logging.WARNING)

# Constants
RECENT_PARAM_PERIODS = 4
TICKER = ""  # Global ticker symbol

def calculate_final_quotes(gamma, sigma, A, k, time_horizon, mid_price_df, ma_window, window_minutes, garch_sigma, rolling_sigma):
    """Calculate the final reservation price and quotes for the current state."""
    s = mid_price_df['mid_price'].iloc[-1]
    q = 1.0  # Placeholder for current inventory, should be replaced by live data in the bot

    spread_base = gamma * sigma**2 * time_horizon + (2 / gamma) * np.log(1 + (gamma / k))
    half_spread = spread_base / 2.0
    r = s - q * gamma * sigma**2 * time_horizon
    gap = abs(r - s)

    delta_a, delta_b = (half_spread + gap, half_spread - gap) if r >= s else (half_spread - gap, half_spread + gap)
    r_a, r_b = r + delta_a, r - delta_b
    
    return {
        "ticker": TICKER,
        "timestamp": pd.Timestamp.now().isoformat(),
        "market_data": {
            "mid_price": float(s), "sigma": float(sigma), "A": float(A), "k": float(k),
            "garch_sigma": float(garch_sigma), "rolling_sigma": float(rolling_sigma)
        },
        "optimal_parameters": {"gamma": float(gamma), "time_horizon_days": float(time_horizon)},
        "current_state": {"inventory": int(q), "minutes_window": window_minutes, "ma_window": ma_window},
        "calculated_values": {"reservation_price": float(r), "gap": float(gap)},
        "limit_orders": {
            "ask_price": float(r_a), "bid_price": float(r_b), 
            "delta_a": float(delta_a), "delta_b": float(delta_b),
            "delta_a_percent": (delta_a / s) * 100.0, "delta_b_percent": (delta_b / s) * 100.0
        }
    }

def print_summary(results: dict, periods: list, df=None):
    """Print a formatted summary of the calculated parameters."""
    # Handle the case of insufficient data first
    if not results or 'market_data' not in results:
        print("\n" + "="*80 + "\nInsufficient data for robust parameter estimation.\n")
        if df is not None and not df.empty:
            minutes_window = results.get('current_state', {}).get('minutes_window', 5) # Default to 5 if not available
            print(f"  - Data points available: {len(df)}")
            print(f"  - Time range: {df.index.min()} to {df.index.max()}")
            print(f"  - Analysis periods formed: {len(periods)} (requires at least 2 periods of {minutes_window} minutes each for backtesting)")
        print("="*80)
        return
        
    print(f"\n{'='*80}\nAVELLANEDA-STOIKOV PARAMETERS - {results['ticker']}\n{'='*80}")

    # Display analysis information
    minutes_window = results.get('current_state', {}).get('minutes_window', 5)
    num_periods = len(periods)
    print(f"Analysis Window:\n  Period length: {minutes_window} minutes")
    print(f"  Number of periods analyzed: {num_periods}")
    if df is not None and not df.empty:
        print(f"  Data range: {df.index.min()} to {df.index.max()}\n")
    else:
        print()

    print(f"Market Data:\n  Mid Price: ${results['market_data']['mid_price']:,.4f}")
    
    garch_sigma = results['market_data']['garch_sigma']
    rolling_sigma = results['market_data']['rolling_sigma']
    final_sigma = results['market_data']['sigma']
    
    garch_str = f"{garch_sigma:.6f}" if pd.notna(garch_sigma) else "N/A"
    rolling_str = f"{rolling_sigma:.6f}" if pd.notna(rolling_sigma) else "N/A"
    
    picked_garch = pd.notna(garch_sigma) and final_sigma == garch_sigma
    picked_rolling = pd.notna(rolling_sigma) and final_sigma == rolling_sigma

    print(f"  GARCH Volatility:   {garch_str} {'<- Picked' if picked_garch else ''}")
    print(f"  Rolling Volatility: {rolling_str} {'<- Picked' if picked_rolling else ''}")
    print(f"  Final Volatility (sigma): {final_sigma:.6f}\n")

    print(f"  Intensity (A): {results['market_data']['A']:.4f}\n  Order arrival decay (k): {results['market_data']['k']:.6f}\n")
    print(f"Optimal Parameters:\n  Risk Aversion (gamma): {results['optimal_parameters']['gamma']:.6f}")
    print(f"  Time Horizon (days): {results['optimal_parameters']['time_horizon_days']:.4f}\n")
    print(f"Calculated Prices (for q=1):\n  Reservation Price: ${results['calculated_values']['reservation_price']:.4f}")
    
    # Calculate spreads in basis points (bps)
    mid_price = results['market_data']['mid_price']
    ask_price = results['limit_orders']['ask_price']
    bid_price = results['limit_orders']['bid_price']
    ask_spread_bps = ((ask_price - mid_price) / mid_price) * 10000
    bid_spread_bps = ((mid_price - bid_price) / mid_price) * 10000
    
    print(f"  Ask Price: ${ask_price:.4f} ({ask_spread_bps:.2f} bps)")
    print(f"  Bid Price: ${bid_price:.4f} ({bid_spread_bps:.2f} bps)\n")
    
    if save_avellaneda_params_atomic(results, TICKER):
        print(f"Results saved to: params/avellaneda_parameters_{TICKER}.json")
    else:
        print("⚠️ Invalid params calculated, previous file was not updated.")
    print("="*80)

def get_continuous_recent_data(raw_df, processed_df, max_gap_seconds=60):
    """
    Extract the most recent continuous block of data.
    Works backwards from the most recent timestamp and stops at the first gap > max_gap_seconds.
    Also checks for gaps between the current time and the latest data point.

    Args:
        raw_df: Raw DataFrame with DatetimeIndex (before resampling)
        processed_df: Processed DataFrame (resampled to 1-second)
        max_gap_seconds: Maximum allowed gap in seconds (default 60 = 1 minute)

    Returns:
        Tuple of (filtered_raw_df, filtered_processed_df) containing only continuous recent data
    """
    if raw_df.empty:
        return raw_df, processed_df

    # Sort raw data in descending order (newest first) to work backwards from current time
    raw_sorted = raw_df.sort_index(ascending=False)
    
    # Check for gap between NOW and the latest data point
    latest_time = raw_sorted.index[0]
    now_utc = pd.Timestamp.utcnow().replace(tzinfo=None)
    time_since_update = (now_utc - latest_time).total_seconds()
    
    if time_since_update > max_gap_seconds:
        print(f"[WARNING] Data is stale! Last update was {time_since_update:.0f}s ago (Limit: {max_gap_seconds}s)")
        print(f"  Latest data timestamp: {latest_time} UTC")
        print(f"  Current system time:   {now_utc} UTC")
        print("  Continuing with available data, but be aware it may not be live.\n")

    # Calculate time differences between consecutive rows (in seconds)
    time_diffs = raw_sorted.index.to_series().diff(-1).abs().dt.total_seconds()

    # Find gaps larger than tolerance
    gaps = time_diffs[time_diffs > max_gap_seconds]

    if len(gaps) == 0:
        # No gaps found, all data is continuous
        print(f"[OK] Data is continuous with no internal gaps > {max_gap_seconds}s")
        return raw_df, processed_df

    # Get the most recent gap (first in our descending sorted data)
    first_gap_idx = gaps.index[0]

    # Keep only data after the gap (more recent than the gap)
    # logic: if gap is at T3 (diff between T3 and T2), then T3 is the start of the new block.
    # So we want >= first_gap_idx.
    cutoff_time = first_gap_idx
    continuous_raw = raw_df[raw_df.index >= cutoff_time]
    continuous_processed = processed_df[processed_df.index >= cutoff_time]

    # Calculate how much data was discarded
    discarded_pct = (len(raw_df) - len(continuous_raw)) / len(raw_df) * 100
    gap_size = gaps.iloc[0]

    print(f"[WARNING] Gap detected: {gap_size:.0f}s at {cutoff_time}")
    print(f"[OK] Using continuous data from {continuous_raw.index.min()} to {continuous_raw.index.max()}")
    print(f"  Discarded {discarded_pct:.1f}% of older data due to time gap\n")

    return continuous_raw, continuous_processed

def main():
    """Main execution hub for the parameter calculation."""
    global TICKER
    args = parse_arguments()
    TICKER = args.ticker
    window_minutes = args.minutes

    ma_window = 3 if window_minutes <= 8 * 60 else (2 if window_minutes < 20 * 60 else 1)

    # Load and process order book data to get VWAP mid-price
    try:
        raw_ob_df, processed_ob_df = load_and_process_orderbook_data(TICKER)
        trades_df = load_trades_data(os.path.join(Path(__file__).parent.absolute(), 'ASTER_data', f'trades_{TICKER}USDT.csv'))
    except (FileNotFoundError, ValueError) as e:
        print(f"Error loading data: {e}. Exiting.")
        sys.exit(1)

    # Filter to continuous recent data only (stop at gaps > 60 seconds)
    print("\n" + "="*80)
    print("Checking data continuity...")
    raw_ob_df, processed_ob_df = get_continuous_recent_data(raw_ob_df, processed_ob_df, max_gap_seconds=60)

    # Also filter trades to match the continuous data time range
    if not processed_ob_df.empty:
        min_time = processed_ob_df.index.min()
        max_time = processed_ob_df.index.max()
        original_trades = len(trades_df)
        trades_df = trades_df[(trades_df.index >= min_time) & (trades_df.index <= max_time)]
        print(f"[OK] Filtered trades to continuous time range: {len(trades_df)}/{original_trades} trades kept")
    print("="*80)

    # Prepare data periods using a more robust grouping method
    freq_str = f'{window_minutes}min'
    # Group by the desired frequency and count non-NA mid_prices
    grouper = pd.Grouper(freq=freq_str)
    group_sizes = processed_ob_df['mid_price'].groupby(grouper).count()
    
    # A full group should have a significant number of valid mid_price data points
    min_samples = int(window_minutes * 60 * 0.90) # Require 90% completeness
    
    # Filter for groups that are sufficiently complete
    complete_groups = group_sizes[group_sizes >= min_samples]
    list_of_periods = complete_groups.index.tolist()

    if not list_of_periods:
        print_summary({"current_state": {"minutes_window": window_minutes}}, [], df=raw_ob_df)
        sys.exit()

    # Calculate parameters
    calc_periods = list_of_periods[-min(len(list_of_periods), RECENT_PARAM_PERIODS + ma_window):]
    sigma_list, garch_sigma_list, rolling_sigma_list = calculate_volatility(processed_ob_df, window_minutes, freq_str, periods=calc_periods)
    
    tick_size = get_fallback_tick_size(TICKER)
    delta_list = np.arange(tick_size, 50.0 * tick_size, tick_size)
    Alist, klist = calculate_intensity_params(calc_periods, window_minutes, trades_df[trades_df['side'] == 'buy'], trades_df[trades_df['side'] == 'sell'], delta_list, processed_ob_df)

    if len(calc_periods) <= 1:
        print_summary({"current_state": {"minutes_window": window_minutes}}, calc_periods, df=raw_ob_df) # Pass context for logging
        sys.exit()

    gammalist, Tlist = optimize_params(calc_periods, sigma_list, Alist, klist, window_minutes, ma_window, processed_ob_df, trades_df[trades_df['side'] == 'buy'], trades_df[trades_df['side'] == 'sell'])
    
    # Aggregate final parameters using moving average
    gamma = pd.Series(gammalist[-ma_window:]).mean()
    T_h = pd.Series(Tlist[-ma_window:]).mean()
    A = pd.Series(Alist[-ma_window-1:-1]).mean()
    k = pd.Series(klist[-ma_window-1:-1]).mean()
    sigma = sigma_list[-1]
    garch_sigma = garch_sigma_list[-1]
    rolling_sigma = rolling_sigma_list[-1]

    # Generate final results and print summary
    if any(pd.isna([gamma, T_h, A, k, sigma])):
        print("Failed to calculate one or more parameters. Exiting.")
        sys.exit(1)
        
    results = calculate_final_quotes(gamma, sigma, A, k, T_h, processed_ob_df, ma_window, window_minutes, garch_sigma, rolling_sigma)
    print_summary(results, calc_periods, df=processed_ob_df)

if __name__ == "__main__":
    main()
