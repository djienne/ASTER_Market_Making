import numpy as np
import pandas as pd
import sys
import os
from pathlib import Path
import logging

# Import modularized functions
from utils import (
    parse_arguments, get_fallback_tick_size, load_and_resample_mid_price,
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

def print_summary(results: dict, periods: list):
    """Print a formatted summary of the calculated parameters."""
    if not results:
        print("\n" + "="*80 + "\nInsufficient data for robust parameter estimation.\n" + "="*80)
        return
        
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

def main():
    """Main execution hub for the parameter calculation."""
    global TICKER
    args = parse_arguments()
    TICKER = args.ticker
    window_minutes = args.minutes
    
    ma_window = 3 if window_minutes <= 8 * 60 else (2 if window_minutes < 20 * 60 else 1)

    # Load data
    data_dir = os.path.join(Path(__file__).parent.absolute(), 'ASTER_data')
    mid_price_df = load_and_resample_mid_price(os.path.join(data_dir, f'prices_{TICKER}USDT.csv'))
    trades_df = load_trades_data(os.path.join(data_dir, f'trades_{TICKER}USDT.csv'))

    # Prepare data periods
    freq_str = f'{window_minutes}min'
    list_of_periods = mid_price_df.index.floor(freq_str).unique().tolist()[:-1]

    if not list_of_periods:
        print_summary({}, [])
        sys.exit()

    # Calculate parameters
    calc_periods = list_of_periods[-min(len(list_of_periods), RECENT_PARAM_PERIODS + ma_window):]
    sigma_list, garch_sigma_list, rolling_sigma_list = calculate_volatility(mid_price_df, window_minutes, freq_str, periods=calc_periods)
    
    tick_size = get_fallback_tick_size(TICKER)
    delta_list = np.arange(tick_size, 50.0 * tick_size, tick_size)
    Alist, klist = calculate_intensity_params(calc_periods, window_minutes, trades_df[trades_df['side'] == 'buy'], trades_df[trades_df['side'] == 'sell'], delta_list, mid_price_df)

    if len(calc_periods) <= 1:
        print_summary({}, calc_periods)
        sys.exit()

    gammalist, Tlist = optimize_params(calc_periods, sigma_list, Alist, klist, window_minutes, ma_window, mid_price_df, trades_df[trades_df['side'] == 'buy'], trades_df[trades_df['side'] == 'sell'])
    
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
        
    results = calculate_final_quotes(gamma, sigma, A, k, T_h, mid_price_df, ma_window, window_minutes, garch_sigma, rolling_sigma)
    print_summary(results, calc_periods)

if __name__ == "__main__":
    main()
