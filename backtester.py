import numpy as np
import pandas as pd
import warnings
from numba import jit
from scipy.optimize import brentq, fsolve

GAMMA_CALCULATION_WINDOW = 4

@jit(nopython=True)
def jit_backtest_loop(s_values, buy_min_values, sell_max_values, gamma, k, sigma, fee, time_remaining, spread_base, half_spread):
    """Core JIT-compiled backtest loop for performance."""
    N = len(s_values)
    q, x, pnl = np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1)
    gamma_sigma2 = gamma * sigma**2
    
    for i in range(N):
        r = s_values[i] - q[i] * gamma_sigma2 * time_remaining[i]
        gap = abs(r - s_values[i])
        delta_a, delta_b = (half_spread[i] + gap, half_spread[i] - gap) if r >= s_values[i] else (half_spread[i] - gap, half_spread[i] + gap)
        
        r_a, r_b = r + delta_a, r - delta_b
        
        sell = not np.isnan(sell_max_values[i]) and sell_max_values[i] >= r_a
        buy = not np.isnan(buy_min_values[i]) and buy_min_values[i] <= r_b
        
        q[i+1] = q[i] + (sell - buy)
        x[i+1] = x[i] + (r_a * (1 - fee) if sell else 0) - (r_b * (1 + fee) if buy else 0)
        pnl[i+1] = x[i+1] + q[i+1] * s_values[i]
        
    return pnl

def run_backtest(mid_prices, buy_trades, sell_trades, gamma, k, sigma, window_minutes, time_horizon, fee=0.00040):
    """Simulate the market making strategy over a historical period."""
    time_index = mid_prices.index
    buy_min = buy_trades['price'].resample('5s').min().reindex(time_index, method='ffill')
    sell_max = sell_trades['price'].resample('5s').max().reindex(time_index, method='ffill')
    s_values = mid_prices.resample('5s').first().reindex(time_index, method='ffill').values

    N = len(s_values)
    T_backtest = window_minutes / 1440.0
    dt = T_backtest / N if N > 0 else 0
    
    time_remaining = np.maximum(0, time_horizon - np.arange(N) * dt)
    spread_base = gamma * sigma**2 * time_remaining + (2 / gamma) * np.log(1 + (gamma / k))
    
    pnl = jit_backtest_loop(s_values, buy_min.values, sell_max.values, gamma, k, sigma, fee, time_remaining, spread_base, spread_base / 2.0)
    return {'pnl': pnl}

def evaluate_gamma(gamma, mid_prices, buy_trades, sell_trades, k, sigma, window_minutes, time_horizon):
    """Evaluate a single gamma value by running a backtest and returning the PnL."""
    res = run_backtest(mid_prices, buy_trades, sell_trades, gamma, k, sigma, window_minutes, time_horizon)
    final_pnl = res['pnl'][-1]
    return [gamma, final_pnl] if np.isfinite(final_pnl) and final_pnl != 0 else [gamma, np.nan]

def find_gamma(target_spread, spread_func, k):
    """Find a gamma value for a given target spread using numerical methods."""
    def equation(gamma):
        return spread_func(gamma) - target_spread if gamma > 0 else float('inf')

    try:
        return brentq(equation, 1e-8, 1000.0)
    except ValueError:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = fsolve(equation, k)
            if abs(equation(result[0])) < 1e-6:
                return result[0]
    raise ValueError("Could not find a suitable gamma.")

def generate_gamma_grid(s, sigma, k, time_horizon):
    """Generate a grid of gamma values to test for optimization."""
    time_remaining = time_horizon / 2.0
    spread_func = lambda g: (g * sigma**2 * time_remaining + (2 / g) * np.log(1 + (g / k))) / s * 100.0
    try:
        gamma_low = find_gamma(2.0, spread_func, k)
        gamma_high = find_gamma(0.01, spread_func, k)
        return np.logspace(np.log10(gamma_low), np.log10(gamma_high), 64)
    except ValueError:
        return None

def optimize_params(list_of_periods, sigma_list, Alist, klist, window_minutes, ma_window, mid_price_df, buy_trades, sell_trades):
    """Optimize gamma and time horizon via backtesting."""
    print("\n" + "-"*20 + "\nOptimizing gamma and time horizon...")
    gammalist, Tlist = [], []
    T_grid = [1.0/24.0, 4.0/24.0, 12.0/24.0, 1.0]
    
    start_index = max(1, len(list_of_periods) - GAMMA_CALCULATION_WINDOW)
    for j in range(start_index, len(list_of_periods)):
        A = pd.Series(Alist[max(0, j - ma_window):j]).mean()
        k = pd.Series(klist[max(0, j - ma_window):j]).mean()
        sigma = sigma_list[j-1]

        if not all(pd.notna([A, k, sigma])):
            gammalist.append(np.nan)
            Tlist.append(np.nan)
            continue

        period_start = list_of_periods[j]
        period_end = period_start + pd.Timedelta(minutes=window_minutes)
        s_period = mid_price_df.loc[period_start:period_end, 'mid_price'].resample('s').ffill()
        
        if s_period.empty:
            gammalist.append(np.nan)
            Tlist.append(np.nan)
            continue
            
        buy_period = buy_trades[(buy_trades.index >= period_start) & (buy_trades.index < period_end)]
        sell_period = sell_trades[(sell_trades.index >= period_start) & (sell_trades.index < period_end)]

        best_pnl, best_gamma, best_T = -np.inf, np.nan, np.nan
        for t_horizon in T_grid:
            gamma_grid = generate_gamma_grid(s_period.iloc[-1], sigma, k, t_horizon)
            if gamma_grid is None: continue
            
            results = [evaluate_gamma(g, s_period, buy_period, sell_period, k, sigma, window_minutes, t_horizon) for g in gamma_grid]
            results_df = pd.DataFrame(results, columns=['gamma', 'pnl']).dropna()

            if not results_df.empty:
                max_pnl_row = results_df.loc[results_df['pnl'].idxmax()]
                if max_pnl_row['pnl'] > best_pnl:
                    best_pnl, best_gamma, best_T = max_pnl_row['pnl'], max_pnl_row['gamma'], t_horizon
        
        gammalist.append(best_gamma if np.isfinite(best_gamma) else 0.5)
        Tlist.append(best_T if np.isfinite(best_T) else 1.0/24.0)

    return gammalist, Tlist
