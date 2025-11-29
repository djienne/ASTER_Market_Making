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
    buy_count = 0
    sell_count = 0

    for i in range(N):
        r = s_values[i] - q[i] * gamma_sigma2 * time_remaining[i]
        gap = abs(r - s_values[i])
        delta_a, delta_b = (half_spread[i] + gap, half_spread[i] - gap) if r >= s_values[i] else (half_spread[i] - gap, half_spread[i] + gap)

        r_a, r_b = r + delta_a, r - delta_b

        sell = not np.isnan(sell_max_values[i]) and sell_max_values[i] >= r_a
        buy = not np.isnan(buy_min_values[i]) and buy_min_values[i] <= r_b

        if sell:
            sell_count += 1
        if buy:
            buy_count += 1

        q[i+1] = q[i] + (sell - buy)
        x[i+1] = x[i] + (r_a * (1 - fee) if sell else 0) - (r_b * (1 + fee) if buy else 0)
        pnl[i+1] = x[i+1] + q[i+1] * s_values[i]

    return pnl, buy_count, sell_count

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

    pnl, buy_count, sell_count = jit_backtest_loop(s_values, buy_min.values, sell_max.values, gamma, k, sigma, fee, time_remaining, spread_base, spread_base / 2.0)
    return {'pnl': pnl, 'buys': buy_count, 'sells': sell_count}

def evaluate_gamma(gamma, mid_prices, buy_trades, sell_trades, k, sigma, window_minutes, time_horizon):
    """Evaluate a single gamma value by running a backtest and returning the PnL and trade counts."""
    res = run_backtest(mid_prices, buy_trades, sell_trades, gamma, k, sigma, window_minutes, time_horizon)
    final_pnl = res['pnl'][-1]
    if np.isfinite(final_pnl):
        return [gamma, final_pnl, res['buys'], res['sells']]
    else:
        return [gamma, np.nan, 0, 0]

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

    def safe_find_gamma(target):
        try:
            return find_gamma(target, spread_func, k)
        except ValueError:
            # Check boundaries if target is unreachable
            s_min = spread_func(1e-8)
            s_max = spread_func(1000.0)
            if s_min > target: return 1e-8
            if s_max < target: return 1000.0
            return 1e-8 # Fallback

    gamma_wide = safe_find_gamma(2.0)
    gamma_tight = safe_find_gamma(0.01)
    
    return np.logspace(np.log10(gamma_wide), np.log10(gamma_tight), 64)

def optimize_params(list_of_periods, sigma_list, Alist, klist, window_minutes, ma_window, mid_price_df, buy_trades, sell_trades, fixed_gamma=None):
    """Optimize gamma and time horizon via backtesting."""
    print("\n" + "-"*20 + "\nOptimizing gamma and time horizon...")
    gammalist, Tlist, pnl_list, buy_count_list, sell_count_list = [], [], [], [], []
    # Expanded grid: 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 24h
    T_grid = [5.0/1440.0, 15.0/1440.0, 30.0/1440.0, 1.0/24.0, 2.0/24.0, 4.0/24.0, 8.0/24.0, 12.0/24.0, 1.0]

    start_index = max(1, len(list_of_periods) - GAMMA_CALCULATION_WINDOW)
    for j in range(start_index, len(list_of_periods)):
        A = pd.Series(Alist[max(0, j - ma_window):j]).mean()
        k = pd.Series(klist[max(0, j - ma_window):j]).mean()
        sigma = sigma_list[j-1]

        if not all(pd.notna([A, k, sigma])):
            gammalist.append(np.nan)
            Tlist.append(np.nan)
            pnl_list.append(np.nan)
            buy_count_list.append(0)
            sell_count_list.append(0)
            continue

        period_start = list_of_periods[j]
        period_end = period_start + pd.Timedelta(minutes=window_minutes)
        s_period = mid_price_df.loc[period_start:period_end, 'mid_price'].resample('s').ffill()

        if s_period.empty:
            gammalist.append(np.nan)
            Tlist.append(np.nan)
            pnl_list.append(np.nan)
            buy_count_list.append(0)
            sell_count_list.append(0)
            continue

        buy_period = buy_trades[(buy_trades.index >= period_start) & (buy_trades.index < period_end)]
        sell_period = sell_trades[(sell_trades.index >= period_start) & (sell_trades.index < period_end)]

        best_pnl, best_gamma, best_T, best_buys, best_sells = -np.inf, np.nan, np.nan, 0, 0
        for t_horizon in T_grid:
            if fixed_gamma is not None:
                gamma_grid = [fixed_gamma]
            else:
                gamma_grid = generate_gamma_grid(s_period.iloc[-1], sigma, k, t_horizon)
                if gamma_grid is None: continue

            results = [evaluate_gamma(g, s_period, buy_period, sell_period, k, sigma, window_minutes, t_horizon) for g in gamma_grid]
            results_df = pd.DataFrame(results, columns=['gamma', 'pnl', 'buys', 'sells']).dropna()

            if not results_df.empty:
                max_pnl_row = results_df.loc[results_df['pnl'].idxmax()]
                if max_pnl_row['pnl'] > best_pnl:
                    best_pnl = max_pnl_row['pnl']
                    best_gamma = max_pnl_row['gamma']
                    best_T = t_horizon
                    best_buys = int(max_pnl_row['buys'])
                    best_sells = int(max_pnl_row['sells'])

        gammalist.append(best_gamma if np.isfinite(best_gamma) else np.nan)
        Tlist.append(best_T if np.isfinite(best_T) else np.nan)
        pnl_list.append(best_pnl if np.isfinite(best_pnl) else np.nan)
        buy_count_list.append(best_buys)
        sell_count_list.append(best_sells)

    return gammalist, Tlist, pnl_list, buy_count_list, sell_count_list
