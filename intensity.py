import numpy as np
import pandas as pd
import scipy.optimize

def calculate_intensity_params(periods: list, window_minutes: int, buy_orders: pd.DataFrame, sell_orders: pd.DataFrame, deltalist: np.ndarray, mid_price_df: pd.DataFrame) -> tuple[list, list, list, list]:
    """
    Calculate order arrival intensity parameters (A and k) separating Bid and Ask sides,
    using weighted regression (recency bias) and R-squared filtering.
    
    Returns:
        A_buys, k_buys: Parameters derived from Market Buys (impacting Ask side liquidity)
        A_sells, k_sells: Parameters derived from Market Sells (impacting Bid side liquidity)
    """
    print("\n" + "-"*20 + "\nCalculating order arrival intensity (A and k) per side with recency weights...")

    A_buys, k_buys = [], []
    A_sells, k_sells = [], []
    
    if not periods:
        return A_buys, k_buys, A_sells, k_sells

    # Ensure index is sorted for merge_asof
    mid_price_df = mid_price_df.sort_index()
    buy_orders = buy_orders.sort_index()
    sell_orders = sell_orders.sort_index()

    # Recency weight half-life (e.g., half the window length)
    # alpha = ln(2) / half_life
    half_life_seconds = (window_minutes * 60) / 2.0
    decay_alpha = np.log(2) / half_life_seconds

    def fit_params_for_trades(trades_subset, side_name):
        if trades_subset.empty:
            return np.nan, np.nan

        # Match trades with latest mid-price
        matched_trades = pd.merge_asof(trades_subset, mid_price_df[['mid_price']], left_index=True, right_index=True, direction='backward')
        
        # Calculate spread
        if side_name == 'buy': # Market Buys hit Ask -> spread = Price - Mid
            matched_trades['spread'] = (matched_trades['price'] - matched_trades['mid_price'])
        else: # Market Sells hit Bid -> spread = Mid - Price
            matched_trades['spread'] = (matched_trades['mid_price'] - matched_trades['price'])
        
        # Filter negative spreads (crossed market or noise) if desired, or take abs
        # Taking abs is safer for simple intensity, though negative spread implies instant fill
        matched_trades['spread'] = matched_trades['spread'].abs()

        # Calculate time-decay weights
        # weight = exp(-alpha * (T_end - t_trade))
        # timestamps in seconds
        # Force nanosecond-resolution epoch conversion across pandas versions.
        trade_times = matched_trades.index.to_numpy(dtype='datetime64[ns]').astype(np.int64) // 10**9
        period_end_ts = period_end.timestamp()
        
        time_diffs = period_end_ts - trade_times
        weights = np.exp(-decay_alpha * time_diffs)
        
        # Effective Time Window (integral of weights)
        # Integral_0^T exp(-alpha * t) dt = (1 - exp(-alpha * T)) / alpha
        # T = window_seconds
        window_seconds = window_minutes * 60
        effective_T = (1 - np.exp(-decay_alpha * window_seconds)) / decay_alpha

        lambdas = []
        valid_deltas = []

        for delta in deltalist:
            # Weighted count of trades with spread >= delta
            mask = matched_trades['spread'] >= delta
            weighted_count = np.sum(weights[mask])
            
            if weighted_count > 0:
                l_val = weighted_count / effective_T
                lambdas.append(l_val)
                valid_deltas.append(delta)

        if len(valid_deltas) < 3: # Need at least 3 points for a decent R^2 check
            return np.nan, np.nan

        try:
            # Fit ln(lambda) = ln(A) - k * delta
            y_data = np.log(lambdas)
            x_data = np.array(valid_deltas)
            
            slope, intercept = np.polyfit(x_data, y_data, 1)
            
            # Calculate R-squared
            y_pred = slope * x_data + intercept
            ss_res = np.sum((y_data - y_pred) ** 2)
            ss_tot = np.sum((y_data - np.mean(y_data)) ** 2)
            
            if ss_tot == 0:
                # Degenerate fit: log-intensity is constant, no slope is meaningful.
                return np.nan, np.nan
            r_squared = 1 - (ss_res / ss_tot)

            # R-squared threshold
            if r_squared < 0.6: # Filter poor fits
                return np.nan, np.nan

            k_param = -slope
            A_param = np.exp(intercept)
            
            if k_param <= 0 or not np.isfinite(A_param):
                return np.nan, np.nan
            
            return A_param, k_param

        except Exception:
            return np.nan, np.nan

    for period_start in periods:
        period_end = period_start + pd.Timedelta(minutes=window_minutes)
        
        period_buys = buy_orders[(buy_orders.index >= period_start) & (buy_orders.index < period_end)].copy()
        period_sells = sell_orders[(sell_orders.index >= period_start) & (sell_orders.index < period_end)].copy()
        
        # Fit Market Buys (Ask side liquidity)
        A_b, k_b = fit_params_for_trades(period_buys, 'buy')
        A_buys.append(A_b)
        k_buys.append(k_b)
        
        # Fit Market Sells (Bid side liquidity)
        A_s, k_s = fit_params_for_trades(period_sells, 'sell')
        A_sells.append(A_s)
        k_sells.append(k_s)
            
    return A_buys, k_buys, A_sells, k_sells

