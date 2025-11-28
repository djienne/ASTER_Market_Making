import numpy as np
import pandas as pd
import scipy.optimize

def calculate_intensity_params(periods: list, window_minutes: int, buy_orders: pd.DataFrame, sell_orders: pd.DataFrame, deltalist: np.ndarray, mid_price_df: pd.DataFrame) -> tuple[list, list]:
    """Calculate order arrival intensity parameters (A and k)."""
    print("\n" + "-"*20 + "\nCalculating order arrival intensity (A and k)...")

    def exp_fit(x, a, b):
        return a * np.exp(-b * x)

    Alist, klist = [], []
    if not periods:
        return Alist, klist

    for period_start in periods:
        period_end = period_start + pd.Timedelta(minutes=window_minutes)
        
        period_buy = buy_orders[(buy_orders.index >= period_start) & (buy_orders.index < period_end)]
        period_sell = sell_orders[(sell_orders.index >= period_start) & (sell_orders.index < period_end)]

        if period_buy.empty and period_sell.empty:
            Alist.append(np.nan)
            klist.append(np.nan)
            continue

        # Prioritize using the mean mid-price from the resampled data for stability
        ref_mid = mid_price_df.loc[period_start:period_end, 'mid_price'].mean()
        if pd.isna(ref_mid):
            # Fallback to trade data if mid-price is unavailable
            ref_mid = (period_buy['price'].max() + period_sell['price'].min()) / 2
        
        if pd.isna(ref_mid):
            Alist.append(np.nan)
            klist.append(np.nan)
            continue

        deltadict = {}
        for delta in deltalist:
            bid_hits = period_sell[period_sell['price'] <= ref_mid - delta].index
            ask_hits = period_buy[period_buy['price'] >= ref_mid + delta].index
            all_hits = sorted(bid_hits.tolist() + ask_hits.tolist())

            if len(all_hits) > 1:
                deltas = pd.to_datetime(all_hits).to_series().diff().dt.total_seconds().dropna()
                deltadict[delta] = deltas
            else:
                deltadict[delta] = pd.Series([window_minutes * 60])
        
        lambdas = pd.DataFrame({
            "lambda_delta": [1 / d.mean() if not d.empty else 0 for d in deltadict.values()]
        }, index=list(deltadict.keys()))

        try:
            params, _ = scipy.optimize.curve_fit(exp_fit, lambdas.index, lambdas["lambda_delta"], maxfev=5000)
            Alist.append(params[0])
            klist.append(params[1])
        except (RuntimeError, ValueError):
            Alist.append(np.nan)
            klist.append(np.nan)
            
    return Alist, klist
