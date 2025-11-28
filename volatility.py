import numpy as np
import pandas as pd
from arch import arch_model

def calculate_garch_volatility(mid_price_df: pd.DataFrame, window_minutes: int, periods: list) -> list:
    """Calculate volatility using GARCH(1,1) for specified periods."""
    print("Calculating GARCH(1,1) Volatility...")
    sigma_garch_list = []

    for i, period_start in enumerate(periods):
        period_end = period_start + pd.Timedelta(minutes=window_minutes)
        mask = mid_price_df.index <= period_end
        historical_data = mid_price_df.loc[mask]

        if len(historical_data) < 100:
            sigma_garch_list.append(np.nan)
            continue

        returns = historical_data['mid_price'].pct_change().dropna() * 1000.0
        if len(returns) < 100:
            sigma_garch_list.append(np.nan)
            continue

        try:
            am = arch_model(returns, mean='Constant', vol='GARCH', p=1, q=1, dist='t')
            res = am.fit(disp='off', show_warning=False)
            forecasts = res.forecast(horizon=1)
            variance_next = forecasts.variance.iloc[-1, 0]
            volatility_decimal = (variance_next**0.5) / 1000.0
            sigma_daily = volatility_decimal * np.sqrt(86400)  # Scale per-second volatility to daily
            sigma_garch_list.append(sigma_daily)
        except Exception:
            sigma_garch_list.append(np.nan)
            
    return sigma_garch_list

def calculate_rolling_volatility(mid_price_df: pd.DataFrame, window_minutes: int, freq_str: str, periods: list) -> list:
    """Calculate rolling volatility as a fallback."""
    print("Calculating rolling volatility as fallback...")
    log_returns = np.log(mid_price_df['mid_price']).diff().dropna()
    period_std = log_returns.groupby(pd.Grouper(freq=freq_str)).std()

    if len(period_std) == 0:
        return [np.nan] * len(periods)

    window_periods = min(6, len(period_std))
    smoothed_std = period_std.rolling(window=window_periods, min_periods=1).mean()
    sigma_series = (smoothed_std * np.sqrt(86400)).reindex(periods)  # Scale per-second volatility to daily
    
    return sigma_series.tolist()

def calculate_volatility(mid_price_df: pd.DataFrame, window_minutes: int, freq_str: str, periods: list = None) -> list:
    """Calculate combined GARCH and rolling volatility with explicit logging."""
    print("\n" + "-"*20 + "\nCalculating volatility (sigma)...")
    all_periods = mid_price_df.index.floor(freq_str).unique().tolist()[:-1]
    target_periods = periods or all_periods

    if not target_periods:
        return [], [], []

    if len(all_periods) < 10:
        print(f"Found {len(all_periods)} periods (fewer than 10); using rolling volatility only.")
        rolling_sigma = calculate_rolling_volatility(mid_price_df, window_minutes, freq_str, target_periods)
        return rolling_sigma, [np.nan] * len(rolling_sigma), rolling_sigma

    garch_sigma = calculate_garch_volatility(mid_price_df, window_minutes, target_periods)
    rolling_sigma = calculate_rolling_volatility(mid_price_df, window_minutes, freq_str, target_periods)

    final_sigma = []
    print("\nCombining GARCH and rolling volatility...")
    for i, (g, r) in enumerate(zip(garch_sigma, rolling_sigma)):
        if pd.notna(g):
            if pd.notna(r) and r > 0:
                ratio = g / r
                if ratio > 5.0 or ratio < 0.2:
                    print(f"  - Period {i}: GARCH value {g:.6f} is an outlier (ratio to rolling: {ratio:.2f}). Using rolling value {r:.6f}.")
                    final_sigma.append(r)
                else:
                    print(f"  - Period {i}: GARCH value {g:.6f} is stable. Using GARCH.")
                    final_sigma.append(g)
            else:
                print(f"  - Period {i}: Rolling volatility is invalid. Using GARCH value {g:.6f}.")
                final_sigma.append(g)
        elif pd.notna(r):
            print(f"  - Period {i}: GARCH calculation failed. Using rolling value {r:.6f}.")
            final_sigma.append(r)
        else:
            print(f"  - Period {i}: Both GARCH and rolling volatility calculations failed.")
            final_sigma.append(np.nan)
    
    final_sigma = pd.Series(final_sigma).ffill().tolist()
    return final_sigma, garch_sigma, rolling_sigma
