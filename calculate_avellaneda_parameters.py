import json
import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from backtester import optimize_params
from intensity import calculate_intensity_params
from utils import (
    get_fallback_tick_size,
    load_and_process_orderbook_data,
    load_trades_data,
    normalize_symbol_base,
    parse_arguments,
    resolve_trades_csv_path,
    save_avellaneda_params_atomic,
)
from volatility import calculate_volatility


logging.getLogger('numba').setLevel(logging.WARNING)

DEFAULT_MIN_SPREAD_BPS = 5.0
DEFAULT_MAX_SPREAD_BPS = 200.0


def normalize_spread_limits_bps(spread_limits) -> dict:
    """Return validated spread guardrails in basis points."""
    default_limits = {"min": DEFAULT_MIN_SPREAD_BPS, "max": DEFAULT_MAX_SPREAD_BPS}
    if not isinstance(spread_limits, dict):
        return default_limits

    try:
        min_bps = float(spread_limits.get("min", DEFAULT_MIN_SPREAD_BPS))
        max_bps = float(spread_limits.get("max", DEFAULT_MAX_SPREAD_BPS))
    except (TypeError, ValueError):
        return default_limits

    if not np.isfinite(min_bps) or not np.isfinite(max_bps) or min_bps < 0.0 or max_bps <= 0.0 or min_bps > max_bps:
        return default_limits

    return {"min": min_bps, "max": max_bps}


def clamp_quote_spread_bps(raw_bps: float, side_label: str, spread_limits_bps: dict) -> tuple[float, str | None]:
    """Clamp a computed quote spread to configured basis-point guardrails."""
    min_bps = spread_limits_bps["min"]
    max_bps = spread_limits_bps["max"]
    clamped_bps = min(max(raw_bps, min_bps), max_bps)

    if np.isclose(clamped_bps, raw_bps, rtol=0.0, atol=1e-12):
        return clamped_bps, None

    warning = (
        f"WARNING: {side_label} spread {raw_bps:.2f} bps was outside the configured "
        f"{min_bps:.2f}-{max_bps:.2f} bps range and was clamped to {clamped_bps:.2f} bps."
    )
    return clamped_bps, warning


def load_config():
    """Load configuration from config.json."""
    config_path = Path(__file__).parent / 'config.json'
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: config.json not found at {config_path}. Using default values.")
        return {
            "avellaneda_calculation": {
                "recent_param_periods": 4,
                "data_completeness_threshold": 0.90,
                "max_gap_seconds": 60,
                "spread_limits_bps": {
                    "min": DEFAULT_MIN_SPREAD_BPS,
                    "max": DEFAULT_MAX_SPREAD_BPS,
                },
                "ma_window_config": {
                    "short_period_threshold_minutes": 480,
                    "medium_period_threshold_minutes": 1200,
                    "short_period_ma_window": 3,
                    "medium_period_ma_window": 2,
                    "long_period_ma_window": 1,
                },
            }
        }


def get_ma_window(window_minutes, config):
    """Calculate moving average window based on analysis period length."""
    ma_config = config['avellaneda_calculation']['ma_window_config']

    if window_minutes <= ma_config['short_period_threshold_minutes']:
        return ma_config['short_period_ma_window']
    if window_minutes < ma_config['medium_period_threshold_minutes']:
        return ma_config['medium_period_ma_window']
    return ma_config['long_period_ma_window']


def calculate_final_quotes(
    ticker,
    gamma,
    sigma,
    A_buy,
    k_buy,
    A_sell,
    k_sell,
    time_horizon,
    mid_price_df,
    ma_window,
    window_minutes,
    garch_sigma,
    rolling_sigma,
    spread_limits_bps,
    avg_backtest_pnl=None,
    total_buys=0,
    total_sells=0,
    avg_buys_per_period=0,
    avg_sells_per_period=0,
):
    """Calculate the final reservation price and quotes for the current state using asymmetric parameters."""
    s = mid_price_df['mid_price'].iloc[-1]
    q = 0
    spread_limits_bps = normalize_spread_limits_bps(spread_limits_bps)

    risk_term = gamma * ((sigma * s) ** 2) * time_horizon
    log_term_buy = (1 / gamma) * np.log(1 + (gamma / k_buy))
    raw_delta_a = log_term_buy + (0.5 - q) * risk_term
    log_term_sell = (1 / gamma) * np.log(1 + (gamma / k_sell))
    raw_delta_b = log_term_sell + (0.5 + q) * risk_term
    raw_delta_a_bps = (raw_delta_a / s) * 10000.0
    raw_delta_b_bps = (raw_delta_b / s) * 10000.0
    delta_a_bps, ask_warning = clamp_quote_spread_bps(raw_delta_a_bps, "Ask", spread_limits_bps)
    delta_b_bps, bid_warning = clamp_quote_spread_bps(raw_delta_b_bps, "Bid", spread_limits_bps)
    delta_a = s * (delta_a_bps / 10000.0)
    delta_b = s * (delta_b_bps / 10000.0)
    r = s - q * risk_term
    r_a = s + delta_a
    r_b = s - delta_b
    warnings = [warning for warning in (ask_warning, bid_warning) if warning]

    result = {
        "ticker": ticker,
        "timestamp": pd.Timestamp.now().isoformat(),
        "spread_limits_bps": {
            "min": float(spread_limits_bps["min"]),
            "max": float(spread_limits_bps["max"]),
        },
        "market_data": {
            "mid_price": float(s),
            "sigma": float(sigma),
            "A_buy": float(A_buy),
            "k_buy": float(k_buy),
            "A_sell": float(A_sell),
            "k_sell": float(k_sell),
            "garch_sigma": float(garch_sigma),
            "rolling_sigma": float(rolling_sigma),
        },
        "optimal_parameters": {"gamma": float(gamma), "time_horizon_days": float(time_horizon)},
        "current_state": {"inventory": int(q), "minutes_window": window_minutes, "ma_window": ma_window},
        "calculated_values": {"reservation_price": float(r), "gap": float(abs(r - s))},
        "limit_orders": {
            "ask_price": float(r_a),
            "bid_price": float(r_b),
            "delta_a": float(delta_a),
            "delta_b": float(delta_b),
            "delta_a_bps": float(delta_a_bps),
            "delta_b_bps": float(delta_b_bps),
            "delta_a_percent": (delta_a / s) * 100.0,
            "delta_b_percent": (delta_b / s) * 100.0,
            "raw_ask_price": float(s + raw_delta_a),
            "raw_bid_price": float(s - raw_delta_b),
            "raw_delta_a": float(raw_delta_a),
            "raw_delta_b": float(raw_delta_b),
            "raw_delta_a_bps": float(raw_delta_a_bps),
            "raw_delta_b_bps": float(raw_delta_b_bps),
        },
    }

    if warnings:
        result["warnings"] = warnings

    if avg_backtest_pnl is not None:
        result["backtest_performance"] = {
            "avg_pnl": float(avg_backtest_pnl),
            "total_buys": int(total_buys),
            "total_sells": int(total_sells),
            "avg_buys_per_period": float(avg_buys_per_period),
            "avg_sells_per_period": float(avg_sells_per_period),
            "total_trades": int(total_buys + total_sells),
        }

    return result


def build_summary_text(results: dict, periods: list, df=None, minutes_window=None) -> str:
    """Build a formatted summary of the calculated parameters."""
    if not results or 'market_data' not in results:
        resolved_window = minutes_window
        if resolved_window is None and results:
            resolved_window = results.get('current_state', {}).get('minutes_window', 5)
        if resolved_window is None:
            resolved_window = 5

        lines = ["", "=" * 80, "Insufficient data for robust parameter estimation.", ""]
        if df is not None and not df.empty:
            lines.append(f"  - Data points available: {len(df)}")
            lines.append(f"  - Time range: {df.index.min()} to {df.index.max()}")
            lines.append(
                f"  - Analysis periods formed: {len(periods)} "
                f"(requires at least 2 periods of {resolved_window} minutes each for backtesting)"
            )
        lines.append("=" * 80)
        return "\n".join(lines)

    lines = [f"\n{'=' * 80}", f"AVELLANEDA-STOIKOV PARAMETERS - {results['ticker']}", "=" * 80]

    resolved_window = results.get('current_state', {}).get('minutes_window', 5)
    num_periods = len(periods)
    lines.append("Analysis Window:")
    lines.append(f"  Period length: {resolved_window} minutes")
    lines.append(f"  Number of periods analyzed: {num_periods}")
    if df is not None and not df.empty:
        lines.append(f"  Data range: {df.index.min()} to {df.index.max()}")
    lines.append("")

    lines.append("Market Data:")
    lines.append(f"  Mid Price: ${results['market_data']['mid_price']:,.4f}")

    garch_sigma = results['market_data']['garch_sigma']
    rolling_sigma = results['market_data']['rolling_sigma']
    final_sigma = results['market_data']['sigma']

    garch_str = f"{garch_sigma:.6f}" if pd.notna(garch_sigma) else "N/A"
    rolling_str = f"{rolling_sigma:.6f}" if pd.notna(rolling_sigma) else "N/A"
    picked_garch = pd.notna(garch_sigma) and final_sigma == garch_sigma
    picked_rolling = pd.notna(rolling_sigma) and final_sigma == rolling_sigma

    lines.append(f"  GARCH Volatility:   {garch_str} {'<- Picked' if picked_garch else ''}")
    lines.append(f"  Rolling Volatility: {rolling_str} {'<- Picked' if picked_rolling else ''}")
    lines.append(f"  Final Volatility (sigma): {final_sigma:.6f}")
    lines.append("")

    lines.append(
        f"  Intensity (Buy/Ask Side):  A={results['market_data']['A_buy']:.4f}, "
        f"k={results['market_data']['k_buy']:.6f}"
    )
    lines.append(
        f"  Intensity (Sell/Bid Side): A={results['market_data']['A_sell']:.4f}, "
        f"k={results['market_data']['k_sell']:.6f}"
    )
    lines.append("")

    lines.append("Optimal Parameters:")
    lines.append(f"  Risk Aversion (gamma): {results['optimal_parameters']['gamma']:.6f}")
    lines.append(f"  Time Horizon (days): {results['optimal_parameters']['time_horizon_days']:.4f}")

    if 'backtest_performance' in results:
        bt_perf = results['backtest_performance']
        lines.append("")
        lines.append(f"Backtest Performance (last {results['current_state']['ma_window']} periods):")
        lines.append(f"  Avg PnL: ${bt_perf['avg_pnl']:.4f}")
        lines.append(f"  Total Trades: {bt_perf['total_trades']} ({bt_perf['total_buys']} buys, {bt_perf['total_sells']} sells)")
        lines.append(
            f"  Avg Trades/Period: {bt_perf['avg_buys_per_period']:.1f} buys, "
            f"{bt_perf['avg_sells_per_period']:.1f} sells"
        )

    lines.append("")
    lines.append("Calculated Prices (for q=0):")
    lines.append(f"  Reservation Price: ${results['calculated_values']['reservation_price']:.4f}")
    spread_limits_bps = normalize_spread_limits_bps(results.get("spread_limits_bps"))
    lines.append(
        f"  Configured Spread Guardrails: {spread_limits_bps['min']:.2f} to {spread_limits_bps['max']:.2f} bps"
    )

    mid_price = results['market_data']['mid_price']
    ask_price = results['limit_orders']['ask_price']
    bid_price = results['limit_orders']['bid_price']
    ask_spread_bps = ((ask_price - mid_price) / mid_price) * 10000
    bid_spread_bps = ((mid_price - bid_price) / mid_price) * 10000
    lines.append(f"  Ask Price: ${ask_price:.4f} ({ask_spread_bps:.2f} bps)")
    lines.append(f"  Bid Price: ${bid_price:.4f} ({bid_spread_bps:.2f} bps)")
    warnings = results.get("warnings", [])
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        for warning in warnings:
            lines.append(f"  {warning}")
    lines.append("")
    lines.append("=" * 80)
    return "\n".join(lines)


def save_results(results: dict, ticker: str) -> bool:
    """Persist the computed params when they pass validation."""
    return save_avellaneda_params_atomic(results, ticker)


def get_continuous_recent_data(raw_df, processed_df, max_gap_seconds):
    """
    Extract the most recent continuous block of data.
    Works backwards from the most recent timestamp and stops at the first gap > max_gap_seconds.
    Also checks for gaps between the current time and the latest data point.
    """
    if raw_df.empty:
        return raw_df, processed_df

    raw_sorted = raw_df.sort_index(ascending=False)

    latest_time = raw_sorted.index[0]
    now_utc = pd.Timestamp.utcnow().replace(tzinfo=None)
    time_since_update = (now_utc - latest_time).total_seconds()

    if time_since_update > max_gap_seconds:
        print(f"[WARNING] Data is stale! Last update was {time_since_update:.0f}s ago (Limit: {max_gap_seconds}s)")
        print(f"  Latest data timestamp: {latest_time} UTC")
        print(f"  Current system time:   {now_utc} UTC")
        print("  Continuing with available data, but be aware it may not be live.\n")

    time_diffs = raw_sorted.index.to_series().diff(-1).abs().dt.total_seconds()
    gaps = time_diffs[time_diffs > max_gap_seconds]

    if len(gaps) == 0:
        print(f"[OK] Data is continuous with no internal gaps > {max_gap_seconds}s")
        return raw_df, processed_df

    first_gap_idx = gaps.index[0]
    cutoff_time = first_gap_idx
    continuous_raw = raw_df[raw_df.index >= cutoff_time]
    continuous_processed = processed_df[processed_df.index >= cutoff_time]

    discarded_pct = (len(raw_df) - len(continuous_raw)) / len(raw_df) * 100
    gap_size = gaps.iloc[0]

    print(f"[WARNING] Gap detected: {gap_size:.0f}s at {cutoff_time}")
    print(f"[OK] Using continuous data from {continuous_raw.index.min()} to {continuous_raw.index.max()}")
    print(f"  Discarded {discarded_pct:.1f}% of older data due to time gap\n")

    return continuous_raw, continuous_processed


def main():
    """Main execution hub for the parameter calculation."""
    config = load_config()
    config_params = config['avellaneda_calculation']

    args = parse_arguments()
    ticker = normalize_symbol_base(args.ticker)
    window_minutes = args.minutes
    ma_window = get_ma_window(window_minutes, config)
    recent_param_periods = config_params['recent_param_periods']
    max_gap_seconds = config_params['max_gap_seconds']
    lookback_periods = recent_param_periods + ma_window + 4
    lookback_seconds = (lookback_periods * window_minutes * 60) + max_gap_seconds

    try:
        raw_ob_df, processed_ob_df = load_and_process_orderbook_data(ticker, lookback_seconds=lookback_seconds)
        trades_csv_path, resolved_trade_symbol = resolve_trades_csv_path(ticker)
        trades_df = load_trades_data(str(trades_csv_path))
    except (FileNotFoundError, ValueError) as e:
        print(f"Error loading data: {e}. Exiting.")
        sys.exit(1)

    if resolved_trade_symbol:
        print(f"Using local trades data for {resolved_trade_symbol}.")

    print("\n" + "=" * 80)
    print("Checking data continuity...")
    raw_ob_df, processed_ob_df = get_continuous_recent_data(raw_ob_df, processed_ob_df, max_gap_seconds)

    if not processed_ob_df.empty:
        min_time = processed_ob_df.index.min()
        max_time = processed_ob_df.index.max()
        original_trades = len(trades_df)
        trades_df = trades_df[(trades_df.index >= min_time) & (trades_df.index <= max_time)]
        print(f"[OK] Filtered trades to continuous time range: {len(trades_df)}/{original_trades} trades kept")
    print("=" * 80)

    freq_str = f'{window_minutes}min'
    grouper = pd.Grouper(freq=freq_str)
    group_sizes = processed_ob_df['mid_price'].groupby(grouper).count()

    completeness_threshold = config_params['data_completeness_threshold']
    min_samples = int(window_minutes * 60 * completeness_threshold)
    complete_groups = group_sizes[group_sizes >= min_samples]
    list_of_periods = complete_groups.index.tolist()

    if not list_of_periods:
        print(build_summary_text(None, [], df=raw_ob_df, minutes_window=window_minutes))
        sys.exit()

    calc_periods = list_of_periods[-min(len(list_of_periods), recent_param_periods + ma_window):]
    sigma_list, garch_sigma_list, rolling_sigma_list = calculate_volatility(
        processed_ob_df,
        window_minutes,
        freq_str,
        periods=calc_periods,
    )

    tick_size = get_fallback_tick_size(ticker)
    delta_list = np.arange(tick_size / 2.0, 50.0 * tick_size, tick_size / 2.0)
    buy_trades = trades_df[trades_df['side'] == 'buy']
    sell_trades = trades_df[trades_df['side'] == 'sell']
    A_buys, k_buys, A_sells, k_sells = calculate_intensity_params(
        calc_periods,
        window_minutes,
        buy_trades,
        sell_trades,
        delta_list,
        processed_ob_df,
    )

    A_buys = pd.Series(A_buys).ffill().tolist()
    k_buys = pd.Series(k_buys).ffill().tolist()
    A_sells = pd.Series(A_sells).ffill().tolist()
    k_sells = pd.Series(k_sells).ffill().tolist()

    if len(calc_periods) <= 1:
        print(build_summary_text(None, calc_periods, df=raw_ob_df, minutes_window=window_minutes))
        sys.exit()

    def avg_lists(l1, l2):
        return [(a + b) / 2 if pd.notna(a) and pd.notna(b) else (a if pd.notna(a) else b) for a, b in zip(l1, l2)]

    Alist = avg_lists(A_buys, A_sells)
    klist = avg_lists(k_buys, k_sells)

    fixed_gamma = config_params.get('fixed_gamma', None)
    gammalist, Tlist, pnl_list, buy_count_list, sell_count_list = optimize_params(
        calc_periods,
        sigma_list,
        Alist,
        klist,
        window_minutes,
        ma_window,
        processed_ob_df,
        buy_trades,
        sell_trades,
        fixed_gamma=fixed_gamma,
    )

    gamma = pd.Series(gammalist[-ma_window:]).mean()
    T_h = pd.Series(Tlist[-ma_window:]).mean()
    A_buy = pd.Series(A_buys[-ma_window - 1:-1]).mean()
    k_buy = pd.Series(k_buys[-ma_window - 1:-1]).mean()
    A_sell = pd.Series(A_sells[-ma_window - 1:-1]).mean()
    k_sell = pd.Series(k_sells[-ma_window - 1:-1]).mean()

    if pd.isna(A_buy):
        A_buy = A_sell
    if pd.isna(k_buy):
        k_buy = k_sell
    if pd.isna(A_sell):
        A_sell = A_buy
    if pd.isna(k_sell):
        k_sell = k_buy

    sigma = sigma_list[-1]
    garch_sigma = garch_sigma_list[-1]
    rolling_sigma = rolling_sigma_list[-1]
    avg_backtest_pnl = pd.Series(pnl_list[-ma_window:]).mean() if pnl_list else None
    total_buys = sum(buy_count_list[-ma_window:]) if buy_count_list else 0
    total_sells = sum(sell_count_list[-ma_window:]) if sell_count_list else 0
    avg_buys_per_period = total_buys / ma_window if ma_window > 0 else 0
    avg_sells_per_period = total_sells / ma_window if ma_window > 0 else 0

    if any(pd.isna([gamma, T_h, A_buy, k_buy, A_sell, k_sell, sigma])):
        print("Failed to calculate one or more parameters. Exiting.")
        sys.exit(1)

    results = calculate_final_quotes(
        ticker,
        gamma,
        sigma,
        A_buy,
        k_buy,
        A_sell,
        k_sell,
        T_h,
        processed_ob_df,
        ma_window,
        window_minutes,
        garch_sigma,
        rolling_sigma,
        config_params.get("spread_limits_bps"),
        avg_backtest_pnl,
        total_buys,
        total_sells,
        avg_buys_per_period,
        avg_sells_per_period,
    )
    print(build_summary_text(results, calc_periods, df=processed_ob_df))

    if save_results(results, ticker):
        print(f"Results saved to: params/avellaneda_parameters_{ticker}.json")
    else:
        print("Invalid params calculated, previous file was not updated.")


if __name__ == "__main__":
    main()
