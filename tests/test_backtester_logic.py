import numpy as np

from backtester import jit_backtest_loop, evaluate_gamma


def _single_ask_fill_inputs():
    """Two-step scenario: ask fills at step 0, nothing at step 1.

    With classical inventory convention (q>0 = long), selling 1 unit at r_a
    should leave q = -1 and pnl ≈ r_a*(1-fee) - s ≈ half_spread*(1-fee).
    """
    s = 100.0
    half = 0.5
    fee = 0.0004

    s_values = np.array([s, s], dtype=np.float64)
    buy_min_values = np.array([np.nan, np.nan], dtype=np.float64)
    sell_max_values = np.array([s + 10.0, np.nan], dtype=np.float64)

    gamma, k, sigma = 1.0, 1.0, 0.0001
    time_remaining = np.array([1e-6, 0.0], dtype=np.float64)

    half_spread = np.array([half, half], dtype=np.float64)
    spread_base = half_spread * 2.0

    return dict(
        s_values=s_values,
        buy_min_values=buy_min_values,
        sell_max_values=sell_max_values,
        gamma=gamma, k=k, sigma=sigma, fee=fee,
        time_remaining=time_remaining,
        spread_base=spread_base,
        half_spread=half_spread,
        expected_r_a=s + half,
    )


def test_single_ask_fill_produces_short_inventory_and_small_positive_pnl():
    args = _single_ask_fill_inputs()
    expected_r_a = args.pop("expected_r_a")

    pnl, buys, sells = jit_backtest_loop(**args)

    assert sells == 1
    assert buys == 0
    # With the correct inventory convention q[1] = -1 after an ask fill,
    # pnl[end] = cash - s ≈ r_a*(1-fee) - s ≈ half_spread (minus fee rebate).
    # The buggy convention q[1] = +1 would give pnl ≈ r_a*(1-fee) + s (~2s),
    # which is off by ~2 * s.
    final_pnl = pnl[-1]
    expected_pnl = expected_r_a * (1.0 - args["fee"]) - args["s_values"][0]
    assert abs(final_pnl - expected_pnl) < 0.01, (
        f"final pnl {final_pnl} should be ~{expected_pnl}; a value near "
        f"{expected_r_a * (1.0 - args['fee']) + args['s_values'][0]} indicates "
        "the inventory sign is inverted (q incremented on sell instead of decremented)."
    )


def test_evaluate_gamma_rejects_nonpositive_gamma():
    # Round 1 fix: gamma <= 0 or k <= 0 must not poison the optimizer.
    dummy = np.array([100.0])
    # evaluate_gamma needs DataFrames; easier to test the guard via run_backtest.
    from backtester import run_backtest
    import pandas as pd

    idx = pd.date_range("2024-01-01", periods=2, freq="s")
    mid = pd.Series([100.0, 100.0], index=idx)
    empty_trades = pd.DataFrame({"price": []}, index=pd.DatetimeIndex([]))

    assert run_backtest(mid, empty_trades, empty_trades, gamma=0.0, k=1.0, sigma=0.01,
                        window_minutes=1, time_horizon=1.0 / 1440) is None
    assert run_backtest(mid, empty_trades, empty_trades, gamma=1.0, k=0.0, sigma=0.01,
                        window_minutes=1, time_horizon=1.0 / 1440) is None
