"""Regression tests for bugs fixed in the Round 1/Round 2 review."""
import importlib
import os
import runpy
import sys

import numpy as np
import pandas as pd
import pytest

import api_client
from api_client import ApiClient
from intensity import calculate_intensity_params


def _make_client():
    return ApiClient(
        "0x0000000000000000000000000000000000000001",
        "0x0000000000000000000000000000000000000002",
        "0x" + ("11" * 32),
    )


def test_nonce_stays_monotonic_when_wall_clock_jumps_backward(monkeypatch):
    """NTP steps / DST adjustments must not produce equal or decreasing nonces."""
    client = _make_client()

    # Seed with a realistic microsecond value, then force the clock 1 second back.
    times = iter([1_700_000_000_000_000, 1_699_999_999_000_000])
    monkeypatch.setattr(api_client.time, "time_ns", lambda: next(times) * 1000)

    n1 = client._next_nonce()
    n2 = client._next_nonce()
    assert n2 > n1, "nonce must strictly increase even when time_ns goes backward"


def test_nonce_recovers_from_stale_seed():
    """Historical _last_nonce must not pin future nonces to a stale second."""
    client = _make_client()
    client._last_nonce = 1  # pretend we shipped with a tiny seed
    n1 = client._next_nonce()
    n2 = client._next_nonce()
    assert n1 > 1_000_000_000_000  # should jump to current wall-clock microseconds
    assert n2 > n1


def test_intensity_flat_log_curve_returns_nan():
    """A flat log-intensity curve (all trades at the same spread) has ss_tot == 0;
    the fit must reject rather than synthesize a fake R^2."""
    timestamps = pd.date_range("2024-01-01", periods=60, freq="1s")
    mid = pd.DataFrame({"mid_price": [100.0] * 60}, index=timestamps)

    # All market buys lifted the ask by exactly 0.1 — constant spread, no slope.
    buy_trades = pd.DataFrame(
        {"price": [100.1] * 60, "side": ["buy"] * 60},
        index=timestamps,
    )
    sell_trades = pd.DataFrame(columns=["price", "side"])
    sell_trades.index = pd.DatetimeIndex([])

    delta_list = np.array([0.02, 0.05, 0.08, 0.1, 0.12])
    periods = [timestamps[0]]

    A_b, k_b, A_s, k_s = calculate_intensity_params(
        periods, window_minutes=1, buy_orders=buy_trades, sell_orders=sell_trades,
        deltalist=delta_list, mid_price_df=mid,
    )
    assert np.isnan(A_b[0]) and np.isnan(k_b[0])
    assert np.isnan(A_s[0]) and np.isnan(k_s[0])


def test_websocket_orders_module_imports_cleanly():
    """Round 1 fixed a NameError at websocket_orders.py:180. Both the production
    module and the tests/ copy must now reach the early-return env-check branch
    without raising."""
    import asyncio

    saved = {k: os.environ.pop(k, None) for k in ("API_USER", "API_SIGNER", "API_PRIVATE_KEY")}
    try:
        # Production module
        import websocket_orders as prod_module
        asyncio.run(prod_module.extended_demo())
        # Test-directory duplicate (regression caught in Round 2)
        import tests.websocket_orders as demo_module
        asyncio.run(demo_module.extended_demo())
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v
