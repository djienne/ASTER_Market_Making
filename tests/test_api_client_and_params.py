import json
from pathlib import Path

import pandas as pd

import calculate_avellaneda_parameters as cap
from api_client import ApiClient
import market_maker
import utils


def make_client():
    return ApiClient(
        "0x0000000000000000000000000000000000000001",
        "0x0000000000000000000000000000000000000002",
        "0xdeadbeef",
    )


def test_prepare_request_does_not_mutate_input_params():
    client = make_client()
    params = {"symbol": "BTCUSDT"}

    request_params, headers = client._prepare_request(
        params,
        use_binance_auth=True,
        api_key="public",
        api_secret="secret",
    )

    assert params == {"symbol": "BTCUSDT"}
    assert request_params["symbol"] == "BTCUSDT"
    assert "timestamp" in request_params
    assert headers["X-MBX-APIKEY"] == "public"


def test_api_client_has_default_http_timeout():
    client = make_client()

    assert client.timeout.total == 20
    assert client.timeout.connect == 10


def test_resolve_symbol_prefers_cli_then_env(monkeypatch):
    monkeypatch.setenv("SYMBOL", "ETHUSDT")

    assert market_maker.resolve_symbol(None) == "ETHUSDT"
    assert market_maker.resolve_symbol("BTCUSDT") == "BTCUSDT"


def test_build_summary_text_handles_missing_results_without_crashing():
    df = pd.DataFrame({"value": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3, freq="min"))

    summary = cap.build_summary_text(None, [], df=df, minutes_window=5)

    assert "Insufficient data" in summary
    assert "Data points available: 3" in summary


def test_calculate_final_quotes_uses_explicit_ticker():
    df = pd.DataFrame({"mid_price": [100.0]})

    result = cap.calculate_final_quotes(
        "BTC",
        0.1,
        0.02,
        1.0,
        2.0,
        1.0,
        2.0,
        0.5,
        df,
        3,
        5,
        0.02,
        0.02,
    )

    assert result["ticker"] == "BTC"


def test_build_summary_text_has_no_file_write_side_effect(monkeypatch, tmp_path):
    output_path = tmp_path / "params"
    monkeypatch.setattr(cap, "save_results", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not save")))

    summary = cap.build_summary_text({"current_state": {"minutes_window": 5}}, [], df=None, minutes_window=5)

    assert "Insufficient data" in summary
    assert not any(Path(output_path).glob("*"))


def test_market_maker_loads_generated_avellaneda_params(monkeypatch, tmp_path):
    params_dir = tmp_path / "params"
    params_dir.mkdir()

    payload = {
        "ticker": "BTC",
        "market_data": {
            "sigma": 0.02,
            "A_buy": 1.5,
            "k_buy": 2.0,
            "A_sell": 1.7,
            "k_sell": 3.0,
        },
        "optimal_parameters": {
            "gamma": 0.1,
            "time_horizon_days": 0.5,
        },
    }
    (params_dir / "avellaneda_parameters_BTC.json").write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(market_maker, "PARAMS_DIR", str(params_dir))
    market_maker._SPREAD_CACHE.clear()

    params = market_maker.get_avellaneda_params("BTCUSDT")

    assert params["source"] == "avellaneda_parameters_BTC.json"
    assert params["k_buy"] == 2.0
    assert params["k_sell"] == 3.0


def test_calculate_vwap_uses_true_execution_price():
    orders = [[100.0, 1.0], [200.0, 1.0]]

    full_vwap = utils.calculate_vwap(orders, 300.0)
    first_level_vwap = utils.calculate_vwap(orders, 100.0)

    assert abs(full_vwap - 150.0) < 1e-9
    assert abs(first_level_vwap - 100.0) < 1e-9
