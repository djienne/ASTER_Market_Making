import asyncio
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
        "0x" + ("11" * 32),
    )


def test_prepare_request_does_not_mutate_input_params():
    client = make_client()
    params = {"symbol": "BTCUSDT"}

    request_params, headers = client._prepare_request(params)

    assert params == {"symbol": "BTCUSDT"}
    assert request_params["symbol"] == "BTCUSDT"
    assert "nonce" in request_params
    assert request_params["user"] == client.api_user
    assert request_params["signer"] == client.api_signer
    assert "signature" in request_params
    assert headers["Content-Type"] == "application/x-www-form-urlencoded"


def test_api_client_has_default_http_timeout():
    client = make_client()

    assert client.timeout.total == 20
    assert client.timeout.connect == 10


def test_get_symbol_filters_exposes_min_qty():
    client = make_client()

    async def fake_exchange_info():
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.005", "minQty": "0.015"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5.0"},
                    ],
                }
            ]
        }

    client.get_exchange_info = fake_exchange_info
    filters = asyncio.run(client.get_symbol_filters("BTCUSDT"))

    assert filters["step_size"] == 0.005
    assert filters["min_qty"] == 0.015
    assert filters["status"] == "TRADING"


def test_resolve_symbol_prefers_cli_then_env(monkeypatch):
    monkeypatch.setenv("SYMBOL", "ETHUSDT")

    assert market_maker.resolve_symbol(None) == "ETHUSDT"
    assert market_maker.resolve_symbol("BTCUSDT") == "BTCUSDT"


def test_normalize_symbol_base_handles_multiple_stable_quotes():
    assert utils.normalize_symbol_base("BTCUSDT") == "BTC"
    assert utils.normalize_symbol_base("ETHUSDC") == "ETH"
    assert utils.normalize_symbol_base("SOLUSD1") == "SOL"


def test_quote_symbol_candidates_and_local_resolution_support_base_or_full_symbols(tmp_path):
    orderbook_root = tmp_path / "orderbook_parquet"
    orderbook_root.mkdir()
    (orderbook_root / "BTCUSDC").mkdir()

    trades_root = tmp_path / "ASTER_data"
    trades_root.mkdir()
    trades_csv = trades_root / "trades_BTCUSDC.csv"
    trades_csv.write_text("id,unix_timestamp_ms,side,price,quantity\n1,1,buy,100,0.1\n", encoding="utf-8")

    assert utils.quote_symbol_candidates("BTC") == ["BTCUSDT", "BTCUSDC", "BTCUSDF", "BTCUSD1", "BTCUSD"]
    assert utils.quote_symbol_candidates("BTCUSDC") == ["BTCUSDC"]
    assert utils.resolve_orderbook_symbol("BTC", data_root=orderbook_root) == "BTCUSDC"

    resolved_path, resolved_symbol = utils.resolve_trades_csv_path("BTC", data_root=trades_root)
    assert resolved_symbol == "BTCUSDC"
    assert resolved_path == trades_csv


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


def test_market_maker_disables_quoting_when_dynamic_params_are_missing(monkeypatch, tmp_path):
    params_dir = tmp_path / "params"
    params_dir.mkdir()

    monkeypatch.setattr(market_maker, "PARAMS_DIR", str(params_dir))
    monkeypatch.setattr(market_maker, "USE_AVELLANEDA_SPREADS", True)
    market_maker._SPREAD_CACHE.clear()

    params = market_maker.get_avellaneda_params("BTCUSDT")

    assert params["source"] == "unavailable"


def test_calculate_vwap_uses_true_execution_price():
    orders = [[100.0, 1.0], [200.0, 1.0]]

    full_vwap = utils.calculate_vwap(orders, 300.0)
    first_level_vwap = utils.calculate_vwap(orders, 100.0)

    assert abs(full_vwap - 150.0) < 1e-9
    assert abs(first_level_vwap - 100.0) < 1e-9


def test_select_recent_orderbook_parquet_files_prefers_recent_history():
    files = [
        r"C:\tmp\1000.parquet",
        r"C:\tmp\2000.parquet",
        r"C:\tmp\3000.parquet",
        r"C:\tmp\_latest.parquet",
    ]

    selected = utils.select_recent_orderbook_parquet_files(files, lookback_seconds=1.2)

    assert r"C:\tmp\1000.parquet" not in selected
    assert r"C:\tmp\2000.parquet" in selected
    assert r"C:\tmp\3000.parquet" in selected
    assert selected[-1] == r"C:\tmp\_latest.parquet"


def test_save_avellaneda_params_rejects_payloads_missing_runtime_fields(monkeypatch, tmp_path):
    monkeypatch.setattr(utils, "PARAMS_DIR", str(tmp_path))

    invalid_payload = {
        "market_data": {
            "sigma": 0.02,
            "k_buy": 2.0,
            "k_sell": 3.0,
        },
        "optimal_parameters": {
            "time_horizon_days": 0.5,
        },
        "limit_orders": {
            "delta_a": 1.0,
            "delta_b": 1.0,
        },
    }

    assert utils.save_avellaneda_params_atomic(invalid_payload, "BTC") is False
    assert not any(Path(tmp_path).glob("*.json"))
