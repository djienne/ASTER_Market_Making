import asyncio

from api_client import ApiClient
import market_maker


def make_client():
    return ApiClient(
        "0x0000000000000000000000000000000000000001",
        "0x0000000000000000000000000000000000000002",
        "0x" + ("11" * 32),
    )


def test_signing_does_not_mutate_input_params():
    client = make_client()
    params = {"symbol": "BTCUSDT"}

    request_params = asyncio.run(client._sign_async(params))
    headers = client._build_headers()

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
