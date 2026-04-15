import json

import data_collector


def test_create_combined_stream_url_uses_raw_trade_stream(monkeypatch):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    collector = data_collector.WebSocketDataCollector(["BTCUSDT"])

    url = collector.create_combined_stream_url()

    assert "@trade" in url
    assert "@aggTrade" not in url


def test_on_trades_message_dedupes_raw_trade_ids(monkeypatch):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    collector = data_collector.WebSocketDataCollector(["BTCUSDT"])

    message = json.dumps(
        {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 123,
            "p": "100.0",
            "q": "0.1",
            "T": 1000,
            "m": False,
        }
    )

    collector.on_trades_message(None, message)
    collector.on_trades_message(None, message)

    assert len(collector.trades_buffer["BTCUSDT"]) == 1
    record = collector.trades_buffer["BTCUSDT"][0]
    assert record["id"] == 123
    assert record["side"] == "buy"
