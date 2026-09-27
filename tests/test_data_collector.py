import json

import data_collector
import pandas as pd


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


def test_flush_orderbook_buffer_archives_completed_hour_and_keeps_current_hour_latest(monkeypatch, tmp_path):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    monkeypatch.chdir(tmp_path)
    collector = data_collector.WebSocketDataCollector(["BTCUSDT"])

    completed_hour_ms = 1713261600000  # 2024-04-16T10:00:00Z
    current_hour_ms = 1713265200000    # 2024-04-16T11:00:00Z
    collector.orderbook_buffer["BTCUSDT"].extend(
        [
            {"timestamp": completed_hour_ms + 1_000, "lastUpdateId": 1, "bids": [["1", "1"]], "asks": [["2", "1"]]},
            {"timestamp": completed_hour_ms + 2_000, "lastUpdateId": 2, "bids": [["1", "2"]], "asks": [["2", "2"]]},
            {"timestamp": current_hour_ms + 1_000, "lastUpdateId": 3, "bids": [["1", "3"]], "asks": [["2", "3"]]},
        ]
    )

    collector.flush_orderbook_buffer_to_parquet("BTCUSDT")

    archive_path = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT" / "20240416T100000Z.parquet"
    latest_path = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT" / "_latest.parquet"

    assert archive_path.is_file()
    assert latest_path.is_file()

    archived_df = pd.read_parquet(archive_path, engine="pyarrow")
    latest_df = pd.read_parquet(latest_path, engine="pyarrow")

    assert archived_df["lastUpdateId"].tolist() == [1, 2]
    assert latest_df["lastUpdateId"].tolist() == [3]
    assert [record["lastUpdateId"] for record in collector.orderbook_buffer["BTCUSDT"]] == [3]


def test_flush_orderbook_buffer_recovers_existing_latest_after_restart(monkeypatch, tmp_path):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    monkeypatch.chdir(tmp_path)

    output_dir = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT"
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_latest_path = output_dir / "_latest.parquet"
    pd.DataFrame(
        [
            {"timestamp": 1713261601000, "lastUpdateId": 1, "bids": [["1", "1"]], "asks": [["2", "1"]]},
            {"timestamp": 1713261602000, "lastUpdateId": 2, "bids": [["1", "2"]], "asks": [["2", "2"]]},
        ]
    ).to_parquet(existing_latest_path, engine="pyarrow", compression="ZSTD")

    collector = data_collector.WebSocketDataCollector(["BTCUSDT"])
    collector.orderbook_buffer["BTCUSDT"].append(
        {"timestamp": 1713265201000, "lastUpdateId": 3, "bids": [["1", "3"]], "asks": [["2", "3"]]}
    )

    collector.flush_orderbook_buffer_to_parquet("BTCUSDT")

    archive_path = output_dir / "20240416T100000Z.parquet"
    latest_path = output_dir / "_latest.parquet"

    assert archive_path.is_file()
    assert latest_path.is_file()
    assert pd.read_parquet(archive_path, engine="pyarrow")["lastUpdateId"].tolist() == [1, 2]
    assert pd.read_parquet(latest_path, engine="pyarrow")["lastUpdateId"].tolist() == [3]


def test_force_flush_orderbook_buffer_archives_current_hour_and_clears_latest(monkeypatch, tmp_path):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    monkeypatch.chdir(tmp_path)
    collector = data_collector.WebSocketDataCollector(["BTCUSDT"])

    hour_ms = 1713265200000  # 2024-04-16T11:00:00Z
    collector.orderbook_buffer["BTCUSDT"].extend(
        [
            {"timestamp": hour_ms + 1_000, "lastUpdateId": 1, "bids": [["1", "1"]], "asks": [["2", "1"]]},
            {"timestamp": hour_ms + 2_000, "lastUpdateId": 2, "bids": [["1", "2"]], "asks": [["2", "2"]]},
        ]
    )

    collector.flush_orderbook_buffer_to_parquet("BTCUSDT", force_archive=True)

    archive_path = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT" / "20240416T110000Z.parquet"
    latest_path = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT" / "_latest.parquet"

    assert archive_path.is_file()
    assert not latest_path.exists()
    assert len(collector.orderbook_buffer["BTCUSDT"]) == 0


def test_flush_orderbook_buffer_drops_invalid_timestamps(monkeypatch, tmp_path):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    monkeypatch.chdir(tmp_path)
    collector = data_collector.WebSocketDataCollector(["BTCUSDT"])
    collector.orderbook_buffer["BTCUSDT"].extend(
        [
            {"timestamp": "bad", "lastUpdateId": 1, "bids": [["1", "1"]], "asks": [["2", "1"]]},
            {"timestamp": 1713265202000, "lastUpdateId": 2, "bids": [["1", "2"]], "asks": [["2", "2"]]},
        ]
    )

    collector.flush_orderbook_buffer_to_parquet("BTCUSDT")

    latest_path = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT" / "_latest.parquet"
    latest_df = pd.read_parquet(latest_path, engine="pyarrow")

    assert latest_df["lastUpdateId"].tolist() == [2]
    assert [record["lastUpdateId"] for record in collector.orderbook_buffer["BTCUSDT"]] == [2]


def test_flush_orderbook_buffer_rolls_current_hour_when_buffer_limit_hit(monkeypatch, tmp_path):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    monkeypatch.chdir(tmp_path)
    collector = data_collector.WebSocketDataCollector(["BTCUSDT"])
    collector.ORDERBOOK_BUFFER_SIZE_LIMIT = 2

    hour_ms = 1713265200000  # 2024-04-16T11:00:00Z
    collector.orderbook_buffer["BTCUSDT"].extend(
        [
            {"timestamp": hour_ms + 1_000, "lastUpdateId": 1, "bids": [["1", "1"]], "asks": [["2", "1"]]},
            {"timestamp": hour_ms + 2_000, "lastUpdateId": 2, "bids": [["1", "2"]], "asks": [["2", "2"]]},
        ]
    )

    collector.flush_orderbook_buffer_to_parquet("BTCUSDT")

    archive_path = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT" / "20240416T110000Z.parquet"
    latest_path = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT" / "_latest.parquet"

    assert archive_path.is_file()
    assert not latest_path.exists()
    assert len(collector.orderbook_buffer["BTCUSDT"]) == 0


def test_unreadable_latest_is_preserved_and_writes_are_atomic(monkeypatch, tmp_path):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    monkeypatch.chdir(tmp_path)
    output_dir = tmp_path / "ASTER_data" / "orderbook_parquet" / "BTCUSDT"
    output_dir.mkdir(parents=True, exist_ok=True)
    truncated = b"PAR1-truncated-by-a-crash-mid-write"
    (output_dir / "_latest.parquet").write_bytes(truncated)

    collector = data_collector.WebSocketDataCollector(["BTCUSDT"])
    collector.orderbook_buffer["BTCUSDT"].append(
        {"timestamp": 1713265201000, "lastUpdateId": 3, "bids": [["1", "3"]], "asks": [["2", "3"]]}
    )
    collector.flush_orderbook_buffer_to_parquet("BTCUSDT")

    corrupt = list(output_dir.glob("_latest.parquet.corrupt-*"))
    assert len(corrupt) == 1 and corrupt[0].read_bytes() == truncated  # kept for recovery
    assert pd.read_parquet(output_dir / "_latest.parquet")["lastUpdateId"].tolist() == [3]
    assert not list(output_dir.glob("*.tmp"))


def test_trade_flush_keeps_full_precision_and_bounds_seen_ids(monkeypatch, tmp_path):
    monkeypatch.setattr(data_collector.WebSocketDataCollector, "load_seen_trade_ids", lambda self, symbol: set())
    monkeypatch.chdir(tmp_path)
    (tmp_path / "ASTER_data").mkdir()
    collector = data_collector.WebSocketDataCollector(["DOGEUSDT"])
    collector.SEEN_TRADE_IDS_PRUNE_AT = 10
    collector.SEEN_TRADE_IDS_KEEP = 5
    collector.seen_trade_ids["DOGEUSDT"] = set(range(1, 21))
    collector.trades_buffer["DOGEUSDT"].append(
        {"id": 20, "timestamp": 1000, "side": "buy", "price": 0.0000123456, "quantity": 12345.6789012}
    )

    collector.flush_trades_buffer("DOGEUSDT")

    assert collector.seen_trade_ids["DOGEUSDT"] == {16, 17, 18, 19, 20}
    row = pd.read_csv(tmp_path / "ASTER_data" / "trades_DOGEUSDT.csv").iloc[0]
    assert row["price"] == 0.0000123456  # 6-decimal formatting wrote 0.000012
    assert row["quantity"] == 12345.6789012
