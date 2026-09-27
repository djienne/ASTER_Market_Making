import pytest

import find_trend


class _StopFetch(Exception):
    pass


def test_catch_up_refetches_the_last_cached_candle(monkeypatch, tmp_path):
    # The last cached candle was usually still open when cached; the catch-up
    # must request it again so its final OHLCV replaces the partial one.
    monkeypatch.chdir(tmp_path)
    (tmp_path / "params").mkdir()
    (tmp_path / "params" / "klines_TESTUSDT_5m.csv").write_text(
        "Open Time,Open,High,Low,Close,Volume\n"
        "1000,1,1,1,1,1\n"
        "301000,1,1,1,1,1\n"
    )
    requests = []

    def fake_fetch(endpoint, params):
        requests.append(dict(params))
        raise _StopFetch

    monkeypatch.setattr(find_trend, "_fetch_with_backoff", fake_fetch)
    with pytest.raises(_StopFetch):
        find_trend.perform_grid_search("TESTUSDT", "5m")

    assert requests[0]["startTime"] == 301000
