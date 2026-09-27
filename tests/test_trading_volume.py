"""Offline check of get_my_trading_volume's chunked pagination and aggregation."""
import asyncio

import get_my_trading_volume as gtv

DAY = gtv.DAY_MS


class FakeExchange:
    """Mimics /fapi/v3/userTrades: time filters, or fromId paging (which ignores time)."""

    def __init__(self, trades, limit):
        self.trades, self.limit, self.calls = trades, limit, []

    async def signed_request(self, method, endpoint, params):
        self.calls.append(dict(params))
        if 'fromId' in params:
            page = [t for t in self.trades if t['id'] >= params['fromId']]
        else:
            assert params['endTime'] - params['startTime'] <= 7 * DAY  # server-side 7-day limit
            page = [t for t in self.trades if params['startTime'] <= t['time'] <= params['endTime']]
        return page[:params['limit']]


def test_fetch_trades_pages_across_the_7_day_chunk_boundary(monkeypatch):
    monkeypatch.setattr(gtv, "PAGE_LIMIT", 3)
    start, end = 0, 10 * DAY
    # 25 trades, every 0.4 days: straddles the 7-day chunk edge and the window end.
    trades = [{'id': i, 'time': int(i * 0.4 * DAY), 'qty': '2', 'price': '10',
               'side': 'BUY' if i % 2 else 'SELL'} for i in range(1, 26)]
    exchange = FakeExchange(trades, limit=3)

    fetched = asyncio.run(gtv.fetch_trades(exchange, 'ETHUSDT', start, end))

    expected = [t for t in trades if start <= t['time'] < end]
    assert [t['id'] for t in fetched] == [t['id'] for t in expected]  # each once, in order
    assert any('fromId' in call for call in exchange.calls)           # paging exercised
    assert sum('startTime' in call for call in exchange.calls) == 2   # 7-day + 3-day chunk

    symbol_volumes, daily_volumes = gtv.aggregate({'ETHUSDT': fetched})
    eth = symbol_volumes['ETHUSDT']
    assert eth['trade_count'] == len(expected)
    assert eth['quote_volume'] == 20.0 * len(expected)
    assert eth['buy_count'] + eth['sell_count'] == len(expected)
    assert sum(v['trade_count'] for v in daily_volumes.values()) == len(expected)
