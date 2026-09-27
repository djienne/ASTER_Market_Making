"""End-to-end offline run of market_maker.main(): fake Aster REST/WS + fake Binance feed.

Guards the startup/shutdown wiring the unit tests never execute (main()
builds the calculator, starts every task, quotes, and cleans up).
"""
import asyncio
import json
import logging
import signal
import sys
import types

import logging_config
import market_maker

MID = 2689.495


class FakeClient:
    """Just the ApiClient surface main() uses."""
    last = None

    def __init__(self, *args):
        FakeClient.last = self
        self.placed, self.open_orders, self.cancel_all_calls = [], {}, 0
        self.session = types.SimpleNamespace(closed=False)
        self.shutdown_scheduled = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.session.closed = True

    async def cancel_all_orders(self, symbol):
        self.cancel_all_calls += 1
        self.open_orders.clear()
        return {}

    async def signed_request(self, method, endpoint, params=None):
        assert (method, endpoint) == ("GET", "/fapi/v3/account")
        return {"assets": [{"asset": "USDT", "walletBalance": "1000"}]}

    async def get_symbol_filters(self, symbol):
        return {"status": "TRADING", "tick_size": 0.01, "price_precision": 2, "step_size": 0.001,
                "quantity_precision": 3, "min_qty": 0.001, "min_notional": 5.0}

    async def create_listen_key(self):
        return {"listenKey": "fake-key"}

    async def keepalive_listen_key(self):
        return {}

    async def get_position_risk(self, symbol):
        return [{"positionAmt": "0", "notional": "0"}]

    async def change_leverage(self, symbol, leverage):
        return {}

    async def place_order(self, symbol, price, quantity, side, reduce_only=False):
        order_id = len(self.placed) + 1
        self.placed.append((side, float(price), float(quantity)))
        self.open_orders[order_id] = side
        if {s for s, _, _ in self.placed} == {"BUY", "SELL"} and not self.shutdown_scheduled:
            self.shutdown_scheduled = True  # both sides quoted: Ctrl+C the bot
            asyncio.get_running_loop().call_later(0.2, signal.raise_signal, signal.SIGINT)
        return {"orderId": order_id}

    async def cancel_order(self, symbol, order_id):
        self.open_orders.pop(order_id, None)
        return {}

    async def get_order_status(self, symbol, order_id):
        return {"status": "CANCELED", "executedQty": "0"}

    async def get_open_orders(self, symbol):
        return [{"orderId": order_id} for order_id in self.open_orders]


class FakeSocket:
    def __init__(self, messages):
        self.messages = messages

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def recv(self):
        await asyncio.sleep(0.01)
        return json.dumps(next(self.messages))


def aster_depth():
    while True:
        yield {"e": "depthUpdate", "b": [["2689.49", "1"]], "a": [["2689.50", "1"]]}


def binance_depth():
    last = 1000
    for i in range(10**6):  # bridge event first (U <= 1000 <= u), then a pu-linked chain
        first = last if i == 0 else last + 1
        yield {"U": first, "u": last + 1, "pu": last, "b": [["2689.50", "3" if i % 2 else "0"]], "a": []}
        last += 1


class Idle:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def recv(self):
        await asyncio.Event().wait()


def fake_connect(url, **kwargs):
    if "binance" in url:
        return FakeSocket(binance_depth())
    if url.endswith("@depth5"):
        return FakeSocket(aster_depth())
    return Idle()  # user data stream: quiet


async def fake_snapshot(symbol):
    return {"lastUpdateId": 1000, "bids": [["2689.40", "5"]], "asks": [["2689.60", "5"]]}


def test_main_starts_quotes_both_sides_and_cleans_up(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["market_maker.py", "--symbol", "ETHUSDT"])
    monkeypatch.setattr(market_maker, "ApiClient", FakeClient)
    monkeypatch.setattr(market_maker.websockets, "connect", fake_connect)
    monkeypatch.setattr(market_maker, "_fetch_binance_depth_snapshot", fake_snapshot)
    monkeypatch.setattr(market_maker, "OBI_MIN_WARMUP_SAMPLES", 5)
    saved_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        asyncio.run(asyncio.wait_for(market_maker.main(), timeout=30))
    finally:
        for sig, handler in saved_handlers.items():
            signal.signal(sig, handler)
        logging_config.stop_root_logging()
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    client = FakeClient.last
    sides = {side: price for side, price, _ in client.placed}
    assert set(sides) == {"BUY", "SELL"}
    assert sides["BUY"] < MID < sides["SELL"]
    assert client.cancel_all_calls >= 2  # clean slate at startup + cleanup at shutdown
    assert not client.open_orders
    errors = (tmp_path / "market_maker.log").read_text()  # release mode logs errors only
    assert "Traceback" not in errors and "ERROR" not in errors, errors
