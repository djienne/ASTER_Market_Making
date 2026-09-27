"""Offline checks for the manual scripts' event formatting (no network)."""
import json

from scripts.user_stream import describe
from scripts.ws_stream import format_event


def test_ws_stream_formats_each_public_event_on_one_line():
    samples = {
        "depthUpdate": ({"e": "depthUpdate", "b": [["2689.49", "1.5"]], "a": [["2689.50", "2"]]}, "bid 2689.49 x 1.5 | ask 2689.50 x 2"),
        "aggTrade": ({"e": "aggTrade", "p": "2689.5", "q": "0.3", "m": True}, "SELL 0.3 @ 2689.5"),
        "trade": ({"e": "trade", "p": "2689.5", "q": "0.3", "m": False}, "BUY  0.3 @ 2689.5"),
        "markPriceUpdate": ({"e": "markPriceUpdate", "p": "2689.1", "i": "2689.0", "r": "0.0001"}, "mark     2689.1"),
        "24hrTicker": ({"e": "24hrTicker", "c": "2690", "P": "1.2", "v": "1000"}, "last 2690 change 1.2%"),
        "bookTicker": ({"e": "bookTicker", "b": "1", "B": "2", "a": "3", "A": "4"}, "bid 1 x 2 | ask 3 x 4"),
    }
    for kind, (event, expected) in samples.items():
        line = format_event(event)
        assert "\n" not in line and expected in line, (kind, line)

    unknown = {"e": "somethingNew", "x": 1}
    assert json.loads(format_event(unknown)) == unknown


def test_user_stream_describes_orders_and_account_updates():
    order = {"e": "ORDER_TRADE_UPDATE", "o": {"s": "ETHUSDT", "S": "BUY", "X": "FILLED", "i": 7, "p": "1", "q": "2", "z": "2"}}
    account = {"e": "ACCOUNT_UPDATE", "a": {"B": [{"a": "USDT", "wb": "100"}], "P": [{"s": "ETHUSDT", "pa": "0.5"}]}}
    assert describe(order) == "ORDER   ETHUSDT BUY FILLED id=7 price=1 qty=2 filled=2"
    assert describe(account) == "ACCOUNT balances[USDT=100] positions[ETHUSDT=0.5]"
    assert describe({"e": "listenKeyExpired"}).startswith("listenKeyExpired: ")
