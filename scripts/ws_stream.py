"""Print one Aster public futures stream, one line per event.

    python scripts/ws_stream.py ETHUSDT depth5 --seconds 30

STREAM is any Aster stream suffix: depth5, depth@100ms, aggTrade, trade,
markPrice@1s, ticker, bookTicker, ... No credentials needed.
"""
import argparse
import asyncio
import json
import time

import websockets


def format_event(event):
    """One compact line per known event type; anything else as raw JSON."""
    kind = event.get("e")
    if kind == "depthUpdate":
        bid = (event.get("b") or [["-", "-"]])[0]
        ask = (event.get("a") or [["-", "-"]])[0]
        return f"depth    bid {bid[0]} x {bid[1]} | ask {ask[0]} x {ask[1]}"
    if kind in ("aggTrade", "trade"):
        side = "SELL" if event.get("m") else "BUY"  # m: buyer is maker -> taker sold
        return f"{kind:<8} {side:<4} {event.get('q')} @ {event.get('p')}"
    if kind == "markPriceUpdate":
        return f"mark     {event.get('p')} index {event.get('i')} funding {event.get('r')}"
    if kind == "24hrTicker":
        return f"ticker   last {event.get('c')} change {event.get('P')}% volume {event.get('v')}"
    if kind == "bookTicker":
        return f"book     bid {event.get('b')} x {event.get('B')} | ask {event.get('a')} x {event.get('A')}"
    return json.dumps(event, separators=(",", ":"))


async def stream(symbol, name, seconds):
    url = f"wss://fstream.asterdex.com/ws/{symbol.lower()}@{name}"
    print(f"Connecting to {url} for {seconds:.0f}s ...")
    deadline = time.monotonic() + seconds
    async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            print(format_event(json.loads(message)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("symbol", help="e.g. ETHUSDT")
    parser.add_argument("stream", help="e.g. depth5, aggTrade, markPrice@1s, ticker, bookTicker")
    parser.add_argument("--seconds", type=float, default=30.0)
    args = parser.parse_args()
    asyncio.run(stream(args.symbol, args.stream, args.seconds))
