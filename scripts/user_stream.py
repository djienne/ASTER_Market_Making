"""Print the Aster user data stream (orders, fills, balances, positions).

    python scripts/user_stream.py --seconds 120

Exercises the whole Pro API V3 path the bot uses: signed listen-key request,
user stream connection, keepalive. Needs aster.env credentials.

The listen key is shared per account (a running bot uses the same one), so
it is deliberately NOT closed on exit; it simply expires 60 min after the
last keepalive.
"""
import argparse
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root

import websockets  # noqa: E402

import utils  # noqa: E402,F401  (loads aster.env and runtime.env)
from api_client import ApiClient  # noqa: E402

KEEPALIVE_SECONDS = 30 * 60


def describe(event):
    """One line per event: key fields for orders and account updates, raw JSON otherwise."""
    kind = event.get("e")
    if kind == "ORDER_TRADE_UPDATE":
        o = event.get("o", {})
        return (f"ORDER   {o.get('s')} {o.get('S')} {o.get('X')} id={o.get('i')} "
                f"price={o.get('p')} qty={o.get('q')} filled={o.get('z')}")
    if kind == "ACCOUNT_UPDATE":
        a = event.get("a", {})
        balances = ", ".join(f"{b.get('a')}={b.get('wb')}" for b in a.get("B", []))
        positions = ", ".join(f"{p.get('s')}={p.get('pa')}" for p in a.get("P", []))
        return f"ACCOUNT balances[{balances}] positions[{positions}]"
    return f"{kind}: {json.dumps(event, separators=(',', ':'))}"


async def main(seconds):
    client = ApiClient(os.getenv("API_USER"), os.getenv("API_SIGNER"), os.getenv("API_PRIVATE_KEY"))
    async with client:
        listen_key = (await client.create_listen_key())["listenKey"]
        print(f"Listen key {listen_key[:8]}... streaming for {seconds:.0f}s")
        deadline = time.monotonic() + seconds
        next_keepalive = time.monotonic() + KEEPALIVE_SECONDS
        async with websockets.connect(f"wss://fstream.asterdex.com/ws/{listen_key}",
                                      ping_interval=20, ping_timeout=10) as ws:
            while (remaining := deadline - time.monotonic()) > 0:
                if time.monotonic() >= next_keepalive:
                    await client.keepalive_listen_key()
                    next_keepalive += KEEPALIVE_SECONDS
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 30.0))
                except asyncio.TimeoutError:
                    continue
                print(describe(json.loads(message)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seconds", type=float, default=120.0)
    asyncio.run(main(parser.parse_args().seconds))
