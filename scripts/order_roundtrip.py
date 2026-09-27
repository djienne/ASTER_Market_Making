"""Order-path check: place a passive GTX limit away from the market, read it back, cancel it.

    python scripts/order_roundtrip.py --symbol ETHUSDT --side BUY

Places a REAL order on the live exchange (then cancels it): the minimum
size, --away-bps (default 100 = 1%) behind the touch on the passive side, so
it only fills if price moves that far during the ~2 s it rests. Keep
--away-bps inside the symbol's PERCENT_PRICE band (ETH/BTC: 2%).
Needs .env credentials.
"""
import argparse
import asyncio
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root

import utils  # noqa: E402,F401  (loads .env and runtime.env)
from api_client import ApiClient  # noqa: E402
from market_maker import round_price_to_tick  # noqa: E402


async def main(symbol, side, away_bps):
    client = ApiClient(os.getenv("API_USER"), os.getenv("API_SIGNER"), os.getenv("API_PRIVATE_KEY"))
    async with client:
        filters = await client.get_symbol_filters(symbol)
        async with client.session.get(f"{client.base_url}/fapi/v1/ticker/bookTicker",
                                      params={"symbol": symbol}) as response:
            response.raise_for_status()
            book = await response.json()

        away = away_bps / 10000.0
        raw_price = float(book["bidPrice"]) * (1 - away) if side == "BUY" else float(book["askPrice"]) * (1 + away)
        price = round_price_to_tick(raw_price, filters["tick_size"], side)
        # Smallest step multiple above both minQty and 1.1 x minNotional.
        step = filters["step_size"]
        quantity = math.ceil(max(filters["min_qty"], 1.1 * filters["min_notional"] / price) / step - 1e-9) * step

        price_str = f"{price:.{filters['price_precision']}f}"
        quantity_str = f"{quantity:.{filters['quantity_precision']}f}"
        print(f"Book {book['bidPrice']} / {book['askPrice']} -> placing GTX {side} {quantity_str} {symbol} @ {price_str}")

        order = await client.place_order(symbol, price_str, quantity_str, side)
        order_id = order["orderId"]
        try:
            await asyncio.sleep(1.0)
            status = await client.get_order_status(symbol, order_id)
            print(f"Order {order_id} status: {status.get('status')} (filled {status.get('executedQty')})")
        finally:
            await client.cancel_order(symbol, order_id)
            status = await client.get_order_status(symbol, order_id)
            print(f"After cancel: {status.get('status')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", default=utils.configured_symbol())
    parser.add_argument("--side", choices=("BUY", "SELL"), default="BUY")
    parser.add_argument("--away-bps", type=float, default=100.0)
    args = parser.parse_args()
    asyncio.run(main(args.symbol.upper(), args.side, args.away_bps))
