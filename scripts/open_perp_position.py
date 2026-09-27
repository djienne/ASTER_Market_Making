import os
import time
import argparse
import asyncio
import aiohttp
from dotenv import load_dotenv
import os, sys  # noqa: E401
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root for api_client
from api_client import ApiClient

# Load environment variables
load_dotenv()

# Global variables to monitor the state
TRADE_STATE = {}
MID_PRICE = None


# --- Standalone Functions for Background Tasks ---

async def get_mid_price(session, symbol):
    """Gets the mid-price for a given symbol asynchronously."""
    url = f"https://fapi.asterdex.com/fapi/v1/ticker/bookTicker"
    params = {"symbol": symbol}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        data = await response.json()
        bid_price = float(data["bidPrice"])
        ask_price = float(data["askPrice"])
        return (bid_price + ask_price) / 2


async def price_updater(symbol, interval):
    """Continuously updates the mid-price for a symbol."""
    global MID_PRICE
    async with aiohttp.ClientSession() as session:
        try:
            while True:
                try:
                    MID_PRICE = await get_mid_price(session, symbol)
                    print(f"Updated mid-price for {symbol}: {MID_PRICE}")
                except aiohttp.ClientError as e:
                    print(f"Error updating price: {e}")
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            print("Price updater cancelled")
            raise


# --- Main Application Logic ---

async def open_and_monitor_order(client, symbol, spread, quantity, side):
    """Opens and monitors a perpetual position using an ApiClient instance."""
    global TRADE_STATE, MID_PRICE

    print("Waiting for the first price update...")
    while MID_PRICE is None:
        await asyncio.sleep(1)

    try:
        price_precision, tick_size = await client.get_symbol_precision(symbol)
    except ValueError as e:
        print(f"Error: {e}")
        return

    limit_price = MID_PRICE * (1 - spread) if side == "BUY" else MID_PRICE * (1 + spread)
    rounded_price = round(limit_price / tick_size) * tick_size
    formatted_price = f"{rounded_price:.{price_precision}f}"

    print(f"Using mid-price: {MID_PRICE}")
    print(f"Calculated limit price: {formatted_price} (precision: {price_precision}, tick size: {tick_size})")

    try:
        print("Placing order...")
        order_data = await client.place_order(symbol, formatted_price, str(quantity), side)

        TRADE_STATE.update({
            'order_id': order_data.get('orderId'),
            'symbol': order_data.get('symbol'),
            'price': order_data.get('price'),
            'quantity': order_data.get('origQty'),
            'side': order_data.get('side'),
            'status': order_data.get('status')
        })
        order_id = TRADE_STATE.get("order_id")
        print(f"Order placed successfully. Current state: {TRADE_STATE}")

        max_checks = 30
        check_count = 0
        while TRADE_STATE.get('status') not in ["FILLED", "CANCELED", "REJECTED", "EXPIRED"] and check_count < max_checks:
            print("Checking order status...")
            status_data = await client.get_order_status(symbol, order_id)
            TRADE_STATE['status'] = status_data.get('status')
            print(f"Current trade state: {TRADE_STATE}")
            check_count += 1
            await asyncio.sleep(10)

        if check_count >= max_checks:
            print("Maximum monitoring time reached. Stopping.")

        print(f"Order monitoring finished. Final status: {TRADE_STATE.get('status')}")

    except aiohttp.ClientError as e:
        print(f"An error occurred: {e}")


async def main():
    """Main entry point for the script."""
    # Global configuration
    QUANTITY = 0.01
    PRICE_INTERVAL = 2
    SPREAD = 0.001

    API_USER = os.getenv("API_USER")
    API_SIGNER = os.getenv("API_SIGNER")
    API_PRIVATE_KEY = os.getenv("API_PRIVATE_KEY")

    parser = argparse.ArgumentParser(description="Open a perpetual position with a limit post-only order.")
    parser.add_argument("--symbol", type=str, default="ETHUSDT", help="The symbol to trade (default: ETHUSDT).")
    parser.add_argument("--side", type=str, default="BUY", choices=["BUY", "SELL"], help="Order side (default: BUY).")
    args = parser.parse_args()

    try:
        client = ApiClient(API_USER, API_SIGNER, API_PRIVATE_KEY)
    except ValueError as e:
        print(f"Initialization Error: {e}")
        exit(1)

    price_updater_task = asyncio.create_task(price_updater(args.symbol, PRICE_INTERVAL))

    async with client:
        order_task = asyncio.create_task(open_and_monitor_order(client, args.symbol, SPREAD, QUANTITY, args.side))
        try:
            await order_task
        finally:
            price_updater_task.cancel()
            try:
                await price_updater_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
