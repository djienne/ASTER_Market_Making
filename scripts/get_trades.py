import requests
import argparse
from datetime import datetime

def get_recent_trades(symbol, limit):
    """
    Fetches and displays the most recent public trades for a given symbol.
    """
    BASE_URL = "https://fapi.asterdex.com"
    endpoint = f"{BASE_URL}/fapi/v1/trades"

    params = {'symbol': symbol, 'limit': limit}

    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        trades = response.json()

        if not trades:
            print(f"No recent trades found for {symbol}.")
            return

        print(f"--- Last {len(trades)} Trades for {symbol} ---")
        print(f"{'Timestamp':<26} {'Side':<6} {'Price':<18} {'Quantity'}")
        print("-" * 68)

        for trade in trades:
            # The 'time' is a Unix timestamp in milliseconds
            timestamp = datetime.fromtimestamp(trade['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # isBuyerMaker: true -> Taker was a seller (SELL)
            # isBuyerMaker: false -> Taker was a buyer (BUY)
            side = "SELL" if trade['isBuyerMaker'] else "BUY"
            
            price = f"{float(trade['price']):.6f}"
            quantity = f"{float(trade['qty']):.6f}"

            print(f"{timestamp:<26} {side:<6} {price:<18} {quantity}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
    except KeyError:
        print(f"Could not parse trade data for the symbol '{symbol}'. Please check if the symbol is correct.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get the last executed trades for a crypto asset.')
    parser.add_argument('symbol', type=str, nargs='?', default='ASTERUSDT',
                        help='The trading symbol to fetch trades for (e.g., BTCUSDT). Defaults to ASTERUSDT.')
    parser.add_argument('--limit', type=int, default=20,
                        help='Number of recent trades to fetch (max: 1000). Defaults to 20.')
    args = parser.parse_args()

    get_recent_trades(args.symbol, args.limit)
