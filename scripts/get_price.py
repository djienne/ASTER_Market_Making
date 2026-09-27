import requests
import argparse

def get_price(symbol):
    """
    Fetches bid and ask prices for spot and perpetual markets, and calculates the mid-price.
    """
    # Base URLs for the APIs
    FUTURES_BASE_URL = "https://fapi.asterdex.com"
    SPOT_BASE_URL = "https://fapi.asterdex.com"

    # API endpoints for order book tickers
    futures_endpoint = f"{FUTURES_BASE_URL}/fapi/v1/ticker/bookTicker"
    spot_endpoint = f"{SPOT_BASE_URL}/fapi/v1/ticker/bookTicker"

    # Parameters for the requests
    params = {'symbol': symbol}

    try:
        # Get the futures prices
        futures_response = requests.get(futures_endpoint, params=params)
        futures_response.raise_for_status()
        futures_data = futures_response.json()
        futures_bid = float(futures_data['bidPrice'])
        futures_ask = float(futures_data['askPrice'])
        futures_mid = (futures_bid + futures_ask) / 2

        # Get the spot prices
        spot_response = requests.get(spot_endpoint, params=params)
        spot_response.raise_for_status()
        spot_data = spot_response.json()
        spot_bid = float(spot_data['bidPrice'])
        spot_ask = float(spot_data['askPrice'])
        spot_mid = (spot_bid + spot_ask) / 2

        # Print the results
        print(f"Symbol: {symbol}")
        print("\n--- Spot Market ---")
        print(f"  Bid Price: {spot_bid}")
        print(f"  Ask Price: {spot_ask}")
        print(f"  Mid Price: {spot_mid:.6f}")
        print("\n--- Perpetual Market ---")
        print(f"  Bid Price: {futures_bid}")
        print(f"  Ask Price: {futures_ask}")
        print(f"  Mid Price: {futures_mid:.6f}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    except KeyError:
        print(f"Could not find price for the symbol '{symbol}'. Please check if the symbol is correct.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get spot and perpetual prices for a crypto asset.')
    parser.add_argument('symbol', type=str, nargs='?', default='ASTERUSDT',
                        help='The trading symbol to fetch prices for (e.g., BTCUSDT). Defaults to ASTERUSDT.')
    args = parser.parse_args()

    get_price(args.symbol)