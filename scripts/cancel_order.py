import os
import asyncio
from dotenv import load_dotenv
import os, sys  # noqa: E401
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root for api_client
from api_client import ApiClient

# --- Configuration ---
# The symbol to test with.
TEST_SYMBOL = "ETHUSDT"
# The side of the order to place.
ORDER_SIDE = "BUY"
# The quantity for the test order.
ORDER_QUANTITY = "0.05"
# A price far from the market to ensure the order remains open.
# We'll place a BUY order far below the market price.
FAR_PRICE = "100.0"


async def main():
    """
    Places an order, waits, and then cancels all orders for the symbol.
    """
    # Load environment variables
    load_dotenv()
    API_USER = os.getenv("API_USER")
    API_SIGNER = os.getenv("API_SIGNER")
    API_PRIVATE_KEY = os.getenv("API_PRIVATE_KEY")

    print("--- Test Script: Place and Cancel Order ---")

    try:
        client = ApiClient(API_USER, API_SIGNER, API_PRIVATE_KEY)
    except ValueError as e:
        print(f"Initialization Error: {e}")
        return

    async with client:
        try:
            # 1. Place a new order
            print(f"\n[Step 1] Placing a new {ORDER_SIDE} order for {ORDER_QUANTITY} {TEST_SYMBOL} at price {FAR_PRICE}...")
            place_response = await client.place_order(
                symbol=TEST_SYMBOL,
                price=FAR_PRICE,
                quantity=ORDER_QUANTITY,
                side=ORDER_SIDE
            )
            print("-> Successfully placed order:")
            print(place_response)
            order_id = place_response.get('orderId')

            if not order_id:
                print("Error: Could not get orderId from place_order response. Exiting.")
                return

            # 2. Wait for 10 seconds
            print("\n[Step 2] Waiting for 10 seconds...")
            await asyncio.sleep(10)

            # 3. Verify the order is open
            print(f"\n[Step 3] Verifying order {order_id} is still open...")
            status_response = await client.get_order_status(TEST_SYMBOL, order_id)
            print("-> Current order status:")
            print(status_response)
            if status_response.get('status') != 'NEW':
                print(f"Warning: Order status is '{status_response.get('status')}', not 'NEW'. The cancel call might not have any effect.")

            # 4. Cancel all orders for the symbol
            print(f"\n[Step 4] Cancelling all open orders for {TEST_SYMBOL}...")
            cancel_response = await client.cancel_all_orders(TEST_SYMBOL)
            print("-> Successfully sent cancel all request:")
            print(cancel_response)

            # 5. Verify the order is now canceled
            print(f"\n[Step 5] Verifying order {order_id} is now canceled...")
            await asyncio.sleep(2)  # Give server a moment to process cancellation
            final_status_response = await client.get_order_status(TEST_SYMBOL, order_id)
            print("-> Final order status:")
            print(final_status_response)

            if final_status_response.get('status') == 'CANCELED':
                print("\n--- Test SUCCESS: Order was placed and then canceled. ---")
            else:
                print(f"\n--- Test FAILED: Order status is '{final_status_response.get('status')}', expected 'CANCELED'. ---")

        except Exception as e:
            print(f"\nAn error occurred during the test: {e}")


if __name__ == "__main__":
    asyncio.run(main())
