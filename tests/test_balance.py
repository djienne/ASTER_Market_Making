import os
import asyncio
from dotenv import load_dotenv
from api_client import ApiClient

async def main():
    """
    Manual live utility: fetch and print the current USDT wallet snapshot.
    """
    load_dotenv()
    API_USER = os.getenv("API_USER")
    API_SIGNER = os.getenv("API_SIGNER")
    API_PRIVATE_KEY = os.getenv("API_PRIVATE_KEY")

    print("--- Manual Balance Check ---")

    try:
        client = ApiClient(API_USER, API_SIGNER, API_PRIVATE_KEY)
    except ValueError as e:
        print(f"Initialization Error: {e}")
        return

    async with client:
        try:
            balance_data = await client.signed_request("GET", "/fapi/v3/account", {})
            print("\nFull balance response:")
            print(balance_data)

            usdt_balance = None
            for asset in balance_data.get('assets', []):
                if asset.get("asset") == "USDT":
                    usdt_balance = asset
                    break
            
            if usdt_balance:
                wallet_balance = usdt_balance.get("walletBalance")
                print(f"\nSuccessfully found USDT balance.")
                print(f"Wallet Balance (USDT): {wallet_balance}")
            else:
                print("\nCould not find USDT balance in the response.")

        except Exception as e:
            print(f"\nAn error occurred during the test: {e}")

if __name__ == "__main__":
    asyncio.run(main())
