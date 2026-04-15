#!/usr/bin/env python3
"""
Quick test of account balance monitoring functionality
"""

import asyncio
import json
import os
import websockets
from datetime import datetime
from dotenv import load_dotenv
from api_client import ApiClient

load_dotenv()

async def test_account_balance():
    """Quick test of account balance monitoring."""
    print("ASTER FINANCE ACCOUNT BALANCE TEST")
    print("=" * 50)

    # Get API keys
    api_user = os.getenv('API_USER')
    api_signer = os.getenv('API_SIGNER')
    api_private_key = os.getenv('API_PRIVATE_KEY')

    if not all([apiv1_public, apiv1_private]):
        print("ERROR: Missing API_USER, API_SIGNER, or API_PRIVATE_KEY")
        return

    # Step 1: Get current account state
    print("\n1. Fetching current account state...")
    client = ApiClient(api_user, api_signer, api_private_key)

    async with client:
        try:
            account_info = await client.signed_request("GET", "/fapi/v3/account", {})
            balances = account_info.get('assets', [])

            print("Current Account Balances:")
            for balance in balances:
                asset = balance.get('asset', 'N/A')
                wallet_balance = balance.get('walletBalance', '0')
                if float(wallet_balance) > 0.01:
                    print(f"  {asset}: {wallet_balance}")

            positions = account_info.get('positions', [])
            print("\nCurrent Positions:")
            active_positions = 0
            for position in positions:
                symbol = position.get('symbol', 'N/A')
                position_amt = position.get('positionAmt', '0')
                if float(position_amt) != 0:
                    active_positions += 1
                    side = "LONG" if float(position_amt) > 0 else "SHORT"
                    pnl = position.get('unrealizedPnl', '0')
                    print(f"  {symbol} ({side}): {position_amt} (PnL: {pnl} USDT)")

            if active_positions == 0:
                print("  No active positions")

        except Exception as e:
            print(f"WARNING: Could not fetch account state: {e}")

    # Step 2: Test WebSocket connection
    print("\n2. Testing WebSocket connection...")
    async with client:
        try:
            response = await client.signed_request(
                "POST", "/fapi/v3/listenKey", {}
            )
            listen_key = response['listenKey']
            print(f"   Listen key obtained: {listen_key[:20]}...")

            # Test WebSocket connection briefly
            ws_url = f"wss://fstream.asterdex.com/ws/{listen_key}"
            print("   Testing WebSocket connection...")

            async with websockets.connect(ws_url) as ws:
                print("   WebSocket connected successfully!")
                print("   Listening for 5 seconds...")

                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = json.loads(message)
                    event_type = data.get('e', 'unknown')
                    print(f"   Received event: {event_type}")
                except asyncio.TimeoutError:
                    print("   No events received (this is normal if no trading activity)")

        except Exception as e:
            print(f"ERROR: WebSocket test failed: {e}")

    print("\n3. Test completed!")
    print("To run full monitoring, use: python account_balance_monitor.py")

if __name__ == "__main__":
    asyncio.run(test_account_balance())
