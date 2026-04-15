#!/usr/bin/env python3
"""
Simplified WebSocket User Data Stream - No emojis for Windows compatibility
"""

import asyncio
import json
import os
import sys
import websockets
from datetime import datetime
from dotenv import load_dotenv
from api_client import ApiClient

# Load environment variables
load_dotenv()

API_USER = os.getenv('API_USER')
API_SIGNER = os.getenv('API_SIGNER')
API_PRIVATE_KEY = os.getenv('API_PRIVATE_KEY')

class UserDataStream:
    def __init__(self):
        self.api_client = ApiClient(API_USER, API_SIGNER, API_PRIVATE_KEY)
        self.listen_key = None
        self.websocket = None
        self.should_reconnect = True

    async def get_listen_key(self):
        """Get a listen key for the user data stream."""
        try:
            async with self.api_client as client:
                response = await client.signed_request(
                    "POST", "/fapi/v3/listenKey", {}
                )
                self.listen_key = response.get('listenKey')
                print(f"[INFO] Listen key obtained: {self.listen_key[:20]}...")
                return self.listen_key
        except Exception as e:
            print(f"[ERROR] Getting listen key: {e}")
            return None

    def print_trade_execution(self, order_data):
        """Print trade execution information."""
        symbol = order_data.get('s', 'N/A')
        side = order_data.get('S', 'N/A')
        filled_qty = order_data.get('l', '0')
        filled_price = order_data.get('L', '0')
        order_id = order_data.get('i', 'N/A')
        trade_time = datetime.fromtimestamp(order_data.get('T', 0) / 1000).strftime('%H:%M:%S')

        print("\n" + "="*60)
        print(f"TRADE EXECUTION - {symbol}")
        print("="*60)
        print(f"Order ID: {order_id}")
        print(f"Time: {trade_time}")
        print(f"Side: {side}")
        print(f"Filled: {filled_qty} @ ${filled_price}")
        print("="*60)

    async def handle_message(self, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            event_type = data.get('e')

            if event_type == 'ORDER_TRADE_UPDATE':
                order_data = data.get('o', {})
                exec_type = order_data.get('x', '')

                if exec_type == 'TRADE':
                    self.print_trade_execution(order_data)
                else:
                    symbol = order_data.get('s', 'N/A')
                    side = order_data.get('S', 'N/A')
                    status = order_data.get('X', 'N/A')
                    print(f"[ORDER] {symbol} {side} -> {status}")

            elif event_type == 'ACCOUNT_UPDATE':
                print(f"[ACCOUNT] Balance/Position update received")

            else:
                print(f"[EVENT] {event_type}: {data}")

        except Exception as e:
            print(f"[ERROR] Handling message: {e}")

    async def connect_and_listen(self):
        """Connect to WebSocket and listen for messages."""
        if not self.listen_key:
            if not await self.get_listen_key():
                return False

        try:
            ws_url = f"wss://fstream.asterdex.com/ws/{self.listen_key}"
            print(f"[INFO] Connecting to: {ws_url[:50]}...")

            async with websockets.connect(ws_url) as websocket:
                print("[SUCCESS] Connected to user data stream!")
                print("[INFO] Listening for trade executions... Press Ctrl+C to stop")

                async for message in websocket:
                    await self.handle_message(message)

        except Exception as e:
            print(f"[ERROR] WebSocket error: {e}")
            return False

async def main():
    """Main function."""
    print("Starting Aster Finance User Data Stream")
    print("="*50)

    # Validate credentials
    if not all([API_USER, API_SIGNER, API_PRIVATE_KEY]):
        print("[ERROR] Missing API_USER, API_SIGNER, or API_PRIVATE_KEY in .env file")
        return

    print("[INFO] Credentials found")

    # Start stream
    stream = UserDataStream()

    try:
        await stream.connect_and_listen()
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down...")

if __name__ == "__main__":
    asyncio.run(main())

