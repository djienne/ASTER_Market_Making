#!/usr/bin/env python3
"""
Simple test to check WebSocket connectivity
"""

import asyncio
import sys
from dotenv import load_dotenv

# Fix Windows encoding issues
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

print("🚀 Starting simple WebSocket test")

async def test_websocket():
    """Test WebSocket connection."""
    try:
        print("📦 Importing websockets...")
        import websockets
        print("✅ websockets imported successfully")

        # Test connection to a known endpoint
        test_url = "wss://fstream.asterdex.com/ws/btcusdt@ticker"
        print(f"🔗 Connecting to: {test_url}")

        async with websockets.connect(test_url) as websocket:
            print("✅ WebSocket connected!")

            # Wait for one message
            message = await asyncio.wait_for(websocket.recv(), timeout=5)
            print(f"📨 Received message: {message[:100]}...")

    except ImportError as e:
        print(f"❌ Import error: {e}")
    except Exception as e:
        print(f"❌ Connection error: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket())