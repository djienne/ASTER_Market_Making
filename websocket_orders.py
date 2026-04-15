#!/usr/bin/env python3
"""
Extended demo script showing detailed user data stream functionality
Monitors for a longer period and shows comprehensive order/trade information
"""

import asyncio
import json
import os
import websockets
from datetime import datetime
from dotenv import load_dotenv
from api_client import ApiClient

load_dotenv()

class UserStreamMonitor:
    def __init__(self):
        self.order_count = 0
        self.trade_count = 0
        self.account_updates = 0
        self.start_time = datetime.now()

    def format_timestamp(self, timestamp_ms):
        """Convert timestamp to readable format."""
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S.%f')[:-3]

    def print_detailed_order(self, order_data):
        """Print detailed order information."""
        symbol = order_data.get('s', 'N/A')
        client_order_id = order_data.get('c', 'N/A')
        side = order_data.get('S', 'N/A')
        order_type = order_data.get('o', 'N/A')
        time_in_force = order_data.get('f', 'N/A')
        quantity = order_data.get('q', '0')
        price = order_data.get('p', '0')
        avg_price = order_data.get('ap', '0')
        exec_type = order_data.get('x', 'N/A')
        order_status = order_data.get('X', 'N/A')
        order_id = order_data.get('i', 'N/A')
        last_filled_qty = order_data.get('l', '0')
        cum_filled_qty = order_data.get('z', '0')
        last_filled_price = order_data.get('L', '0')
        commission = order_data.get('n', '0')
        commission_asset = order_data.get('N', 'N/A')
        trade_time = self.format_timestamp(order_data.get('T', 0))
        trade_id = order_data.get('t', 0)
        is_maker = order_data.get('m', False)
        reduce_only = order_data.get('R', False)
        position_side = order_data.get('ps', 'N/A')
        realized_pnl = order_data.get('rp', '0')

        print("\n" + "="*80)
        print(f"ORDER UPDATE #{self.order_count + 1} - {symbol}")
        print("="*80)
        print(f"Time: {trade_time}")
        print(f"Order ID: {order_id}")
        print(f"Client Order ID: {client_order_id}")
        print(f"Side: {side} | Position Side: {position_side}")
        print(f"Type: {order_type} | Time in Force: {time_in_force}")
        print(f"Execution Type: {exec_type} -> Order Status: {order_status}")

        print(f"\nOrder Details:")
        print(f"  Original Quantity: {quantity}")
        print(f"  Original Price: ${price}")
        print(f"  Cumulative Filled: {cum_filled_qty}")

        if order_status == 'PARTIALLY_FILLED':
            print(f"  [PARTIALLY FILLED - Cumulative: {cum_filled_qty} / {quantity}]")

        if float(avg_price) > 0:
            print(f"  Average Fill Price: ${avg_price}")

        if float(last_filled_qty) > 0:
            print(f"\nLast Fill:")
            print(f"  Quantity: {last_filled_qty}")
            print(f"  Price: ${last_filled_price}")
            print(f"  Trade ID: {trade_id}")
            print(f"  Maker: {'Yes' if is_maker else 'No'}")

        if float(commission) > 0:
            print(f"  Commission: {commission} {commission_asset}")

        if float(realized_pnl) != 0:
            pnl_sign = "+" if float(realized_pnl) > 0 else ""
            print(f"  Realized PnL: {pnl_sign}{realized_pnl} USDT")

        if reduce_only:
            print(f"  [REDUCE ONLY ORDER]")

        print("="*80)

        # Count trades
        if exec_type == 'TRADE':
            self.trade_count += 1
            print(f"*** TRADE EXECUTION #{self.trade_count} ***")

        self.order_count += 1

    def print_account_update(self, account_data):
        """Print detailed account update information."""
        reason = account_data.get('m', 'N/A')

        print("\n" + "="*80)
        print(f"ACCOUNT UPDATE #{self.account_updates + 1} - {reason}")
        print("="*80)

        # Balance updates
        balances = account_data.get('B', [])
        if balances:
            print("Balance Changes:")
            for balance in balances:
                asset = balance.get('a', 'N/A')
                wallet_balance = balance.get('wb', '0')
                cross_wallet_balance = balance.get('cw', '0')
                balance_change = balance.get('bc', '0')

                if float(balance_change) != 0:
                    change_sign = "+" if float(balance_change) > 0 else ""
                    print(f"  {asset}:")
                    print(f"    Wallet Balance: {wallet_balance}")
                    print(f"    Cross Wallet: {cross_wallet_balance}")
                    print(f"    Change: {change_sign}{balance_change}")

        # Position updates
        positions = account_data.get('P', [])
        if positions:
            print("\nPosition Updates:")
            for position in positions:
                symbol = position.get('s', 'N/A')
                position_amt = position.get('pa', '0')
                entry_price = position.get('ep', '0')
                accumulated_realized = position.get('cr', '0')
                unrealized_pnl = position.get('up', '0')
                margin_type = position.get('mt', 'N/A')
                isolated_wallet = position.get('iw', '0')
                position_side = position.get('ps', 'N/A')

                if float(position_amt) != 0:
                    print(f"  {symbol} ({position_side}):")
                    print(f"    Position Size: {position_amt}")
                    print(f"    Entry Price: ${entry_price}")
                    print(f"    Unrealized PnL: {unrealized_pnl}")
                    print(f"    Margin Type: {margin_type}")
                    if float(isolated_wallet) > 0:
                        print(f"    Isolated Wallet: {isolated_wallet}")

        print("="*80)
        self.account_updates += 1

    def print_statistics(self):
        """Print session statistics."""
        duration = datetime.now() - self.start_time
        duration_seconds = duration.total_seconds()

        print("\n" + "="*80)
        print("SESSION STATISTICS")
        print("="*80)
        print(f"Session Duration: {int(duration_seconds // 60)}m {int(duration_seconds % 60)}s")
        print(f"Order Updates: {self.order_count}")
        print(f"Trade Executions: {self.trade_count}")
        print(f"Account Updates: {self.account_updates}")
        print(f"Total Events: {self.order_count + self.account_updates}")

        if duration_seconds > 0:
            events_per_minute = (self.order_count + self.account_updates) * 60 / duration_seconds
            print(f"Events per minute: {events_per_minute:.1f}")

        print("="*80)

async def extended_demo():
    """Extended demo with detailed monitoring."""
    print("=== ASTER FINANCE USER DATA STREAM - EXTENDED DEMO ===")

    # Get API keys
    api_user = os.getenv('API_USER')
    api_signer = os.getenv('API_SIGNER')
    api_private_key = os.getenv('API_PRIVATE_KEY')

    if not all([apiv1_public, apiv1_private]):
        print("ERROR: Missing API_USER, API_SIGNER, or API_PRIVATE_KEY")
        return

    monitor = UserStreamMonitor()

    # Step 1: Get listen key
    print("\n1. Getting listen key...")
    client = ApiClient(api_user, api_signer, api_private_key)

    async with client:
        response = await client.signed_request(
            "POST", "/fapi/v3/listenKey", {}
        )
        listen_key = response['listenKey']
        print(f"   Listen key: {listen_key[:20]}...")

    # Step 2: Connect to WebSocket
    print("\n2. Connecting to WebSocket...")
    ws_url = f"wss://fstream.asterdex.com/ws/{listen_key}"

    print("\n3. Starting extended monitoring...")
    print("   Duration: 2 minutes (120 seconds)")
    print("   Press Ctrl+C to stop early")
    print("   Monitoring for:")
    print("   - Order status changes (NEW, PARTIALLY_FILLED, FILLED, CANCELED)")
    print("   - Trade executions with full details")
    print("   - Account balance and position updates")
    print("   - Commission and PnL information")

    try:
        async with websockets.connect(ws_url) as ws:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Connected! Monitoring started...")

            # Monitor for 2 minutes or until interrupted
            end_time = asyncio.get_event_loop().time() + 120  # 2 minutes

            while asyncio.get_event_loop().time() < end_time:
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = json.loads(message)
                    event_type = data.get('e', 'unknown')
                    event_time = monitor.format_timestamp(data.get('E', 0))

                    if event_type == 'ORDER_TRADE_UPDATE':
                        order_data = data.get('o', {})
                        monitor.print_detailed_order(order_data)

                    elif event_type == 'ACCOUNT_UPDATE':
                        account_data = data.get('a', {})
                        monitor.print_account_update(account_data)

                    elif event_type == 'MARGIN_CALL':
                        print(f"\n[{event_time}] *** MARGIN CALL ALERT ***")
                        print(f"Cross Wallet Balance: {data.get('cw', 'N/A')}")
                        positions = data.get('p', [])
                        for pos in positions:
                            symbol = pos.get('s', 'N/A')
                            side = pos.get('ps', 'N/A')
                            amount = pos.get('pa', '0')
                            unrealized_pnl = pos.get('up', '0')
                            print(f"Position at risk: {symbol} {side} {amount} (PnL: {unrealized_pnl})")

                    elif event_type == 'listenKeyExpired':
                        print(f"\n[{event_time}] Listen key expired - would need to reconnect")
                        break

                    else:
                        print(f"\n[{event_time}] Unknown event type: {event_type}")
                        print(f"Raw data: {json.dumps(data, indent=2)}")

                except asyncio.TimeoutError:
                    # No message received in timeout period - this is normal
                    current_time = datetime.now().strftime('%H:%M:%S')
                    remaining = int(end_time - asyncio.get_event_loop().time())
                    if remaining > 0:
                        print(f"\n[{current_time}] Monitoring... ({remaining}s remaining)")
                    continue

                except websockets.exceptions.ConnectionClosed:
                    print("WebSocket connection closed")
                    break

    except KeyboardInterrupt:
        print("\n\nMonitoring interrupted by user")

    # Show final statistics
    monitor.print_statistics()

    print("\n=== EXTENDED DEMO COMPLETED ===")
    print("\nNext steps:")
    print("- Use 'websocket_user_data_simple.py' for continuous monitoring")
    print("- Integrate with your trading bot for real-time order tracking")
    print("- Set up alerts for specific order states or PnL thresholds")

if __name__ == "__main__":
    asyncio.run(extended_demo())
