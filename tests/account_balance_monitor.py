#!/usr/bin/env python3
"""
Account Balance Monitor for Aster Finance
Real-time monitoring of account balances, positions, and P&L using WebSocket streams
"""

import asyncio
import json
import os
import websockets
from datetime import datetime
from dotenv import load_dotenv
from api_client import ApiClient

load_dotenv()

class AccountBalanceMonitor:
    def __init__(self):
        self.balance_updates = 0
        self.position_updates = 0
        self.pnl_changes = 0
        self.start_time = datetime.now()

        # Track current balances and positions
        self.current_balances = {}
        self.current_positions = {}
        self.total_unrealized_pnl = 0.0
        self.total_wallet_balance = 0.0

    def format_timestamp(self, timestamp_ms):
        """Convert timestamp to readable format."""
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S.%f')[:-3]

    def format_currency(self, amount, currency="USDT"):
        """Format currency with proper decimal places and sign."""
        try:
            value = float(amount)
            if value == 0:
                return f"0.00 {currency}"
            elif abs(value) >= 1000:
                return f"{value:,.2f} {currency}"
            else:
                return f"{value:.4f} {currency}"
        except:
            return f"{amount} {currency}"

    def format_percentage(self, value):
        """Format percentage with proper sign."""
        try:
            pct = float(value)
            sign = "+" if pct > 0 else ""
            return f"{sign}{pct:.2f}%"
        except:
            return f"{value}%"

    def calculate_portfolio_summary(self):
        """Calculate total portfolio value and statistics."""
        total_balance = 0.0
        total_unrealized = 0.0

        for asset, balance in self.current_balances.items():
            if asset in ['USDT', 'BUSD', 'USDC']:  # Stable coins
                total_balance += float(balance.get('wallet_balance', 0))

        for symbol, position in self.current_positions.items():
            if float(position.get('position_amt', 0)) != 0:
                total_unrealized += float(position.get('unrealized_pnl', 0))

        self.total_wallet_balance = total_balance
        self.total_unrealized_pnl = total_unrealized

        return {
            'total_balance': total_balance,
            'total_unrealized_pnl': total_unrealized,
            'total_equity': total_balance + total_unrealized,
            'position_count': len([p for p in self.current_positions.values()
                                 if float(p.get('position_amt', 0)) != 0])
        }

    def print_balance_update(self, balance_data, reason):
        """Print detailed balance update information."""
        print("\n" + "="*90)
        print(f"BALANCE UPDATE #{self.balance_updates + 1} - {reason}")
        print("="*90)
        print(f"Time: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")

        # Process balance changes
        balances = balance_data.get('B', [])
        if balances:
            print("\nBalance Changes:")
            for balance in balances:
                asset = balance.get('a', 'N/A')
                wallet_balance = balance.get('wb', '0')
                cross_wallet_balance = balance.get('cw', '0')
                balance_change = balance.get('bc', '0')

                # Update current balances
                self.current_balances[asset] = {
                    'wallet_balance': wallet_balance,
                    'cross_wallet_balance': cross_wallet_balance,
                    'last_change': balance_change
                }

                change_value = float(balance_change)
                if change_value != 0:
                    change_indicator = "[+]" if change_value > 0 else "[-]"
                    change_color = "PROFIT" if change_value > 0 else "LOSS"

                    print(f"  {change_indicator} {asset}:")
                    print(f"    Wallet Balance: {self.format_currency(wallet_balance, asset)}")
                    print(f"    Cross Wallet: {self.format_currency(cross_wallet_balance, asset)}")
                    print(f"    Change: {self.format_currency(balance_change, asset)} [{change_color}]")

        # Process position changes
        positions = balance_data.get('P', [])
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

                # Update current positions
                self.current_positions[symbol] = {
                    'position_amt': position_amt,
                    'entry_price': entry_price,
                    'unrealized_pnl': unrealized_pnl,
                    'margin_type': margin_type,
                    'position_side': position_side,
                    'isolated_wallet': isolated_wallet,
                    'accumulated_realized': accumulated_realized
                }

                pos_value = float(position_amt)
                if pos_value != 0:
                    pnl_value = float(unrealized_pnl)
                    pnl_indicator = "[+]" if pnl_value >= 0 else "[-]"
                    side_indicator = "[LONG]" if pos_value > 0 else "[SHORT]"

                    print(f"  {side_indicator} {symbol} ({position_side}):")
                    print(f"    Position Size: {position_amt}")
                    print(f"    Entry Price: ${entry_price}")
                    print(f"    {pnl_indicator} Unrealized PnL: {self.format_currency(unrealized_pnl)}")
                    print(f"    Margin: {margin_type}")

                    if float(isolated_wallet) > 0:
                        print(f"    Isolated Wallet: {self.format_currency(isolated_wallet)}")

                    if float(accumulated_realized) != 0:
                        print(f"    Realized PnL: {self.format_currency(accumulated_realized)}")

        # Calculate and show portfolio summary
        summary = self.calculate_portfolio_summary()

        print(f"\n{'='*40} PORTFOLIO SUMMARY {'='*40}")
        print(f"Total Wallet Balance: {self.format_currency(summary['total_balance'])}")
        print(f"Total Unrealized PnL: {self.format_currency(summary['total_unrealized_pnl'])}")
        print(f"Total Equity: {self.format_currency(summary['total_equity'])}")
        print(f"Active Positions: {summary['position_count']}")
        print("="*90)

        self.balance_updates += 1

    def print_current_portfolio(self):
        """Print current portfolio status."""
        print("\n" + "="*30)
        print("CURRENT PORTFOLIO STATUS")
        print("="*30)

        if self.current_balances:
            print("\nCurrent Balances:")
            for asset, balance_info in self.current_balances.items():
                wallet_bal = balance_info.get('wallet_balance', '0')
                if float(wallet_bal) > 0.01:  # Only show significant balances
                    print(f"  {asset}: {self.format_currency(wallet_bal, asset)}")

        if self.current_positions:
            print("\nActive Positions:")
            total_unrealized = 0.0
            for symbol, position_info in self.current_positions.items():
                pos_amt = position_info.get('position_amt', '0')
                if float(pos_amt) != 0:
                    entry_price = position_info.get('entry_price', '0')
                    unrealized_pnl = position_info.get('unrealized_pnl', '0')
                    side = position_info.get('position_side', 'N/A')

                    pnl_val = float(unrealized_pnl)
                    total_unrealized += pnl_val

                    side_symbol = "[LONG]" if float(pos_amt) > 0 else "[SHORT]"
                    pnl_symbol = "[+]" if pnl_val >= 0 else "[-]"

                    print(f"  {symbol} - {side_symbol}")
                    print(f"    Size: {pos_amt} @ ${entry_price}")
                    print(f"    {pnl_symbol} PnL: {self.format_currency(unrealized_pnl)}")

            print(f"\nTotal Unrealized PnL: {self.format_currency(total_unrealized)}")

        summary = self.calculate_portfolio_summary()
        print(f"\nTotal Portfolio Value: {self.format_currency(summary['total_equity'])}")
        print("="*30)

    def print_statistics(self):
        """Print session statistics."""
        duration = datetime.now() - self.start_time
        duration_seconds = duration.total_seconds()

        print("\n" + "="*40)
        print("MONITORING SESSION STATISTICS")
        print("="*40)
        print(f"Session Duration: {int(duration_seconds // 60)}m {int(duration_seconds % 60)}s")
        print(f"Balance Updates: {self.balance_updates}")
        print(f"Position Updates: {self.position_updates}")
        print(f"Total Events: {self.balance_updates + self.position_updates}")

        if duration_seconds > 0:
            events_per_minute = (self.balance_updates + self.position_updates) * 60 / duration_seconds
            print(f"Events per minute: {events_per_minute:.1f}")

        print("="*40)

async def monitor_account_balance():
    """Main account balance monitoring function."""
    print("ASTER FINANCE ACCOUNT BALANCE MONITOR")
    print("="*60)

    # Get API keys
    api_user = os.getenv('API_USER')
    api_signer = os.getenv('API_SIGNER')
    api_private_key = os.getenv('API_PRIVATE_KEY')

    if not all([apiv1_public, apiv1_private]):
        print("ERROR: Missing API_USER, API_SIGNER, or API_PRIVATE_KEY")
        return

    monitor = AccountBalanceMonitor()

    # Step 1: Get current account state via REST API
    print("\n1. Fetching current account state...")
    client = ApiClient(api_user, api_signer, api_private_key)

    async with client:
        try:
            # Get current balances
            account_info = await client.signed_request("GET", "/fapi/v3/account", {})
            balances = account_info.get('assets', [])

            print("Current Account Balances:")
            for balance in balances:
                asset = balance.get('asset', 'N/A')
                wallet_balance = balance.get('walletBalance', '0')
                if float(wallet_balance) > 0.01:
                    monitor.current_balances[asset] = {
                        'wallet_balance': wallet_balance,
                        'cross_wallet_balance': balance.get('crossWalletBalance', '0'),
                        'last_change': '0'
                    }
                    print(f"  {asset}: {monitor.format_currency(wallet_balance, asset)}")

            # Get current positions
            positions = account_info.get('positions', [])
            print("\nCurrent Positions:")
            for position in positions:
                symbol = position.get('symbol', 'N/A')
                position_amt = position.get('positionAmt', '0')
                if float(position_amt) != 0:
                    monitor.current_positions[symbol] = {
                        'position_amt': position_amt,
                        'entry_price': position.get('entryPrice', '0'),
                        'unrealized_pnl': position.get('unrealizedPnl', '0'),
                        'margin_type': position.get('marginType', 'N/A'),
                        'position_side': position.get('positionSide', 'N/A'),
                        'isolated_wallet': position.get('isolatedWallet', '0'),
                        'accumulated_realized': '0'
                    }

                    side = "LONG" if float(position_amt) > 0 else "SHORT"
                    pnl = position.get('unrealizedPnl', '0')
                    print(f"  {symbol} ({side}): {position_amt} (PnL: {monitor.format_currency(pnl)})")

        except Exception as e:
            print(f"WARNING: Could not fetch initial account state: {e}")
            print("Proceeding with real-time monitoring...")

    # Step 2: Get listen key for WebSocket
    print("\n2. Getting WebSocket listen key...")
    async with client:
        response = await client.signed_request(
            "POST", "/fapi/v3/listenKey", {}
        )
        listen_key = response['listenKey']
        print(f"   Listen key obtained: {listen_key[:20]}...")

    # Step 3: Start real-time monitoring
    print("\n3. Starting real-time balance monitoring...")
    ws_url = f"wss://fstream.asterdex.com/ws/{listen_key}"

    print("\nMonitoring Duration: 3 minutes")
    print("Monitoring for:")
    print("   - Real-time balance changes")
    print("   - Position updates and P&L changes")
    print("   - Trade settlements and fees")
    print("   - Funding fee payments")
    print("   - Margin changes")
    print("\nPress Ctrl+C to stop early")

    try:
        async with websockets.connect(ws_url) as ws:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Connected! Monitoring started...")

            # Show initial portfolio
            monitor.print_current_portfolio()

            # Monitor for 3 minutes
            end_time = asyncio.get_event_loop().time() + 180  # 3 minutes

            while asyncio.get_event_loop().time() < end_time:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=10)
                    data = json.loads(message)
                    event_type = data.get('e', 'unknown')

                    if event_type == 'ACCOUNT_UPDATE':
                        account_data = data.get('a', {})
                        reason = account_data.get('m', 'UNKNOWN')
                        monitor.print_balance_update(account_data, reason)

                    elif event_type == 'ORDER_TRADE_UPDATE':
                        # We can also track order-related balance changes
                        order_data = data.get('o', {})
                        if order_data.get('x') == 'TRADE':
                            realized_pnl = order_data.get('rp', '0')
                            commission = order_data.get('n', '0')
                            if float(realized_pnl) != 0 or float(commission) != 0:
                                print(f"\nTrade Settlement:")
                                print(f"   Realized PnL: {monitor.format_currency(realized_pnl)}")
                                if float(commission) > 0:
                                    print(f"   Commission: {monitor.format_currency(commission, order_data.get('N', 'USDT'))}")

                    elif event_type == 'MARGIN_CALL':
                        print(f"\n*** MARGIN CALL ALERT ***")
                        cross_wallet = data.get('cw', '0')
                        print(f"Cross Wallet Balance: {monitor.format_currency(cross_wallet)}")

                except asyncio.TimeoutError:
                    current_time = datetime.now().strftime('%H:%M:%S')
                    remaining = int(end_time - asyncio.get_event_loop().time())
                    if remaining > 0:
                        print(f"\n[{current_time}] Monitoring... ({remaining}s remaining)")

                        # Show portfolio summary every 30 seconds
                        if remaining % 30 == 0:
                            monitor.print_current_portfolio()
                    continue

                except websockets.exceptions.ConnectionClosed:
                    print("WebSocket connection closed")
                    break

    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")

    # Final summary
    monitor.print_current_portfolio()
    monitor.print_statistics()

    print("\nACCOUNT BALANCE MONITORING COMPLETED")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(monitor_account_balance())
