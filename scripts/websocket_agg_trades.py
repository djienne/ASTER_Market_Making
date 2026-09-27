import websocket
import json
import threading
import time
from datetime import datetime

class AsterAggTradesClient:
    def __init__(self, symbol="ethusdt"):
        self.ws = None
        self.base_url = "wss://fstream.asterdex.com"

        # Set up stream name for aggregate trades
        self.stream_name = f"{symbol.lower()}@aggTrade"
        self.url = f"{self.base_url}/ws/{self.stream_name}"
        self.symbol = symbol.upper()

        # Connection management
        self.is_connected = False
        self.reconnect_interval = 5  # seconds
        self.max_reconnect_attempts = None  # unlimited
        self.reconnect_count = 0
        self.should_reconnect = True
        self.last_ping_time = None
        self.last_pong_time = None
        self.ping_timeout = 15  # 15 seconds as per API docs
        self.ping_interval = 30  # Send ping every 30 seconds
        self.connection_check_thread = None
        self.lock = threading.Lock()

        # Trade statistics
        self.trade_count = 0
        self.volume_sum = 0.0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.last_price = None
        self.price_changes = []

    def format_trade_size(self, quantity):
        """Format trade size with appropriate units"""
        qty = float(quantity)
        if qty >= 1000:
            return f"{qty:,.1f}"
        elif qty >= 100:
            return f"{qty:.1f}"
        elif qty >= 10:
            return f"{qty:.2f}"
        else:
            return f"{qty:.3f}"

    def format_price_change(self, current_price):
        """Calculate and format price change from last trade"""
        if self.last_price is None:
            return "", ""

        change = current_price - self.last_price
        change_pct = (change / self.last_price) * 100

        if abs(change) < 0.01:
            return "", "NEUTRAL"
        elif change > 0:
            return f"+${change:.2f}", "UP"
        else:
            return f"-${abs(change):.2f}", "DOWN"

    def get_trade_direction_indicator(self, is_buyer_maker):
        """Get indicator based on trade direction"""
        if is_buyer_maker:
            return "[SELL]"  # Buyer is maker, seller is taker
        else:
            return "[BUY]"   # Seller is maker, buyer is taker

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Parse aggregate trade data
            if data.get('e') == 'aggTrade':
                self.trade_count += 1

                # Extract trade data
                symbol = data.get('s')
                agg_trade_id = data.get('a')
                price = float(data.get('p', 0))
                quantity = float(data.get('q', 0))
                first_trade_id = data.get('f')
                last_trade_id = data.get('l')
                trade_time = data.get('T')
                is_buyer_maker = data.get('m', False)

                # Update statistics
                self.volume_sum += quantity
                if is_buyer_maker:
                    self.sell_volume += quantity  # Buyer is maker, so this is a sell
                else:
                    self.buy_volume += quantity   # Seller is maker, so this is a buy

                # Calculate price change
                change_str, direction = self.format_price_change(price)

                # Format trade time
                trade_dt = datetime.fromtimestamp(trade_time / 1000) if trade_time else None

                # Determine trade side and indicator
                side = "SELL" if is_buyer_maker else "BUY"
                indicator = self.get_trade_direction_indicator(is_buyer_maker)

                # Display trade information
                print(f"\n{indicator} [{timestamp}] {symbol} Aggregate Trade #{self.trade_count}")
                print("=" * 65)
                print(f"  Trade ID: {agg_trade_id} (trades {first_trade_id}-{last_trade_id})")
                print(f"  Price: ${price:.2f} {change_str}")
                print(f"  Size: {self.format_trade_size(quantity)} ETH")
                print(f"  Notional: ${price * quantity:,.2f}")
                print(f"  Side: {side}")

                if trade_dt:
                    print(f"  Trade Time: {trade_dt.strftime('%H:%M:%S.%f')[:-3]}")

                # Show session statistics every 10 trades
                if self.trade_count % 10 == 0:
                    buy_pct = (self.buy_volume / self.volume_sum * 100) if self.volume_sum > 0 else 0
                    sell_pct = (self.sell_volume / self.volume_sum * 100) if self.volume_sum > 0 else 0

                    print("-" * 65)
                    print(f"  [STATS] Session Stats (Last {self.trade_count} trades):")
                    print(f"     Total Volume: {self.volume_sum:,.2f} ETH")
                    print(f"     Buy Volume:  {self.buy_volume:,.2f} ETH ({buy_pct:.1f}%)")
                    print(f"     Sell Volume: {self.sell_volume:,.2f} ETH ({sell_pct:.1f}%)")

                print("=" * 65)

                # Update last price for next calculation
                self.last_price = price

            else:
                print(f"[{timestamp}] Raw message: {message[:100]}...")

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Raw message: {message}")
        except Exception as e:
            print(f"Error processing message: {e}")

    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        print(f"WebSocket error: {error}")
        with self.lock:
            self.is_connected = False
        self._schedule_reconnect()

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        print(f"WebSocket connection closed. Status: {close_status_code}, Message: {close_msg}")
        with self.lock:
            self.is_connected = False
        self._schedule_reconnect()

    def on_open(self, ws):
        """Handle WebSocket open"""
        print(f"Connected to {self.url}")
        print(f"Listening for {self.symbol} aggregate trade updates...")
        print(f"[BUY] = Buy trades | [SELL] = Sell trades")
        print("=" * 65)
        with self.lock:
            self.is_connected = True
            self.reconnect_count = 0
            self.last_ping_time = time.time()
            self.last_pong_time = time.time()

        # Reset statistics on new connection
        self.trade_count = 0
        self.volume_sum = 0.0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.last_price = None

        # Start connection health monitoring
        self._start_connection_monitor()

    def on_ping(self, ws, message):
        """Handle ping from server"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Received ping from server")
        with self.lock:
            self.last_ping_time = time.time()

    def on_pong(self, ws, message):
        """Handle pong response"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Received pong from server")
        with self.lock:
            self.last_pong_time = time.time()

    def _start_connection_monitor(self):
        """Start a thread to monitor connection health"""
        if self.connection_check_thread and self.connection_check_thread.is_alive():
            return

        self.connection_check_thread = threading.Thread(target=self._monitor_connection, daemon=True)
        self.connection_check_thread.start()

    def _monitor_connection(self):
        """Monitor connection health and trigger reconnect if needed"""
        while self.should_reconnect:
            time.sleep(5)  # Check every 5 seconds

            with self.lock:
                if not self.is_connected:
                    continue

                current_time = time.time()

                # Check if we haven't received ping/pong within timeout period
                if self.last_ping_time and (current_time - self.last_ping_time) > self.ping_timeout:
                    print(f"[WARNING] No ping received for {current_time - self.last_ping_time:.1f}s (timeout: {self.ping_timeout}s)")

                if self.last_pong_time and (current_time - self.last_pong_time) > self.ping_timeout:
                    print(f"[WARNING] No pong received for {current_time - self.last_pong_time:.1f}s (timeout: {self.ping_timeout}s)")

                # Trigger reconnect if connection seems dead
                time_since_last_activity = min(
                    current_time - (self.last_ping_time or 0),
                    current_time - (self.last_pong_time or 0)
                )

                if time_since_last_activity > self.ping_timeout + 10:  # Extra 10s buffer
                    print(f"[ERROR] Connection appears dead (no activity for {time_since_last_activity:.1f}s). Forcing reconnect...")
                    self.is_connected = False
                    if self.ws:
                        try:
                            self.ws.close()
                        except:
                            pass
                    self._schedule_reconnect()

    def _schedule_reconnect(self):
        """Schedule a reconnection attempt"""
        if not self.should_reconnect:
            return

        self.reconnect_count += 1
        print(f"Scheduling reconnect attempt #{self.reconnect_count} in {self.reconnect_interval}s...")

        def reconnect_worker():
            time.sleep(self.reconnect_interval)
            if self.should_reconnect and not self.is_connected:
                print(f"Attempting to reconnect (attempt #{self.reconnect_count})...")
                self._connect()

        threading.Thread(target=reconnect_worker, daemon=True).start()

    def _connect(self):
        """Internal method to establish WebSocket connection"""
        try:
            # Enable WebSocket debugging (optional)
            # websocket.enableTrace(True)

            self.ws = websocket.WebSocketApp(
                self.url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_ping=self.on_ping,
                on_pong=self.on_pong
            )

            # Run forever with automatic ping/pong
            self.ws.run_forever(
                ping_interval=self.ping_interval,  # Send ping every 30 seconds
                ping_timeout=10    # Wait 10 seconds for pong response
            )

        except Exception as e:
            print(f"Error in connection: {e}")
            if self.should_reconnect:
                self._schedule_reconnect()

    def start(self):
        """Start the WebSocket connection with automatic reconnection"""
        print(f"Starting Aster Finance {self.symbol} Aggregate Trades WebSocket client...")
        print(f"URL: {self.url}")
        print(f"Update Speed: Real-time (as trades occur)")
        print(f"Auto-reconnect enabled (interval: {self.reconnect_interval}s)")
        print(f"Ping timeout: {self.ping_timeout}s")
        print("=" * 65)

        self.should_reconnect = True

        try:
            self._connect()
        except KeyboardInterrupt:
            print("\nReceived keyboard interrupt. Shutting down...")
            self.close()
        except Exception as e:
            print(f"Error starting WebSocket: {e}")
            self.close()

    def close(self):
        """Close the WebSocket connection and stop reconnection attempts"""
        print("Shutting down Aggregate Trades WebSocket client...")

        # Print final statistics
        if self.trade_count > 0:
            buy_pct = (self.buy_volume / self.volume_sum * 100) if self.volume_sum > 0 else 0
            sell_pct = (self.sell_volume / self.volume_sum * 100) if self.volume_sum > 0 else 0

            print("=" * 65)
            print(f"[FINAL STATS] Session Statistics:")
            print(f"   Total Trades: {self.trade_count}")
            print(f"   Total Volume: {self.volume_sum:,.2f} ETH")
            print(f"   Buy Volume:  {self.buy_volume:,.2f} ETH ({buy_pct:.1f}%)")
            print(f"   Sell Volume: {self.sell_volume:,.2f} ETH ({sell_pct:.1f}%)")
            print("=" * 65)

        with self.lock:
            self.should_reconnect = False
            self.is_connected = False

        if self.ws:
            try:
                self.ws.close()
            except:
                pass

        print("WebSocket connection closed.")

    def is_alive(self):
        """Check if the connection is alive"""
        with self.lock:
            return self.is_connected

def main():
    print("Aster Finance Aggregate Trades WebSocket Client")
    print("=" * 65)
    print("Available options:")
    print("  1. ETHUSDT Aggregate Trades")
    print("  2. BTCUSDT Aggregate Trades")
    print("  3. Custom symbol")
    print("-" * 65)

    try:
        choice = input("Enter your choice (1-3, default=1): ").strip()

        if choice == "2":
            client = AsterAggTradesClient("btcusdt")
        elif choice == "3":
            symbol = input("Enter symbol (e.g., SOLUSDT): ").strip().upper()
            if not symbol:
                symbol = "ETHUSDT"
            client = AsterAggTradesClient(symbol.lower())
        else:
            # Default: ETHUSDT
            client = AsterAggTradesClient("ethusdt")

        client.start()

    except KeyboardInterrupt:
        print("\nShutting down...")
        if 'client' in locals():
            client.close()
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()