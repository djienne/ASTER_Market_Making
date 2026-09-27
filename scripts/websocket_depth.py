import websocket
import json
import threading
import time
from datetime import datetime

class AsterDepthClient:
    def __init__(self, symbol="ethusdt", depth_level=5):
        self.ws = None
        self.base_url = "wss://fstream.asterdex.com"

        # Set up stream name for partial book depth
        # Available levels: 5, 10, 20 (for partial depth streams)
        if depth_level in [5, 10, 20]:
            self.stream_name = f"{symbol.lower()}@depth{depth_level}"
        else:
            # Use depth5 as default if invalid level provided
            self.stream_name = f"{symbol.lower()}@depth5"
            depth_level = 5

        self.url = f"{self.base_url}/ws/{self.stream_name}"
        self.symbol = symbol.upper()
        self.depth_level = depth_level

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

    def format_order_book_level(self, price, quantity, side="bid"):
        """Format order book level for display"""
        price_str = f"${float(price):>10.2f}"
        qty_str = f"{float(quantity):>12.3f}"

        if side == "bid":
            return f"  {price_str} | {qty_str} ETH"
        else:  # ask
            return f"  {price_str} | {qty_str} ETH"

    def calculate_spread(self, best_bid, best_ask):
        """Calculate bid-ask spread"""
        spread = float(best_ask) - float(best_bid)
        spread_pct = (spread / float(best_ask)) * 100
        return spread, spread_pct

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Check if this is a depth update (uses 'b' and 'a' keys)
            if data.get('e') == 'depthUpdate' and ('b' in data and 'a' in data):
                print(f"\n[{timestamp}] {self.symbol} Order Book (Top {self.depth_level}):")
                print("=" * 70)

                # Get bids and asks (using correct keys 'b' and 'a')
                bids = data.get('b', [])
                asks = data.get('a', [])

                if not bids or not asks:
                    print("No bid/ask data available")
                    return

                # Calculate mid price and spread
                best_bid = float(bids[0][0]) if bids else 0
                best_ask = float(asks[0][0]) if asks else 0
                mid_price = (best_bid + best_ask) / 2
                spread, spread_pct = self.calculate_spread(best_bid, best_ask)

                print(f"Mid Price: ${mid_price:.2f} | Spread: ${spread:.2f} ({spread_pct:.3f}%)")
                print("-" * 70)

                # Display asks (highest to lowest price)
                print("ASKS (Sell Orders):")
                print("      Price     |   Quantity")
                print("  " + "-" * 30)

                # Show asks in reverse order (highest price first)
                for i, ask in enumerate(asks[:self.depth_level]):
                    if len(ask) >= 2:
                        formatted = self.format_order_book_level(ask[0], ask[1], "ask")
                        print(f"{formatted}")

                print()
                print("BIDS (Buy Orders):")
                print("      Price     |   Quantity")
                print("  " + "-" * 30)

                # Show bids (highest price first - natural order)
                for i, bid in enumerate(bids[:self.depth_level]):
                    if len(bid) >= 2:
                        formatted = self.format_order_book_level(bid[0], bid[1], "bid")
                        print(f"{formatted}")

                # Show last update info if available
                if data.get('lastUpdateId'):
                    print(f"\nLast Update ID: {data.get('lastUpdateId')}")

                if data.get('E'):
                    event_time = datetime.fromtimestamp(data.get('E') / 1000)
                    print(f"Event Time: {event_time.strftime('%H:%M:%S.%f')[:-3]}")

                print("=" * 70)

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
        print(f"Listening for {self.symbol} order book updates (top {self.depth_level} levels)...")
        print("=" * 70)
        with self.lock:
            self.is_connected = True
            self.reconnect_count = 0
            self.last_ping_time = time.time()
            self.last_pong_time = time.time()

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
        print(f"Starting Aster Finance {self.symbol} Depth WebSocket client...")
        print(f"URL: {self.url}")
        print(f"Depth Level: Top {self.depth_level} levels")
        print(f"Auto-reconnect enabled (interval: {self.reconnect_interval}s)")
        print(f"Ping timeout: {self.ping_timeout}s")
        print("=" * 70)

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
        print("Shutting down Depth WebSocket client...")

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
    print("Aster Finance Order Book Depth WebSocket Client")
    print("=" * 70)
    print("Available options:")
    print("  1. ETHUSDT Order Book (Top 5 levels)")
    print("  2. ETHUSDT Order Book (Top 10 levels)")
    print("  3. ETHUSDT Order Book (Top 20 levels)")
    print("  4. BTCUSDT Order Book (Top 5 levels)")
    print("  5. Custom symbol")
    print("-" * 70)

    try:
        choice = input("Enter your choice (1-5, default=1): ").strip()

        if choice == "2":
            client = AsterDepthClient("ethusdt", 10)
        elif choice == "3":
            client = AsterDepthClient("ethusdt", 20)
        elif choice == "4":
            client = AsterDepthClient("btcusdt", 5)
        elif choice == "5":
            symbol = input("Enter symbol (e.g., BTCUSDT): ").strip().upper()
            level = input("Enter depth level (5/10/20, default=5): ").strip()
            try:
                level = int(level)
                if level not in [5, 10, 20]:
                    level = 5
            except ValueError:
                level = 5
            client = AsterDepthClient(symbol.lower(), level)
        else:
            # Default: ETHUSDT with top 5 levels
            client = AsterDepthClient("ethusdt", 5)

        client.start()

    except KeyboardInterrupt:
        print("\nShutting down...")
        if 'client' in locals():
            client.close()
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()