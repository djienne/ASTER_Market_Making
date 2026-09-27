import websocket
import json
import threading
import time
from datetime import datetime

class AsterWebSocketClient:
    def __init__(self):
        self.ws = None
        self.base_url = "wss://fstream.asterdex.com"
        self.stream_name = "ethusdt@ticker"
        self.url = f"{self.base_url}/ws/{self.stream_name}"
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

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Parse ticker data
            if data.get('e') == '24hrTicker':
                print(f"\n[{timestamp}] ETHUSDT Ticker Update:")
                print(f"  Symbol: {data.get('s')}")
                print(f"  Last Price: {data.get('c')}")
                print(f"  Price Change: {data.get('p')} ({data.get('P')}%)")
                print(f"  Open Price: {data.get('o')}")
                print(f"  High Price: {data.get('h')}")
                print(f"  Low Price: {data.get('l')}")
                print(f"  Volume: {data.get('v')}")
                print(f"  Quote Volume: {data.get('q')}")
                print(f"  Weighted Avg Price: {data.get('w')}")
                print(f"  Trade Count: {data.get('n')}")
                print("-" * 50)
            else:
                print(f"[{timestamp}] Raw message: {message}")

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
        print(f"Listening for ETHUSDT ticker updates...")
        print("=" * 50)
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
        print(f"Starting Aster Finance WebSocket client...")
        print(f"URL: {self.url}")
        print(f"Auto-reconnect enabled (interval: {self.reconnect_interval}s)")
        print(f"Ping timeout: {self.ping_timeout}s")
        print("=" * 50)

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
        print("Shutting down WebSocket client...")

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
    print("Aster Finance ETHUSDT Ticker WebSocket Client")
    print("=" * 50)

    client = AsterWebSocketClient()

    try:
        client.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        client.close()
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()