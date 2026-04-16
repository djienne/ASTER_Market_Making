import websocket
import json
import threading
import time
import os
import csv
import argparse
import signal
import requests
from datetime import datetime
from collections import deque
import pandas as pd

def _default_markets():
    raw_symbols = os.getenv("SYMBOLS") or os.getenv("SYMBOL") or "BTCUSDT"
    return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]


LIST_MARKETS = _default_markets()

class WebSocketDataCollector:
    ORDERBOOK_ARCHIVE_INTERVAL_MS = 60 * 60 * 1000

    def __init__(self, symbols, flush_interval=5, order_book_levels=10):
        self.symbols = [symbol.upper() for symbol in symbols]
        self.flush_interval = flush_interval
        self.order_book_levels = order_book_levels
        self.base_url = "wss://fstream.asterdex.com"
        self.api_base_url = "https://fapi.asterdex.com"

        # WebSocket connections
        self.depth_ws = None
        self.trades_ws = None

        # Connection management
        self.is_connected = False
        self.should_reconnect = True
        self.reconnect_interval = 5
        self.ping_timeout = 15
        self.ping_interval = 30
        self.ORDERBOOK_BUFFER_SIZE_LIMIT = 50000

        # Data buffers
        self.prices_buffer = {}  # symbol -> deque of price records (bid/ask/mid)
        self.orderbook_buffer = {}  # symbol -> deque of partial order book snapshots
        self.trades_buffer = {}  # symbol -> deque of trade records
        self.seen_trade_ids = {}  # symbol -> set of seen trade IDs
        self.orderbook_staging_recovered = {}  # symbol -> whether `_latest.parquet` has been merged after startup

        # Thread management
        self.flush_thread = None
        self.lock = threading.Lock()

        # Initialize buffers and load existing trade IDs
        for symbol in self.symbols:
            self.prices_buffer[symbol] = deque()
            self.orderbook_buffer[symbol] = deque()
            self.trades_buffer[symbol] = deque()
            self.seen_trade_ids[symbol] = self.load_seen_trade_ids(symbol)
            self.orderbook_staging_recovered[symbol] = False

        print(f"Initialized WebSocket Data Collector for: {', '.join(self.symbols)}")

    def create_data_directory(self):
        """Creates the ASTER_data directory if it doesn't exist."""
        if not os.path.exists('ASTER_data'):
            os.makedirs('ASTER_data')

    def load_seen_trade_ids(self, symbol):
        """Loads the last 1000 raw trade IDs from the CSV file to prevent duplicates."""
        seen_ids = set()
        file_path = os.path.join('ASTER_data', f'trades_{symbol}.csv')
        if not os.path.isfile(file_path):
            return seen_ids

        try:
            with open(file_path, 'r', newline='') as f:
                # Use deque to efficiently get the last N lines without reading the whole file
                q = deque(f, 1000)
            
            # Now process the lines from the deque
            reader = csv.reader(q)
            for row in reader:
                # Skip header if it happens to be in the last 1000 lines
                if row and row[0] != 'id':
                    try:
                        seen_ids.add(int(row[0]))
                    except (ValueError, IndexError):
                        # Ignore lines that don't have a valid ID in the first column
                        pass
        except Exception as e:
            print(f"Warning: Error loading trade IDs for {symbol}: {e}")

        return seen_ids

    def get_initial_prices_api(self, symbol):
        """Get initial price data via API to establish baseline."""
        endpoint = f"{self.api_base_url}/fapi/v1/ticker/bookTicker"
        params = {'symbol': symbol}

        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            bid = float(data['bidPrice'])
            ask = float(data['askPrice'])
            mid = (bid + ask) / 2
            # Use transaction time from API response (in ms)
            timestamp = int(data['time'])

            return {
                'timestamp': timestamp,
                'bid': bid,
                'ask': ask,
                'mid': mid
            }
        except Exception as e:
            print(f"Error getting initial prices for {symbol}: {e}")
            return None

    def get_initial_orderbook_api(self, symbol):
        """Get an initial partial order book snapshot via REST to establish baseline."""
        endpoint = f"{self.api_base_url}/fapi/v1/depth"
        params = {'symbol': symbol, 'limit': self.order_book_levels}

        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Prefer exchange timestamps when present, otherwise fall back to current wall time.
            timestamp = (
                self._coerce_timestamp_ms(data.get('T'))
                or self._coerce_timestamp_ms(data.get('E'))
                or int(time.time() * 1000)
            )
            bids = [[float(bid[0]), float(bid[1])] for bid in data.get('bids', [])[:self.order_book_levels]]
            asks = [[float(ask[0]), float(ask[1])] for ask in data.get('asks', [])[:self.order_book_levels]]

            return {
                'timestamp': timestamp,
                'bids': bids,
                'asks': asks,
                'lastUpdateId': data.get('lastUpdateId'),
                'bookMode': 'partial',
                'levels': self.order_book_levels,
            }
        except Exception as e:
            print(f"Error getting initial order book for {symbol}: {e}")
            return None

    def get_initial_trades_api(self, symbol):
        """Get initial trade data via API to establish baseline."""
        endpoint = f"{self.api_base_url}/fapi/v1/trades"
        params = {'symbol': symbol, 'limit': 100}

        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            trades = response.json()

            new_trades = []
            for trade in trades:
                if trade['id'] not in self.seen_trade_ids[symbol]:
                    trade_data = {
                        'id': trade['id'],
                        'timestamp': trade['time'],
                        'side': "sell" if trade['isBuyerMaker'] else "buy",
                        'price': float(trade['price']),
                        'quantity': float(trade['qty'])
                    }
                    new_trades.append(trade_data)
                    self.seen_trade_ids[symbol].add(trade['id'])

            return new_trades

        except Exception as e:
            print(f"Error getting initial trades for {symbol}: {e}")
            return []

    def on_depth_message(self, ws, message):
        """Handle partial depth WebSocket messages."""
        try:
            data = json.loads(message)

            if data.get('e') == 'depthUpdate' and ('b' in data and 'a' in data):
                symbol = data.get('s')
                if symbol not in self.symbols:
                    return

                bids = data.get('b', [])
                asks = data.get('a', [])

                if bids and asks:
                    # Use event time from WebSocket message (in ms)
                    timestamp = self._coerce_timestamp_ms(data.get('E'))
                    if timestamp is None:
                        return

                    # Process bids and asks up to the specified levels
                    processed_bids = [[float(bid[0]), float(bid[1])] for bid in bids[:self.order_book_levels] if len(bid) >= 2]
                    processed_asks = [[float(ask[0]), float(ask[1])] for ask in asks[:self.order_book_levels] if len(ask) >= 2]

                    # Get best bid and ask for price record
                    if processed_bids and processed_asks:
                        best_bid = processed_bids[0][0]
                        best_ask = processed_asks[0][0]
                        mid = (best_bid + best_ask) / 2

                        # Store simple price data (for compatibility)
                        price_record = {
                            'timestamp': timestamp,
                            'bid': best_bid,
                            'ask': best_ask,
                            'mid': mid
                        }

                        # Store partial order book data from the top N levels only
                        orderbook_record = {
                            'timestamp': timestamp,
                            'bids': processed_bids,
                            'asks': processed_asks,
                            'lastUpdateId': data.get('u'),  # final update ID for this partial snapshot
                            'bookMode': 'partial',
                            'levels': self.order_book_levels,
                        }

                        with self.lock:
                            self.prices_buffer[symbol].append(price_record)
                            self.orderbook_buffer[symbol].append(orderbook_record)

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"Error processing depth message: {e}")

    def on_trades_message(self, ws, message):
        """Handle raw trade or aggregate trade WebSocket messages."""
        try:
            data = json.loads(message)

            event_type = data.get('e')
            if event_type in {'trade', 'aggTrade'}:
                symbol = data.get('s')
                if symbol not in self.symbols:
                    return

                if event_type == 'trade':
                    trade_id = data.get('t')
                else:
                    trade_id = data.get('a')

                price = float(data.get('p', 0))
                quantity = float(data.get('q', 0))
                trade_time = data.get('T')
                is_buyer_maker = data.get('m', False)

                if trade_id not in self.seen_trade_ids[symbol]:
                    side = "sell" if is_buyer_maker else "buy"

                    trade_record = {
                        'id': trade_id,
                        'timestamp': trade_time,
                        'side': side,
                        'price': price,
                        'quantity': quantity
                    }

                    with self.lock:
                        self.trades_buffer[symbol].append(trade_record)
                        self.seen_trade_ids[symbol].add(trade_id)

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"Error processing trades message: {e}")

    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        print(f"WebSocket error: {error}")
        self.is_connected = False

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        print(f"WebSocket connection closed. Status: {close_status_code}")
        self.is_connected = False

    def on_depth_open(self, ws):
        """Handle depth WebSocket open."""
        print("Depth WebSocket connected")

    def on_trades_open(self, ws):
        """Handle trades WebSocket open."""
        print("Trades WebSocket connected")
        self.is_connected = True

    def create_combined_stream_url(self):
        """Create combined stream URL for all symbols."""
        # Use appropriate depth stream based on order book levels
        if self.order_book_levels <= 5:
            depth_suffix = "depth5"
        elif self.order_book_levels <= 10:
            depth_suffix = "depth10"
        elif self.order_book_levels <= 20:
            depth_suffix = "depth20"
        else:
            depth_suffix = "depth20"  # Max available via WebSocket

        depth_streams = [f"{symbol.lower()}@{depth_suffix}" for symbol in self.symbols]
        trade_streams = [f"{symbol.lower()}@trade" for symbol in self.symbols]
        all_streams = depth_streams + trade_streams

        stream_names = "/".join(all_streams)
        url = f"{self.base_url}/stream?streams={stream_names}"
        return url

    def on_combined_message(self, ws, message):
        """Handle combined stream messages."""
        try:
            data = json.loads(message)
            stream_name = data.get('stream', '')
            stream_data = data.get('data', {})

            if '@depth' in stream_name:
                # Process as depth message
                self.on_depth_message(None, json.dumps(stream_data))
            elif '@trade' in stream_name or '@aggTrade' in stream_name:
                # Process as trade message
                self.on_trades_message(None, json.dumps(stream_data))

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"Error processing combined message: {e}")

    def on_combined_open(self, ws):
        """Handle combined WebSocket open."""
        print("Combined WebSocket connected for all symbols")
        self.is_connected = True

    def start_websockets(self):
        """Start WebSocket connections."""
        websocket.enableTrace(False)

        # Use combined stream for all symbols
        combined_url = self.create_combined_stream_url()
        print(f"Connecting to: {combined_url}")

        self.combined_ws = websocket.WebSocketApp(
            combined_url,
            on_open=self.on_combined_open,
            on_message=self.on_combined_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        # Start WebSocket in a separate thread
        def run_websocket():
            while self.should_reconnect:
                try:
                    self.combined_ws.run_forever(
                        ping_interval=self.ping_interval,
                        ping_timeout=10
                    )

                    if self.should_reconnect:
                        print(f"WebSocket disconnected. Reconnecting in {self.reconnect_interval}s...")
                        time.sleep(self.reconnect_interval)

                except Exception as e:
                    print(f"WebSocket error: {e}")
                    if self.should_reconnect:
                        time.sleep(self.reconnect_interval)

        ws_thread = threading.Thread(target=run_websocket, daemon=True)
        ws_thread.start()

    def flush_buffers(self, force=False):
        """Flush all buffers to files. The order book will be flushed to a staging file."""
        with self.lock:
            for symbol in self.symbols:
                if self.prices_buffer[symbol]:
                    self.flush_prices_buffer(symbol)

                if self.orderbook_buffer[symbol]:
                    self.flush_orderbook_buffer_to_parquet(symbol, force_archive=force)

                if self.trades_buffer[symbol]:
                    self.flush_trades_buffer(symbol)

    def flush_prices_buffer(self, symbol):
        """Flush price buffer for a specific symbol."""
        file_path = os.path.join('ASTER_data', f'prices_{symbol}.csv')
        file_exists = os.path.isfile(file_path)

        try:
            with open(file_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['unix_timestamp_ms', 'bid', 'ask', 'mid'])

                count = 0
                while self.prices_buffer[symbol]:
                    record = self.prices_buffer[symbol].popleft()
                    writer.writerow([
                        record['timestamp'],
                        record['bid'],
                        record['ask'],
                        f"{record['mid']:.6f}"
                    ])
                    count += 1

                if count > 0:
                    print(f"Flushed {count} price records for {symbol}")

        except Exception as e:
            print(f"Error flushing prices for {symbol}: {e}")

    def _coerce_timestamp_ms(self, value):
        """Convert exchange timestamps to millisecond integers when possible."""
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _normalize_orderbook_frame(self, df):
        """Sort and deduplicate order book snapshots before writing them."""
        if df.empty:
            return df

        df = df.copy()
        if 'timestamp' not in df.columns:
            return df.iloc[0:0]

        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        invalid_rows = int(df['timestamp'].isna().sum())
        if invalid_rows:
            print(f"Warning: Dropping {invalid_rows} order book records with invalid timestamps")
            df = df[df['timestamp'].notna()].copy()

        if df.empty:
            return df

        df['timestamp'] = df['timestamp'].astype('int64')
        if 'lastUpdateId' not in df.columns:
            df['lastUpdateId'] = pd.NA

        df.sort_values(by='timestamp', inplace=True)
        df.drop_duplicates(subset=['timestamp', 'lastUpdateId'], keep='first', inplace=True)
        return df

    def _orderbook_hour_bucket_ms(self, timestamp_ms):
        """Return the UTC hour bucket for a millisecond timestamp."""
        return int(timestamp_ms // self.ORDERBOOK_ARCHIVE_INTERVAL_MS * self.ORDERBOOK_ARCHIVE_INTERVAL_MS)

    def _orderbook_archive_path(self, output_dir, hour_bucket_ms):
        """Build a deterministic parquet filename for a UTC hour bucket."""
        hour_label = datetime.utcfromtimestamp(hour_bucket_ms / 1000).strftime('%Y%m%dT%H0000Z')
        return os.path.join(output_dir, f'{hour_label}.parquet')

    def _write_orderbook_archive(self, archive_file_path, df):
        """Write or merge an archived hourly order book parquet file."""
        existing_df = self._read_orderbook_parquet(archive_file_path)
        if not existing_df.empty:
            df = pd.concat([existing_df, df], ignore_index=True)

        df = self._normalize_orderbook_frame(df)
        df.to_parquet(archive_file_path, engine='pyarrow', compression='ZSTD')

    def _read_orderbook_parquet(self, file_path):
        """Read an order book parquet file if it exists."""
        if not os.path.isfile(file_path):
            return pd.DataFrame()

        try:
            return pd.read_parquet(file_path, engine='pyarrow')
        except Exception as e:
            print(f"Warning: Could not read order book parquet {file_path}: {e}")
            return pd.DataFrame()

    def flush_orderbook_buffer_to_parquet(self, symbol, force_archive=False):
        """
        Flush partial order book data to parquet files.
        Completed UTC hours are archived into one parquet per hour, while the
        current in-progress hour is mirrored to `_latest.parquet`.
        """
        if not self.orderbook_buffer[symbol]:
            return

        output_dir = os.path.join('ASTER_data', 'orderbook_parquet', symbol)
        os.makedirs(output_dir, exist_ok=True)
        staging_file_path = os.path.join(output_dir, '_latest.parquet')

        df = pd.DataFrame(list(self.orderbook_buffer[symbol]))
        
        try:
            if not self.orderbook_staging_recovered[symbol]:
                existing_latest_df = self._read_orderbook_parquet(staging_file_path)
                if not existing_latest_df.empty:
                    df = pd.concat([existing_latest_df, df], ignore_index=True)
                self.orderbook_staging_recovered[symbol] = True

            df = self._normalize_orderbook_frame(df)
            if df.empty:
                self.orderbook_buffer[symbol].clear()
                if os.path.isfile(staging_file_path):
                    os.remove(staging_file_path)
                print(f"No valid order book records remain for {symbol}")
                return

            df['hour_bucket_ms'] = df['timestamp'].map(self._orderbook_hour_bucket_ms)
            current_hour_bucket = int(df['hour_bucket_ms'].max())
            archive_hour_buckets = {
                int(bucket_ms)
                for bucket_ms in df['hour_bucket_ms'].unique()
                if force_archive or int(bucket_ms) != current_hour_bucket
            }

            if len(df) >= self.ORDERBOOK_BUFFER_SIZE_LIMIT:
                archive_hour_buckets.add(current_hour_bucket)

            for hour_bucket_ms in sorted(archive_hour_buckets):
                archive_df = df[df['hour_bucket_ms'] == hour_bucket_ms].drop(columns=['hour_bucket_ms'])
                if archive_df.empty:
                    continue

                archive_file_path = self._orderbook_archive_path(output_dir, hour_bucket_ms)
                self._write_orderbook_archive(archive_file_path, archive_df)
                print(
                    f"Archived {len(archive_df)} partial order book records for {symbol} "
                    f"to {archive_file_path}"
                )

            latest_df = df[~df['hour_bucket_ms'].isin(archive_hour_buckets)].drop(columns=['hour_bucket_ms'])

            if latest_df.empty:
                if os.path.isfile(staging_file_path):
                    os.remove(staging_file_path)
                self.orderbook_buffer[symbol].clear()
                print(f"No in-progress hourly order book staging file remains for {symbol}")
                return

            latest_df = self._normalize_orderbook_frame(latest_df)
            latest_df.to_parquet(staging_file_path, engine='pyarrow', compression='ZSTD')
            self.orderbook_buffer[symbol] = deque(latest_df.to_dict('records'))
            print(f"Flushed {len(latest_df)} partial order book records for {symbol} to {staging_file_path}")
            print(f"Flushed {len(latest_df)} partial order book records to staging file for {symbol}")

        except Exception as e:
            print(f"Error flushing partial order book for {symbol} to Parquet: {e}")

    def flush_trades_buffer(self, symbol):
        """Flush trades buffer for a specific symbol."""
        file_path = os.path.join('ASTER_data', f'trades_{symbol}.csv')
        file_exists = os.path.isfile(file_path)

        try:
            with open(file_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['id', 'unix_timestamp_ms', 'side', 'price', 'quantity'])

                count = 0
                while self.trades_buffer[symbol]:
                    record = self.trades_buffer[symbol].popleft()
                    writer.writerow([
                        record['id'],
                        record['timestamp'],
                        record['side'],
                        f"{record['price']:.6f}",
                        f"{record['quantity']:.6f}"
                    ])
                    count += 1

                if count > 0:
                    print(f"Flushed {count} trade records for {symbol}")

        except Exception as e:
            print(f"Error flushing trades for {symbol}: {e}")

    def start_flush_thread(self):
        """Start the buffer flush thread."""
        def flush_worker():
            while self.should_reconnect:
                time.sleep(self.flush_interval)
                try:
                    self.flush_buffers()
                except Exception as e:
                    print(f"Error flushing buffers: {e}")

        self.flush_thread = threading.Thread(target=flush_worker, daemon=True)
        self.flush_thread.start()
        print(f"Started buffer flush thread (interval: {self.flush_interval}s)")

    def collect_initial_data(self):
        """Collect initial data via API calls."""
        print("Collecting initial data via API...")

        for symbol in self.symbols:
            print(f"Getting initial data for {symbol}...")

            # Get initial price data
            initial_price = self.get_initial_prices_api(symbol)
            if initial_price:
                with self.lock:
                    self.prices_buffer[symbol].append(initial_price)
                print(f"  Initial price: ${initial_price['mid']:.2f}")

            # Get initial partial order book data
            initial_orderbook = self.get_initial_orderbook_api(symbol)
            if initial_orderbook:
                with self.lock:
                    self.orderbook_buffer[symbol].append(initial_orderbook)
                print(f"  Initial partial order book: {len(initial_orderbook['bids'])} bids, {len(initial_orderbook['asks'])} asks")

            # Get initial trade data
            initial_trades = self.get_initial_trades_api(symbol)
            if initial_trades:
                with self.lock:
                    self.trades_buffer[symbol].extend(initial_trades)
                print(f"  Loaded {len(initial_trades)} initial trades")

            print(f"  Existing trade IDs loaded: {len(self.seen_trade_ids[symbol])}")

    def start(self):
        """Start the data collector."""
        self.create_data_directory()

        # Collect initial data via API
        self.collect_initial_data()

        # Start buffer flush thread
        self.start_flush_thread()

        # Start WebSocket connections
        self.start_websockets()

        # Wait for connection
        max_wait = 10
        waited = 0
        while not self.is_connected and waited < max_wait:
            time.sleep(1)
            waited += 1

        if self.is_connected:
            print("WebSocket data collector started successfully!")
        else:
            print("Warning: WebSocket connection not established within timeout")

    def stop(self):
        """Stop the data collector."""
        print("Stopping data collector...")
        self.should_reconnect = False

        if hasattr(self, 'combined_ws') and self.combined_ws:
            self.combined_ws.close()

        # Final flush to save any remaining data
        self.flush_buffers(force=True)
        print("Data collector stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='WebSocket-based data collector for crypto market data.')
    parser.add_argument('symbols', type=str, nargs='*', default=LIST_MARKETS,
                        help='The trading symbols to collect data for (e.g., BTCUSDT ETHUSDT).')
    parser.add_argument('--flush-interval', type=int, default=5,
                        help='The interval in seconds to flush buffers to CSV files. Defaults to 5.')
    parser.add_argument('--order-book-levels', type=int, default=10,
                        help='Number of order book levels to collect (5, 10, or 20). Defaults to 10.')
    args = parser.parse_args()

    collector = WebSocketDataCollector(args.symbols, args.flush_interval, args.order_book_levels)
    shutdown_requested = threading.Event()

    def handle_shutdown(signum, frame):
        signal_name = signal.Signals(signum).name
        print(f"\nReceived {signal_name}. Shutting down...")
        shutdown_requested.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        collector.start()

        # Keep running
        while not shutdown_requested.is_set():
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
        shutdown_requested.set()
    finally:
        if collector.should_reconnect:
            collector.stop()
