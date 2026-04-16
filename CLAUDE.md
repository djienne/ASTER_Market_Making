# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ASTER Market Making is a sophisticated Python-based market making bot for the Aster Finance DEX platform. It implements the Avellaneda-Stoikov model for optimal spread calculation and uses real-time WebSocket data feeds for market making operations.

## Quick Start Commands

### Installation
```bash
pip install -r requirements.txt
```

### Data Collection (Required First Step)
```bash
# Run for at least 1 hour before using other components
python data_collector.py
```

### Parameter Calculation
```bash
# Calculate Avellaneda-Stoikov parameters (requires collected data)
python calculate_avellaneda_parameters.py ETH --minutes 5

# Calculate SuperTrend directional bias
python find_trend.py --symbol ETHUSDT --interval 5m
```

### Running the Market Maker
```bash
# Requires .env file with API credentials
python market_maker.py --symbol ETHUSDT
```

### Monitoring
```bash
# View real-time account dashboard
python terminal_dashboard.py

# Check trading volume
python get_my_trading_volume.py --symbol ETHUSDT --days 7
```

### Docker Deployment
```bash
# Build and start all services
docker-compose build
docker-compose up -d

# View logs for specific service
docker-compose logs -f market-maker
docker-compose logs -f data-collector

# Stop all services
docker-compose down
```

## Architecture

### Core Components

**Data Collection Pipeline**
- `data_collector.py`: WebSocket-based real-time market data collection
  - Collects orderbook depth (bid/ask levels)
  - Captures all trades with deduplication
  - Stores data in CSV (prices/trades) and Parquet (orderbooks)
  - Configurable symbols via `LIST_MARKETS` variable
  - Auto-reconnection with exponential backoff

**Parameter Calculation**
- `calculate_avellaneda_parameters.py`: Implements Avellaneda-Stoikov model
  - Calculates optimal spreads (delta_a, delta_b) based on collected data
  - Volatility estimation via GARCH and rolling methods
  - Order arrival intensity calculation (A, k parameters)
  - Backtesting-based optimization for gamma (risk aversion) and time horizon
  - Outputs JSON files to `params/` directory

- `find_trend.py`: SuperTrend indicator calculation
  - Determines market directional bias (bullish/bearish)
  - Uses Numba-optimized calculations for performance
  - Backtests multiple parameter combinations to find optimal settings
  - Outputs signal to `params/supertrend_params_{symbol}.json`

**Trading Engine**
- `market_maker.py`: Main market making bot
  - Async event-driven architecture using asyncio
  - Dual WebSocket connections: market data + user account updates
  - Dynamic spread loading from Avellaneda parameter files
  - SuperTrend integration for directional bias
  - Position management with threshold-based mode switching
  - Order reuse logic to minimize API calls
  - Graceful shutdown with automatic order cleanup

**API Client**
- `api_client.py`: Aster Finance API wrapper
  - Ethereum-style signature authentication (EIP-712)
  - Both Pro API (trading) and API v1 (user streams) support
  - Async session management with aiohttp
  - Automatic parameter signing and nonce generation

**Utilities**
- `utils.py`: Shared functions for parameter validation, data loading, VWAP calculation
- `volatility.py`: GARCH and rolling volatility estimation
- `intensity.py`: Order arrival intensity parameter calculation
- `backtester.py`: Numba-optimized backtesting for parameter optimization
- `websocket_orders.py`: Standalone order monitoring WebSocket client
- `terminal_dashboard.py`: Rich terminal UI for account monitoring

### Data Flow

1. **Collection Phase**: `data_collector.py` continuously streams market data → saves to `ASTER_data/`
2. **Analysis Phase**: Parameter calculation scripts read from `ASTER_data/` → compute optimal parameters → save to `params/`
3. **Trading Phase**: `market_maker.py` loads parameters from `params/` → places orders via API → monitors fills via WebSocket

### State Management

The `StrategyState` class in `market_maker.py` maintains:
- Real-time market prices (bid/ask/mid from WebSocket)
- Account balances (USDF, USDT, position sizes)
- Active order tracking (order IDs, placement times)
- Mode state: 'BUY' (opening position) or 'SELL' (closing position)
- WebSocket health flags
- Order update queue for async communication between tasks

### Key Configuration Files

**Environment Variables (.env)**
```bash
# Pro API (Ethereum-style) - for trading
API_USER=0x...           # Main wallet address
API_SIGNER=0x...         # API wallet address
API_PRIVATE_KEY=0x...    # API wallet private key

# Pro API V3 user data streams use the same credentials above

# Trading symbol is configured in runtime.env
```

**Market Maker Parameters (market_maker.py)**
- `FLIP_MODE`: Boolean for long-biased (False) vs short-biased (True) strategy
- `USE_AVELLANEDA_SPREADS`: Toggle dynamic spreads vs fixed `DEFAULT_BUY_SPREAD`/`DEFAULT_SELL_SPREAD`
- `USE_SUPERTREND_SIGNAL`: Enable SuperTrend directional bias
- `DEFAULT_BALANCE_FRACTION`: Portion of balance per order (0.2 = 20%)
- `POSITION_THRESHOLD_USD`: USD value threshold to switch to position-reducing mode
- `ORDER_REFRESH_INTERVAL`: Seconds before canceling unfilled orders
- `RELEASE_MODE`: When True, suppresses non-error logs for production

## Development Workflows

### Adding a New Trading Symbol

1. Update `runtime.env`:
   ```bash
   SYMBOL=NEWUSDT
   ```

2. Add tick size to `get_fallback_tick_size()` in `utils.py`:
   ```python
   tick_sizes = {
       # ... existing tickers
       'NEW': 0.01,  # Adjust based on symbol price range
   }
   ```

3. Run data collection for sufficient time (1+ hours)

4. Calculate parameters:
   ```bash
   python calculate_avellaneda_parameters.py NEW --minutes 5
   python find_trend.py --symbol NEWUSDT --interval 5m
   ```

### Modifying Spread Calculation

The spread logic is in `market_maker.py:get_spreads()`. To customize:

1. Read Avellaneda parameters from `params/avellaneda_parameters_{symbol}.json`
2. Parameters include `delta_a` (ask spread) and `delta_b` (bid spread) in absolute price units
3. The `_SPREAD_CACHE` provides 10-second TTL caching to avoid excessive file reads
4. Validation ensures spreads are within `SPREAD_MIN_THRESHOLD` (0.005%) and `SPREAD_MAX_THRESHOLD` (2%)

### Testing WebSocket Connections

Use scripts in `tests/` directory:
- `websocket_depth.py`: Test orderbook stream
- `websocket_user_data.py`: Test account update stream
- `websocket_orders.py`: Test order fill notifications
- `test_user_stream_step_by_step.py`: Debug user stream connection

### Debugging Order Placement Issues

1. Set `RELEASE_MODE = False` in `market_maker.py` for detailed logs
2. Check `market_maker.log` for complete execution trace
3. Verify symbol filters with:
   ```python
   # In Python REPL with api_client initialized
   filters = await client.get_symbol_filters('ETHUSDT')
   print(filters)  # Shows price_precision, quantity_precision, tick_size, etc.
   ```
4. Monitor order reuse logic: orders are reused if price change < `DEFAULT_PRICE_CHANGE_THRESHOLD` (0.1%)

## Data Storage Structure

```
ASTER_data/
├── prices_{SYMBOL}.csv           # Timestamped bid/ask/mid prices
├── trades_{SYMBOL}.csv           # All executed trades with deduplication
└── orderbook_parquet/{SYMBOL}/   # Full orderbook snapshots
    ├── _latest.parquet           # Current staging file
    └── orderbook_*.parquet       # Timestamped archives

params/
├── avellaneda_parameters_{SYMBOL}.json  # Optimal spread parameters
└── supertrend_params_{SYMBOL}.json      # Trend direction signal
```

## Important Implementation Details

### Signature Authentication (api_client.py)

Aster Finance uses Ethereum-style signatures:
1. Parameters are JSON-stringified and sorted
2. EIP-712 encoding with [json_params, user_address, signer_address, nonce]
3. Keccak256 hash → sign with private key
4. Signature included in request headers

### Order Book VWAP Calculation

The `calculate_vwap()` function in `utils.py`:
- Target volume: $1000 USD by default
- Walks through bid/ask levels accumulating volume
- Returns volume-weighted average price up to target
- Used for more accurate mid-price than simple bid/ask average

### Numba Optimization

Performance-critical functions use `@jit` or `@njit` decorators:
- `backtester.py`: Trade simulation loops
- `find_trend.py`: SuperTrend indicator calculation
- `intensity.py`: Vectorized order arrival calculations

First run compiles these functions; subsequent runs are significantly faster.

### WebSocket Reconnection

Both `market_maker.py` and `data_collector.py` implement:
- Exponential backoff: starts at 1s, maxes at 60s
- Ping/pong timeout detection (15s timeout, 30s interval)
- Automatic listenKey refresh every 30 minutes for user streams
- Connection health monitoring via timestamps

## Docker Service Dependencies

The `docker-compose.yml` orchestrates 4 services:

1. **data-collector**: Runs continuously, restarts every `RESTART_MINUTES` if it fails
2. **avellaneda-params**: Recalculates parameters every `PARAM_REFRESH_MINUTES`
3. **trend-finder**: Updates trend signal every `TREND_REFRESH_MINUTES`
4. **market-maker**: Depends on data-collector and avellaneda-params, runs trading logic

`runtime.env` is the single source of truth for the active symbol across `data-collector`, `avellaneda-params`, `trend-finder`, and `market-maker`. The trading-related services still use the repo-root `.env` file for credentials.

## Risk Management Features

- **Position Threshold**: When position > `POSITION_THRESHOLD_USD`, bot only places orders to reduce position
- **Order Refresh**: Unfilled orders canceled after `ORDER_REFRESH_INTERVAL` to avoid stale prices
- **Price Staleness Check**: Rejects operations if price data older than 5 seconds
- **Balance Fraction**: Limits each order to fraction of available balance
- **Spread Validation**: Enforces min/max spread thresholds to prevent extreme values
- **Graceful Shutdown**: SIGINT/SIGTERM handlers ensure all orders canceled on exit

## Common Pitfalls

1. **Running market maker before data collection**: Requires at least 2 complete time periods of data for backtesting
2. **Missing .env file**: All trading scripts require properly configured API credentials
3. **Incorrect symbol format**: Use "BNBUSDT" not "BNB-USDT" or "BNB/USDT"
4. **Insufficient data for GARCH**: Volatility estimation needs substantial historical data (hours to days)
5. **Shared account interference**: Bot assumes exclusive control of account; manual trading creates position tracking issues
6. **Parameter file timing**: Market maker reads parameter files on startup and every 10 minutes, not on every order


