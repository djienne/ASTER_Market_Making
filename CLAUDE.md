# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ASTER Market Making is a Python-based two-sided market making bot for the Aster Finance DEX platform. It runs a volatility + order-book-imbalance (Vol+OBI) strategy ported from the lighter_MM project: volatility sets the half-spread, a Binance order-book-imbalance z-score (alpha) shifts the fair price, and inventory skew manages position risk. Both a bid and an ask are quoted simultaneously (one level per side, GTX post-only).

## Quick Start Commands

### Installation
```bash
pip install -r requirements.txt
```

### Running the Market Maker
```bash
# Requires .env file with API credentials. No parameter files needed —
# the Vol+OBI signal warms up from the live Binance depth stream.
python market_maker.py --symbol ETHUSDT
```

### Optional Analytics (not used by the live bot)
```bash
# Collect market data
python data_collector.py

# Avellaneda-Stoikov parameter analysis
python calculate_avellaneda_parameters.py ETH --minutes 5

# SuperTrend directional bias analysis
python find_trend.py --symbol ETHUSDT --interval 5m
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

**Trading Engine**
- `market_maker.py`: Main market making bot
  - Async event-driven architecture using asyncio
  - Three WebSocket connections: Aster depth5 (top-of-book), Aster user stream (fills/balances), Binance futures diff-depth @100ms (Vol+OBI signal)
  - Two-sided quoting: per-side order tracking in `state.side_orders['BUY'/'SELL']`
  - Inventory skew + dynamic max-position cap replace mode switching
  - Per-side order reuse logic (5 bps price threshold) to minimize API calls
  - Graceful shutdown with automatic order cleanup

- `vol_obi.py`: Vol+OBI strategy math (port of lighter_MM)
  - `RollingStats`: O(1) Welford ring-buffer mean/std/z-score
  - `VolObiCalculator`: volatility from mid-price changes, alpha from imbalance z-score, `quote()` returns skewed bid/ask
  - Fed via `on_sample(mid, imbalance)` from the Binance depth handler using incrementally maintained band totals

- `logging_config.py`: Non-blocking logging (QueueHandler/QueueListener) so log I/O never blocks the event loop; the log file rotates at 50MB x 5 backups

**Hot/cold path separation (important design constraint)**
- Hot path (never blocks, no REST/disk): Binance depth handler → O(Δ) band totals → `VolObiCalculator.on_sample` (O(1)) → refresh trigger; Aster depth handler → top-of-book snapshot → 2 bps prefilter; quote engine → `build_quote_set` (pure float math) → latest-wins command queue
- WebSocket payloads are parsed with `orjson`; EIP-712 signing is offloaded to a worker thread (`api_client._sign_async`) so Keccak/secp256k1 math never stalls the event loop
- Cold path: order manager REST calls, position reconciliation, listen-key keepalive, watchdog, reporters

**Optional Analytics (not consumed by the live bot)**
- `data_collector.py`: WebSocket-based market data collection into `ASTER_data/`
- `calculate_avellaneda_parameters.py`: Avellaneda-Stoikov parameter analysis
- `find_trend.py`: SuperTrend indicator analysis

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

1. **Signal**: Binance diff-depth @100ms → local bounded book → incremental ±2.5% band totals → `VolObiCalculator.on_sample(mid, bid_qty - ask_qty)` → volatility + alpha z-score
2. **Quote**: Aster depth5 top-of-book + Vol+OBI signal → `build_quote_set()` → two-sided `QuoteSetCommand` on the latest-wins queue
3. **Execute**: order manager reconciles desired vs live per side (cancel pass, then placement pass) → monitors fills via the user stream

### State Management

The `StrategyState` class in `market_maker.py` maintains:
- Real-time market prices (bid/ask/mid from WebSocket)
- Account balances (USDF, USDT, USDC, position sizes)
- Per-side order tracking: `side_orders['BUY'/'SELL']` (`SideOrderState`: order_id, price, quantity, reduce_only, placed_at)
- `vol_obi_calc` (the live calculator) and `vol_obi_snapshot` (immutable signal view)
- Binance local book + band totals for the OBI signal
- WebSocket health flags
- Order update queue + latest-wins `order_commands` queue (maxsize 1; one command always carries the full desired state of both sides)

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
- `OBI_VOL_TO_HALF_SPREAD` (env-overridable): volatility → half-spread gain; the primary tuning knob (42.0 from the lighter_MM production config; the vol_obi.py code default is 0.8 — 50x apart, tune in dry runs)
- `OBI_MIN_HALF_SPREAD_BPS` (env): half-spread floor per side (4 bps)
- `OBI_C1_TICKS` (env): alpha → fair-price shift in ticks per sigma (120)
- `OBI_SKEW` (env): inventory skew gain (1.5)
- `OBI_LOOKING_DEPTH`: imbalance band around Binance mid (±2.5%)
- `OBI_MIN_WARMUP_SAMPLES`: Binance depth samples required before quoting (100)
- `DEFAULT_BALANCE_FRACTION`: portion of balance per side (0.2 = 20%)
- `MAX_POSITION_SAFETY_FACTOR`: headroom under the leverage-derived position cap (0.9)
- `ORDER_REFRESH_INTERVAL`: safety lifetime before a resting order is refreshed (60s)
- `DEFAULT_PRICE_CHANGE_THRESHOLD_BPS`: per-side reuse threshold (5 bps, price-only)
- `RELEASE_MODE`: When True, suppresses non-error logs for production

## Development Workflows

### Adding a New Trading Symbol

1. Update `runtime.env`:
   ```bash
   SYMBOL=NEWUSDT
   ```

2. The symbol must exist on BOTH Aster and Binance USDT-margined futures (the Vol+OBI signal comes from the Binance diff-depth stream).

3. Start the bot — tick size and filters are fetched from the exchange at startup; no parameter files are needed.

### Modifying the Quote Calculation

The strategy math lives in `vol_obi.py` (`VolObiCalculator.quote()`); the per-cycle quote construction is `market_maker.py:build_quote_set()`:

1. `calc.quote(aster_mid, position_size)` returns the skewed, floored, tick-snapped bid/ask
2. `build_quote_set` applies the position cap (suppress + reduce-only), the opening circuit breaker, and the GTX post-only clamp (never cross the opposite side of the Aster book)
3. Each side is sized and validated independently via `prepare_order_candidate`
4. Keep `build_quote_set` pure float math off in-memory state — it runs on the hot path

### Testing WebSocket Connections

Use scripts in `tests/` directory:
- `websocket_depth.py`: Test orderbook stream
- `websocket_user_data.py`: Test account update stream
- `websocket_orders.py`: Test order fill notifications
- `test_user_stream_step_by_step.py`: Debug user stream connection

### Debugging Order Placement Issues

1. Set the `RELEASE_MODE=0` environment variable for detailed logs
2. Check `market_maker.log` for complete execution trace
3. Verify symbol filters with:
   ```python
   # In Python REPL with api_client initialized
   filters = await client.get_symbol_filters('ETHUSDT')
   print(filters)  # Shows price_precision, quantity_precision, tick_size, etc.
   ```
4. Monitor per-side order reuse: a side's resting order is reused if its price change < `DEFAULT_PRICE_CHANGE_THRESHOLD` (5 bps); quantity changes alone never force a replace
5. Watch for clusters of GTX post-only rejects — they mean the clamp is not being applied or `OBI_C1_TICKS` is too aggressive

## Data Storage Structure

```
ASTER_data/
├── prices_{SYMBOL}.csv           # Timestamped bid/ask/mid prices
├── trades_{SYMBOL}.csv           # All executed trades with deduplication
└── orderbook_parquet/{SYMBOL}/   # Full orderbook snapshots
    ├── _latest.parquet           # Current staging file
    └── orderbook_*.parquet       # Timestamped archives

params/
├── avellaneda_parameters_{SYMBOL}.json  # Analytics output (not read by the live bot)
└── supertrend_params_{SYMBOL}.json      # Analytics output (not read by the live bot)
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
- Exponential backoff: starts at 5s, maxes at 60s
- Ping/pong keepalive (20s interval, 10s timeout) plus recv-timeout stale detection
- Automatic listenKey keepalive every 10 minutes for user streams
- Proactive connection rotation before the documented 24h server limit (~23h)
- Connection health monitoring via timestamps

Note: every Binance signal-stream reconnect resets the Vol+OBI calculator — quotes are pulled until the ~10s warmup completes (signal integrity by design).

## Docker Service Dependencies

The `docker-compose.yml` defines 4 services:

1. **market-maker**: Self-contained trading logic — only needs `.env` credentials and WebSocket connectivity
2. **data-collector**: Optional analytics; gathers market data continuously
3. **avellaneda-params**: Optional analytics; recalculates parameters every `PARAM_REFRESH_MINUTES`
4. **trend-finder**: Optional analytics; updates trend signal every `TREND_REFRESH_MINUTES`

`runtime.env` is the single source of truth for the active symbol across all services. The trading-related services still use the repo-root `.env` file for credentials.

## Risk Management Features

- **Dynamic Position Cap**: `max_position_usd = (balance * leverage - 2 * order_value) * 0.9`; at the cap the position-increasing side is suppressed and the surviving side is flagged reduce-only
- **Inventory Skew**: Long inventory widens the bid and tightens the ask (and vice versa), continuously steering position back to flat
- **Order Refresh**: Resting orders refreshed after `ORDER_REFRESH_INTERVAL` to avoid stale prices (per side)
- **Signal Staleness**: Quotes are pulled when the Binance Vol+OBI feed disconnects or goes stale (>5s); warmup restarts after every reconnect
- **Price Staleness Check**: Rejects quoting if Aster price data is older than 30 seconds
- **GTX Post-Only Clamp**: Quotes that would cross the opposite side of the Aster book are clamped one tick inside it
- **Spread Floor**: `OBI_MIN_HALF_SPREAD_BPS` keeps quotes at least 4 bps per side off the mid
- **Opening Circuit Breaker**: 3 opening-order failures in 60s pause exposure-adding quotes for 120s (the reducing side keeps working)
- **Graceful Shutdown**: SIGINT/SIGTERM handlers cancel both sides, with a REST cancel-all backstop

## Common Pitfalls

1. **Missing .env file**: All trading scripts require properly configured API credentials
2. **Incorrect symbol format**: Use "BNBUSDT" not "BNB-USDT" or "BNB/USDT"
3. **Symbol not on Binance futures**: The Vol+OBI signal needs the same symbol on Binance USDT-margined futures
4. **Shared account interference**: Bot assumes exclusive control of account; manual trading creates position tracking issues (the open-order watchdog cancels untracked orders)
5. **Daily warmup gaps**: WebSocket connections rotate proactively every ~23h; each Binance reconnect restarts the ~10s Vol+OBI warmup and pulls quotes until it completes
6. **Tuning `OBI_VOL_TO_HALF_SPREAD`**: lighter_MM production used 42.0, the vol_obi.py code default is 0.8 — these differ by 50x; validate spreads in a dry run before sizing up


