# Aster Finance Market Making Bot

Python market-making tooling for Aster Finance using WebSocket market data plus signed REST/WebSocket trading APIs.

The main strategy combines:
- Avellaneda-Stoikov style spread calculation
- SuperTrend directional bias
- WebSocket-based price, balance, and order monitoring

Referral link: [https://www.asterdex.com/en/referral/164f81](https://www.asterdex.com/en/referral/164f81)

## Operating Assumptions

- The market maker assumes it has exclusive control of the account and of `BTCUSDT` trading on that account.
- Do not run this bot alongside a manual trader or another bot on the same account.
- `market_maker.py` cancels all open orders for the configured symbol on startup and again during shutdown cleanup.
- If the startup cancel-all cannot be confirmed, the bot aborts instead of trading on top of unknown open orders.
- If that behavior is not acceptable for your setup, do not run the trading bot as-is.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set SYMBOL=BTCUSDT in .env, or pass explicit CLI symbols below.

# Collect market data
python data_collector.py
python data_collector.py BTCUSDT ETHUSDT

# Compute parameters from local data
python calculate_avellaneda_parameters.py BTC --minutes 5
python find_trend.py --symbol BTCUSDT --interval 5m

# Run the market maker
python market_maker.py
python market_maker.py --symbol BTCUSDT
```

## Configuration

### `.env`

You need both Aster **Pro API** credentials for trading and Aster **API** credentials for user-data streams:

![API Management](APIs.png)

```bash
# API V3 (Ethereum-style) - trading operations
API_USER=0x...
API_SIGNER=0x...
API_PRIVATE_KEY=0x...

# API V1 (HMAC-style) - user data streams
APIV1_PUBLIC_KEY=...
APIV1_PRIVATE_KEY=...

# Default symbol used when CLI args do not override it
SYMBOL=BTCUSDT
```

<img src="infos_API_p1.png" alt="Info API 1" width="600"/>
<img src="infos_API_p2.png" alt="Info API 2" width="600"/>

### Runtime Symbol Behavior

- `market_maker.py` uses `--symbol` first, then `.env` `SYMBOL`, then falls back to `BTCUSDT`.
- `data_collector.py` defaults to `.env` `SYMBOL`, or `BTCUSDT` if unset.
- `calculate_avellaneda_parameters.py` defaults to the base ticker derived from `.env` `SYMBOL` after stripping common stablecoin quotes like `USDT`, `USDC`, `USDF`, `USD1`, and `USD`.
- The local analytics loader accepts either a base ticker like `BTC` or a full symbol like `BTCUSDT`, then resolves the matching local trades/orderbook data using the available quote-suffix files.
- `find_trend.py` defaults to `.env` `SYMBOL`, or `BTCUSDT`, and writes its params file using the same base-symbol normalization.

## Main Strategy Parameters

Current defaults live in [market_maker.py](market_maker.py).

```python
DEFAULT_SYMBOL = "BTCUSDT"
FLIP_MODE = False
DEFAULT_BUY_SPREAD = 0.006
DEFAULT_SELL_SPREAD = 0.006
USE_AVELLANEDA_SPREADS = True
DEFAULT_BALANCE_FRACTION = 0.2
POSITION_THRESHOLD_USD = 15.0

ORDER_REFRESH_INTERVAL = 30
PRICE_REPORT_INTERVAL = 60
BALANCE_REPORT_INTERVAL = 60

USE_SUPERTREND_SIGNAL = True
SUPERTREND_CHECK_INTERVAL = 600

DEFAULT_PRICE_CHANGE_THRESHOLD = 0.0001  # 1 bp
CANCEL_SPECIFIC_ORDER = True

RELEASE_MODE = True
```

Important notes:
- `DEFAULT_BALANCE_FRACTION` currently sizes from tracked wallet balances (`walletBalance` from account snapshots / user stream), not `availableBalance`.
- `POSITION_THRESHOLD_USD` controls when a position is treated as significant for bias/mode logic, but the bot still tries to flatten any non-zero BTC position before opening fresh inventory.
- Positions that round below exchange `minQty` or `minNotional` cannot be reduced automatically and will block new openings until they are cleared.
- `DEFAULT_PRICE_CHANGE_THRESHOLD = 0.0001` means the bot tries not to refresh an order unless the intended price moves by at least 1 basis point.
- The fallback spread logic is still `+/-0.6%` if dynamic parameters are unavailable.
- The bot assumes exclusive ownership of the account and symbol, cancels all open orders for the configured symbol during startup and shutdown, and aborts startup if it cannot confirm the initial cleanup.

## Available Scripts

```bash
# Trading
python market_maker.py
python market_maker.py --symbol BTCUSDT

# Data / analytics
python data_collector.py
python calculate_avellaneda_parameters.py BTC
python find_trend.py --symbol BTCUSDT --interval 5m

# Monitoring / utilities
python terminal_dashboard.py
python get_my_trading_volume.py --symbol BTCUSDT --days 7
python get_my_trading_volume.py --days 30
```

## Terminal Dashboard

`terminal_dashboard.py` gives a live account view with:
- balances
- open positions
- recent order activity

```bash
python terminal_dashboard.py
```

<img src="dashboard.png" alt="dashboard" width="1000"/>

## Docker

The repository currently ships with only `data-collector` enabled in [docker-compose.yml](docker-compose.yml).

Commented service templates are included for:
- `avellaneda-params`
- `market-maker`
- `trend-finder`

You need to uncomment those service blocks before starting them through Compose.

```bash
docker-compose build
docker-compose up -d
docker-compose up -d data-collector
docker-compose logs -f data-collector
docker-compose down
```

## Testing

Local-safe tests run by default and skip live exchange scripts unless you opt in.

```bash
pytest -q

# Only if you intentionally want live API test collection:
RUN_LIVE_API_TESTS=1 pytest -q
```

The default test suite does not place live trades. It covers local order-state logic, filter rounding, parameter-file loading, and analytics helpers.

## Performance Notes

Low latency matters for market making, especially if you reduce `ORDER_REFRESH_INTERVAL`.

Recommendations:
- Prefer an Asia-Pacific region close to the exchange
- AWS Tokyo (`ap-northeast-1`) is the suggested baseline
- Avoid noisy shared infrastructure during active trading periods

## Risk Warning

This software will likely lose money, even if it generates significant volume, because it is not competitive with professional firms. Start with very small amounts and make sure you understand the risks of automated crypto trading.
