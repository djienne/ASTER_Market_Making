# Market Maker Bot (`market_maker.py`) Code Summary

This document provides a summary of the structure, components, and logic of the `market_maker.py` script.

## 1. Overview

The script is an asynchronous, event-driven, two-sided market-making bot for the Aster Finance platform. It continuously quotes a bid and an ask (one level per side, GTX post-only) around a fair price derived from the Aster mid-price and a Binance order-book-imbalance signal. The strategy is a port of the lighter_MM Vol+OBI model:

- **Volatility** (per-second standard deviation of Binance 100ms mid-price changes) sets the half-spread width.
- **Alpha** (z-score of the Binance order-book imbalance within ±2.5% of mid) shifts the fair price: `fair = aster_mid + c1 * alpha`.
- **Inventory skew** widens the position-increasing side and tightens the reducing side as inventory builds.
- **A dynamic max-position cap** (`(balance * leverage - 2 * order_value) * 0.9`) suppresses the increasing side entirely at the limit and flags the surviving side reduce-only.

The bot is built using Python's `asyncio` library for concurrent operations and `websockets` for real-time data feeds.

## 2. File Structure

1.  **Imports**: Standard libraries plus project modules (`api_client`, `vol_obi`, `logging_config`, `utils`).
2.  **Configuration**: Global constants for the strategy (`OBI_*` knobs, env-overridable), timing, order reuse, circuit breaker, and the Binance feed plumbing.
3.  **Logging Setup**: `setup_logging` delegates to `logging_config.setup_root_logging`, which routes all records through a `QueueHandler`/`QueueListener` pair so log I/O never blocks the event loop (hot-path safety).
4.  **Strategy Math** (`vol_obi.py`): `RollingStats` (O(1) Welford ring buffer) and `VolObiCalculator` (volatility, alpha, and the skewed `quote()` calculation).
5.  **State Management**: `StrategyState` holds all dynamic data; `SideOrderState` tracks the resting order per side (`state.side_orders['BUY'/'SELL']`).
6.  **Core Logic**:
    *   **WebSocket Handlers**: `websocket_price_updater` (Aster depth5), `websocket_user_data_updater` (fills/balances), `binance_orderbook_imbalance_updater` (Binance diff-depth @100ms feeding the Vol+OBI signal).
    *   **Quote Engine**: `QuoteEngine.run` → `build_quote_set` computes the desired two-sided quote set.
    *   **Order Manager**: `order_manager_loop_impl` → `apply_quote_set` reconciles desired vs live orders per side.
7.  **Execution Block**: `main()` handles initialization, task creation, and graceful shutdown.

## 3. Hot/Cold Path Separation

A first-class design constraint, mirroring lighter_MM:

- **Hot path** (latency-critical; never blocks, no REST, no disk I/O):
  - Binance depth handler → O(Δ) incremental band totals (rebuilt only on ≥1 bps mid drift) → `VolObiCalculator.on_sample` (O(1) Welford) → cheap quote-refresh trigger.
  - Aster depth handler → immutable top-of-book snapshot → 2 bps quote-center prefilter → `quote_refresh_event`.
  - Quote engine → `build_quote_set` (pure float math off in-memory state; `Decimal` only at the order-formatting edge) → latest-wins command queue.
- **Cold path** (may await REST): order placement/cancellation, position reconciliation, listen-key keepalive, the open-order watchdog, and periodic reporters.
- Logging is non-blocking everywhere (queue-based), and the quote engine never awaits order placement — they communicate only through the latest-wins queue.

## 4. Core Components

### Configuration

Key parameters (the `OBI_*` knobs are environment-overridable):
- **Strategy**: `OBI_VOL_TO_HALF_SPREAD` (primary tuning knob, 42.0), `OBI_MIN_HALF_SPREAD_BPS` (4), `OBI_C1_TICKS` (120), `OBI_SKEW` (1.5), `OBI_LOOKING_DEPTH` (0.025), `OBI_MIN_WARMUP_SAMPLES` (100), `DEFAULT_BALANCE_FRACTION` (0.2), `MAX_POSITION_SAFETY_FACTOR` (0.9).
- **Timing**: `ORDER_REFRESH_INTERVAL` (60s safety lifetime per side), `MIN_ORDER_INTERVAL` (pacing per reconcile burst), `DEFAULT_PRICE_CHANGE_THRESHOLD_BPS` (5 bps per-side reuse threshold).
- **Safety**: opening circuit breaker (`ORDER_FAILURE_LIMIT` failures in `ORDER_FAILURE_WINDOW_SECONDS` → `OPENING_CIRCUIT_BREAKER_COOLDOWN` pause), `BINANCE_OBI_STALE_TIMEOUT_SECONDS` (signal staleness gate).
- **Logging**: `LOG_FILE`, `RELEASE_MODE` (env flag).

### `StrategyState` Class

- **Market Data**: `bid_price`, `ask_price`, `mid_price` plus an immutable `aster_top_of_book_snapshot`.
- **Signal**: `vol_obi_calc` (live `VolObiCalculator`) and `vol_obi_snapshot` (immutable view: warmed_up, volatility, alpha, freshness), plus the bounded local Binance book and its band totals.
- **Position & Orders**: `position_size` (position snapshots are the source of truth), `side_orders['BUY'/'SELL']` (`SideOrderState`: order_id, price, quantity, reduce_only, placed_at), `pending_terminal_orders` for fast-replaced orders awaiting terminal confirmation.
- **Account Data**: `account_balance` (USDF + USDT + USDC wallet balances).
- **Communication**: `order_updates` queue (user-stream events) and the latest-wins `order_commands` queue (maxsize 1 — every command carries the full desired state of both sides, so an update can never be half-evicted).
- **Health Flags**: `price_ws_connected`, `user_data_ws_connected`, `binance_alpha_ws_connected`.

### WebSocket Handlers

1.  **`websocket_price_updater`** — Aster public depth stream (`@depth5`): publishes top-of-book snapshots and wakes the quote engine when the estimated quote center moves ≥2 bps (or one tick). Auto-reconnects with exponential backoff and stale-connection detection.
2.  **`websocket_user_data_updater`** — private user stream via `listenKey`: balance updates (`ACCOUNT_UPDATE`, including position snapshots) and order updates (`ORDER_TRADE_UPDATE`) into `state.order_updates`. Manages listen-key keepalive.
3.  **`binance_orderbook_imbalance_updater`** — Binance futures diff-depth @100ms: maintains a bounded local book (snapshot + sequenced diffs, resync on gaps), keeps incremental ±2.5% band totals, and feeds `VolObiCalculator.on_sample(binance_mid, bid_qty - ask_qty)` every tick. The calculator resets on every reconnect (warmup restarts by design — a stale signal must never survive a reconnect). Triggers a quote refresh when alpha moves the fair price materially.

### Quote Engine (`QuoteEngine.run` → `build_quote_set`)

Each cycle (woken by the refresh event):
1.  **Gates**: symbol `TRADING`, both Aster sockets up, fresh price and balance data, Vol+OBI signal live (warmed up + connected + updated within 5s) — otherwise any working orders are pulled via a `cancel_all` command.
2.  **Position cap**: `compute_max_position_usd` is recomputed from live balance and pushed into the calculator.
3.  **Quote**: `calc.quote(aster_mid, position_size)` returns the skewed, floored, tick-snapped bid/ask (or `(None, None)` if crossed/warming).
4.  **Suppression**: at the position cap the increasing side is dropped and the surviving side is flagged reduce-only; while the opening circuit breaker is active only the reducing side keeps quoting.
5.  **GTX clamp**: a quote that would cross the opposite side of the Aster book is clamped one tick inside it (post-only orders are rejected outright if they cross).
6.  **Validation**: each side is sized (`balance * DEFAULT_BALANCE_FRACTION / mid`, or `abs(position)` for reduce-only) and validated independently against exchange filters.
7.  **Publish**: the resulting `QuoteSetCommand` is published only if some side differs from the live orders beyond the 5 bps reuse threshold (price-only comparison — quantity wobble does not churn quotes).

### Order Manager (`order_manager_loop_impl` → `apply_quote_set`)

- Waits concurrently on order updates, new commands, and the per-side refresh timeout.
- **`apply_quote_set`** reconciles desired vs live: a cancel pass first (so a large fair-price jump can never leave a new bid resting above the bot's own still-live old ask), then a placement pass. Pacing (`MIN_ORDER_INTERVAL`) gates the reconcile burst, not each order, so both sides go out on the same price snapshot; after any pacing wait the freshest command is re-drained so stale prices are never sent.
- **Fast replace** (default): cancel is sent and the old order is parked in `pending_terminal_orders` (resolved asynchronously) so the new quote is placed immediately.
- **Fills**: terminal updates are matched against both `side_orders` and `pending_terminal_orders`; position is reconciled from position snapshots (WebSocket seq-wait with REST fallback) — never derived from fill quantities.
- **Watchdog**: periodically cancels any open order on the exchange that is not tracked locally.

## 5. Execution Flow and Shutdown

1.  **Initialization**: parse `--symbol`, set up non-blocking logging, load `.env` credentials.
2.  **Cleanup**: cancel-all for the symbol; abort startup if it cannot be confirmed.
3.  **Setup**: fetch initial balance and symbol filters, construct the `VolObiCalculator` with the Aster tick size, set leverage if flat.
4.  **Warmup**: support tasks start (price, user data, Binance signal); trading loops start only after the Vol+OBI signal is warmed up.
5.  **Running**: quote engine, order manager, watchdog, and reporters run concurrently until shutdown.
6.  **Graceful Shutdown**: `SIGINT`/`SIGTERM` set the shutdown flag; the order manager receives a `cancel_all` command and both sides are cleared, with a final REST cancel-all backstop (`cleanup_orders`) before exit.
