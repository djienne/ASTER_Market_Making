import asyncio
import logging
import statistics

import market_maker
from vol_obi import RollingStats, VolObiCalculator


TEST_FILTERS = {
    "status": "TRADING",
    "tick_size": 0.1,
    "price_precision": 1,
    "step_size": 0.001,
    "quantity_precision": 3,
    "min_qty": 0.001,
    "min_notional": 5.0,
}


def _make_warmed_calc(tick_size=0.1, alpha=0.0, **overrides):
    """Build a warmed-up VolObiCalculator with a controlled alpha."""
    settings = dict(
        tick_size=tick_size,
        window_steps=500,
        step_ns=100_000_000,
        vol_to_half_spread=42.0,
        min_half_spread_bps=4.0,
        c1_ticks=120.0,
        skew=1.5,
        looking_depth=0.025,
        min_warmup_samples=10,
        max_position_dollar=1000.0,
    )
    settings.update(overrides)
    calc = VolObiCalculator(**settings)

    mid = 100.0
    for i in range(30):
        mid += 0.01 if i % 2 == 0 else -0.01
        calc.on_sample(mid, 10.0 if i % 2 == 0 else -10.0)

    calc.set_alpha_override(alpha)
    calc.on_sample(mid, 0.0)
    return calc


def _make_live_state(calc=None, mid=100.0, balance=1000.0):
    """Build a StrategyState that passes all build_quote_set liveness gates."""
    state = market_maker.StrategyState()
    state.vol_obi_calc = calc if calc is not None else _make_warmed_calc()
    state.mid_price = mid
    state.bid_price = mid - 0.1
    state.ask_price = mid + 0.1
    state.account_balance = balance
    state.balance_last_updated = 1.0
    state.price_ws_connected = True
    state.user_data_ws_connected = True
    state.binance_alpha_ws_connected = True
    state.binance_alpha_last_updated = 9.5
    market_maker.publish_vol_obi_snapshot(state)
    return state


def _make_runtime(clock_value=10.0):
    return market_maker.RuntimeContext("BTCUSDT", clock=lambda: clock_value)


# ---------------------------------------------------------------------------
# Order update classification
# ---------------------------------------------------------------------------

def test_classify_order_update_distinguishes_fill_from_cancel():
    canceled = market_maker.classify_order_update({"X": "CANCELED", "z": "0", "ap": "100"})
    assert canceled["is_terminal"] is True
    assert canceled["treat_as_fill"] is False

    canceled_with_fill = market_maker.classify_order_update({"X": "CANCELED", "z": "0.2", "ap": "100"})
    assert canceled_with_fill["is_terminal"] is True
    assert canceled_with_fill["treat_as_fill"] is True


def test_classify_order_update_supports_rest_payload_fields():
    rest_update = market_maker.classify_order_update({"status": "CANCELED", "executedQty": "0.2"})

    assert rest_update["is_terminal"] is True
    assert rest_update["treat_as_fill"] is True
    assert rest_update["filled_qty"] == 0.2


def test_classify_order_update_keeps_partial_fills_open():
    partial = market_maker.classify_order_update({"X": "PARTIALLY_FILLED", "z": "0.05", "ap": "100"})

    assert partial["is_terminal"] is False
    assert partial["treat_as_fill"] is False


# ---------------------------------------------------------------------------
# Rounding / order candidate validation
# ---------------------------------------------------------------------------

def test_round_price_to_tick_stays_passive_for_each_side():
    assert abs(market_maker.round_price_to_tick(100.06, 0.1, "BUY") - 100.0) < 1e-9
    assert abs(market_maker.round_price_to_tick(100.06, 0.1, "SELL") - 100.1) < 1e-9


def test_round_quantity_to_step_respects_non_decimal_lot_sizes():
    assert abs(market_maker.round_quantity_to_step(1.234, 0.005) - 1.23) < 1e-9
    assert abs(market_maker.round_quantity_to_step(0.0149, 0.005) - 0.01) < 1e-9


def test_prepare_order_candidate_rejects_quantity_below_min_qty():
    candidate = market_maker.prepare_order_candidate(
        {
            "tick_size": 0.1,
            "price_precision": 1,
            "step_size": 0.005,
            "quantity_precision": 3,
            "min_qty": 0.015,
            "min_notional": 5.0,
        },
        side="BUY",
        reduce_only=False,
        limit_price=100.06,
        quantity_to_trade=0.0149,
    )

    assert candidate["ok"] is False
    assert candidate["reason"] == "min_qty"
    assert abs(candidate["rounded_price"] - 100.0) < 1e-9
    assert abs(candidate["rounded_quantity"] - 0.01) < 1e-9


# ---------------------------------------------------------------------------
# Vol+OBI calculator (ported strategy math)
# ---------------------------------------------------------------------------

def test_rolling_stats_matches_statistics_module():
    stats = RollingStats(5)
    values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
    for value in values:
        stats.push(value)

    window = values[-5:]
    assert abs(stats.mean() - statistics.mean(window)) < 1e-9
    assert abs(stats.std() - statistics.pstdev(window)) < 1e-9


def test_vol_obi_does_not_quote_before_warmup():
    calc = VolObiCalculator(tick_size=0.1, min_warmup_samples=100)
    calc.on_sample(100.0, 5.0)
    calc.on_sample(100.1, 5.0)

    assert calc.warmed_up is False
    assert calc.quote(100.0, 0.0) == (None, None)


def test_vol_obi_inventory_skew_widens_the_position_increasing_side():
    calc = _make_warmed_calc()

    flat_bid, flat_ask = calc.quote(100.0, 0.0)
    long_bid, long_ask = calc.quote(100.0, 6.0)   # $600 long of a $1000 cap
    short_bid, short_ask = calc.quote(100.0, -6.0)

    # Long inventory: bid backs away from fair, ask leans in to shed.
    assert (100.0 - long_bid) > (100.0 - flat_bid)
    assert (long_ask - 100.0) < (flat_ask - 100.0)
    # Short inventory: mirrored.
    assert (100.0 - short_bid) < (100.0 - flat_bid)
    assert (short_ask - 100.0) > (flat_ask - 100.0)


def test_vol_obi_enforces_min_half_spread_floor():
    # Near-zero volatility: only the bps floor keeps quotes off the mid.
    calc = VolObiCalculator(
        tick_size=0.001,
        min_warmup_samples=5,
        vol_to_half_spread=0.0,
        min_half_spread_bps=4.0,
        c1_ticks=0.0,
        c1=0.000001,
    )
    for _ in range(10):
        calc.on_sample(100.0, 0.0)

    bid, ask = calc.quote(100.0, 0.0)
    assert bid <= 100.0 * (1.0 - 4.0 / 10000.0) + 1e-9
    assert ask >= 100.0 * (1.0 + 4.0 / 10000.0) - 1e-9


def test_vol_obi_returns_none_when_quotes_would_cross():
    # Max long with a huge skew drives ask depth negative -> floored to fair,
    # while the min-spread floor pulls both sides to the same band edge only
    # if it cannot cross. A zero floor and zero vol forces bid == ask == fair.
    calc = VolObiCalculator(
        tick_size=0.1,
        min_warmup_samples=5,
        vol_to_half_spread=0.0,
        min_half_spread_bps=0.0,
        c1=0.000001,
    )
    for _ in range(10):
        calc.on_sample(100.0, 0.0)

    assert calc.quote(100.0, 0.0) == (None, None)


def test_compute_binance_band_imbalance_uses_raw_quantity_difference():
    state = market_maker.StrategyState()
    state.binance_bid_book = {100.0: 5.0, 99.0: 4.0, 96.0: 100.0}
    state.binance_ask_book = {101.0: 3.0, 102.0: 2.0, 104.0: 200.0}

    imbalance = market_maker.compute_binance_band_imbalance(state)

    # Band is +/-2.5% around mid 100.5: bids 100+99, asks 101+102.
    assert abs(imbalance - (9.0 - 5.0)) < 1e-9


# ---------------------------------------------------------------------------
# Position cap and quote-set construction
# ---------------------------------------------------------------------------

def test_compute_max_position_usd_reserves_order_margin_and_safety():
    state = market_maker.StrategyState()
    state.account_balance = 1000.0

    expected = (1000.0 * market_maker.DEFAULT_LEVERAGE - 2.0 * 1000.0 * market_maker.DEFAULT_BALANCE_FRACTION) * \
        market_maker.MAX_POSITION_SAFETY_FACTOR
    assert abs(market_maker.compute_max_position_usd(state) - expected) < 1e-9

    state.account_balance = None
    assert market_maker.compute_max_position_usd(state) == 0.0


def test_build_quote_set_emits_two_sided_quotes():
    state = _make_live_state()
    runtime = _make_runtime()

    command, diagnostics = market_maker.build_quote_set(state, TEST_FILTERS, runtime)

    assert diagnostics["reason"] == "ok"
    assert command.kind == "quote_set"
    assert command.bid is not None and command.ask is not None
    assert command.bid.side == "BUY" and command.ask.side == "SELL"
    assert command.bid.price < command.ask.price
    assert command.bid.reduce_only is False and command.ask.reduce_only is False
    assert command.bid.quantity > 0 and command.ask.quantity > 0


def test_build_quote_set_requires_live_vol_obi_signal():
    state = _make_live_state()
    state.binance_alpha_last_updated = 1.0  # stale vs clock 10.0 and 5s timeout
    market_maker.publish_vol_obi_snapshot(state)
    runtime = _make_runtime()

    command, diagnostics = market_maker.build_quote_set(state, TEST_FILTERS, runtime)

    assert command is None
    assert diagnostics["reason"] == "vol_obi_unavailable"


def test_build_quote_set_suppresses_increasing_side_at_position_limit():
    state = _make_live_state()
    runtime = _make_runtime()
    max_pos = market_maker.compute_max_position_usd(state)
    state.position_size = (max_pos / state.mid_price) * 1.01  # just past the cap, long

    command, diagnostics = market_maker.build_quote_set(state, TEST_FILTERS, runtime)

    assert diagnostics["reason"] == "ok"
    assert command.bid is None
    assert command.ask is not None
    assert command.ask.reduce_only is True
    assert abs(command.ask.quantity - market_maker.round_quantity_to_step(
        abs(state.position_size), TEST_FILTERS["step_size"])) < 1e-9


def test_build_quote_set_clamps_post_only_bid_below_best_ask():
    # Strong positive alpha pushes fair price far through the book.
    calc = _make_warmed_calc(alpha=5.0)
    state = _make_live_state(calc=calc)
    runtime = _make_runtime()

    command, diagnostics = market_maker.build_quote_set(state, TEST_FILTERS, runtime)

    assert diagnostics["reason"] == "ok"
    if command.bid is not None:
        assert command.bid.price < state.ask_price  # GTX-safe: never crosses best ask
    if command.bid is not None and command.ask is not None:
        assert command.bid.price < command.ask.price


def test_build_quote_set_keeps_reducing_side_when_circuit_breaker_active():
    state = _make_live_state()
    state.position_size = 1.0  # long, well under the cap
    state.opening_circuit_breaker_until = 20.0  # active vs clock 10.0
    runtime = _make_runtime()

    command, diagnostics = market_maker.build_quote_set(state, TEST_FILTERS, runtime)

    assert diagnostics["reason"] == "ok"
    assert command.bid is None       # exposure-adding side paused
    assert command.ask is not None   # reducing side keeps working

    state.position_size = 0.0
    command, diagnostics = market_maker.build_quote_set(state, TEST_FILTERS, runtime)
    assert command is None
    assert diagnostics["reason"] == "opening_circuit_breaker"


# ---------------------------------------------------------------------------
# Per-side reuse and quote-set diffing
# ---------------------------------------------------------------------------

def test_should_reuse_side_applies_price_threshold_only():
    side_state = market_maker.SideOrderState(order_id=42, price=100.0, quantity=2.0)

    assert market_maker.should_reuse_side(side_state, 100.01) is True   # 1 bps
    assert market_maker.should_reuse_side(side_state, 100.2) is False   # 20 bps
    assert market_maker.should_reuse_side(market_maker.SideOrderState(), 100.0) is False


def test_quote_set_requires_update_detects_per_side_changes():
    state = market_maker.StrategyState()
    bid = market_maker.SideQuote(side="BUY", price=99.5, quantity=2.0)
    ask = market_maker.SideQuote(side="SELL", price=100.5, quantity=2.0)
    command = market_maker.QuoteSetCommand(kind="quote_set", bid=bid, ask=ask)

    # Nothing live yet: needs update.
    assert market_maker.quote_set_requires_update(state, command) is True

    state.side_orders["BUY"] = market_maker.SideOrderState(order_id=1, price=99.5, quantity=2.0)
    state.side_orders["SELL"] = market_maker.SideOrderState(order_id=2, price=100.5, quantity=2.0)
    assert market_maker.quote_set_requires_update(state, command) is False

    # Price drift beyond 5 bps on one side.
    moved = market_maker.QuoteSetCommand(
        kind="quote_set",
        bid=market_maker.SideQuote(side="BUY", price=99.0, quantity=2.0),
        ask=ask,
    )
    assert market_maker.quote_set_requires_update(state, moved) is True

    # Side disappearing requires an update too.
    one_sided = market_maker.QuoteSetCommand(kind="quote_set", bid=None, ask=ask)
    assert market_maker.quote_set_requires_update(state, one_sided) is True


# ---------------------------------------------------------------------------
# Quote engine
# ---------------------------------------------------------------------------

class _FiltersClient:
    def __init__(self, filters=None):
        self.filters = dict(filters or TEST_FILTERS)

    async def get_symbol_filters(self, symbol):
        return self.filters


def test_quote_engine_emits_two_sided_quote_set_command():
    async def runner():
        state = _make_live_state()
        runtime = _make_runtime()
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(market_maker.market_making_loop(state, _FiltersClient(), "BTCUSDT", runtime))
        market_maker.request_quote_refresh(state)

        for _ in range(20):
            if not state.order_commands.empty():
                break
            await asyncio.sleep(0.01)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        command = state.order_commands.get_nowait()
        assert command.kind == "quote_set"
        assert command.bid is not None and command.ask is not None
        assert command.bid.price < command.ask.price

    asyncio.run(runner())


def test_quote_engine_cancels_all_when_symbol_is_not_trading():
    async def runner():
        halted = dict(TEST_FILTERS, status="HALT")
        state = _make_live_state()
        state.side_orders["BUY"] = market_maker.SideOrderState(order_id=42, price=99.0, quantity=2.0, placed_at=9.0)
        runtime = _make_runtime()
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(
            market_maker.market_making_loop(state, _FiltersClient(halted), "BTCUSDT", runtime)
        )
        market_maker.request_quote_refresh(state)

        for _ in range(20):
            if not state.order_commands.empty():
                break
            await asyncio.sleep(0.01)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        command = state.order_commands.get_nowait()
        assert command.kind == "cancel_all"
        assert "Symbol status HALT" in command.trigger

    asyncio.run(runner())


def test_quote_engine_stays_quiet_when_breaker_active_and_flat():
    async def runner():
        state = _make_live_state()
        state.opening_circuit_breaker_until = 20.0
        runtime = _make_runtime()
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(market_maker.market_making_loop(state, _FiltersClient(), "BTCUSDT", runtime))
        market_maker.request_quote_refresh(state)
        await asyncio.sleep(0.05)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        assert state.order_commands.empty()

    asyncio.run(runner())


def test_quote_engine_does_not_emit_opening_quote_without_enough_capital():
    async def runner():
        # Balance below the buffered minimum-notional requirement.
        filters = dict(TEST_FILTERS, min_notional=10.0)
        state = _make_live_state(balance=20.0)
        runtime = _make_runtime()
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(
            market_maker.market_making_loop(state, _FiltersClient(filters), "BTCUSDT", runtime)
        )
        market_maker.request_quote_refresh(state)
        await asyncio.sleep(0.05)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        assert state.order_commands.empty()

    asyncio.run(runner())


# ---------------------------------------------------------------------------
# Order manager (two concurrent orders)
# ---------------------------------------------------------------------------

def test_order_manager_places_both_sides_and_fill_leaves_other_side_live(monkeypatch):
    class TwoSidedClient:
        def __init__(self):
            self.placed = []
            self.cancel_all_calls = 0
            self.next_order_id = 100

        async def place_order(self, symbol, price, quantity, side, reduce_only=False):
            self.next_order_id += 1
            self.placed.append((side, float(price), float(quantity), reduce_only))
            return {"orderId": self.next_order_id}

        async def cancel_all_orders(self, symbol):
            self.cancel_all_calls += 1
            return {}

        async def get_position_risk(self, symbol):
            return [{"positionAmt": "0.5", "notional": "50.0"}]

    async def runner():
        monkeypatch.setattr(market_maker, "MIN_ORDER_INTERVAL", 0.0)
        monkeypatch.setattr(market_maker, "POSITION_SYNC_TIMEOUT", 0.01)
        state = market_maker.StrategyState()
        state.mid_price = 100.0
        runtime = _make_runtime()
        client = TwoSidedClient()

        market_maker.publish_latest_order_command(
            state,
            market_maker.QuoteSetCommand(
                kind="quote_set",
                bid=market_maker.SideQuote(side="BUY", price=99.5, quantity=2.0),
                ask=market_maker.SideQuote(side="SELL", price=100.5, quantity=2.0),
                trigger="test",
            ),
        )

        task = asyncio.create_task(market_maker.order_manager_loop(state, client, "BTCUSDT", runtime))

        for _ in range(100):
            if len(client.placed) == 2:
                break
            await asyncio.sleep(0.01)
        assert len(client.placed) == 2
        assert {p[0] for p in client.placed} == {"BUY", "SELL"}

        buy_id = state.side_orders["BUY"].order_id
        sell_id = state.side_orders["SELL"].order_id
        assert buy_id is not None and sell_id is not None and buy_id != sell_id

        # Fill the bid: the ask must stay live and position must reconcile.
        await state.order_updates.put(
            {"e": "ORDER_TRADE_UPDATE", "o": {"i": buy_id, "X": "FILLED", "z": "2.0"}}
        )
        for _ in range(100):
            if state.side_orders["BUY"].order_id is None:
                break
            await asyncio.sleep(0.01)

        assert state.side_orders["BUY"].order_id is None
        assert state.side_orders["SELL"].order_id == sell_id
        assert state.position_size == 0.5  # via REST fallback

        market_maker.publish_latest_order_command(
            state, market_maker.QuoteSetCommand(kind="cancel_all", trigger="test shutdown")
        )
        for _ in range(100):
            if client.cancel_all_calls == 1:
                break
            await asyncio.sleep(0.01)
        assert client.cancel_all_calls == 1
        assert state.side_orders["SELL"].order_id is None
        assert sell_id in state.pending_terminal_orders

        runtime.request_shutdown()
        # Wake the manager so it observes the shutdown flag.
        await state.order_updates.put({"e": "PING"})
        await asyncio.wait_for(task, timeout=2.0)

    asyncio.run(runner())


def test_order_manager_fast_replace_moves_old_order_to_pending(monkeypatch):
    class ReplacingClient:
        def __init__(self):
            self.placed = []
            self.canceled = []
            self.next_order_id = 200

        async def place_order(self, symbol, price, quantity, side, reduce_only=False):
            self.next_order_id += 1
            self.placed.append((side, float(price)))
            return {"orderId": self.next_order_id}

        async def cancel_order(self, symbol, order_id):
            self.canceled.append(order_id)
            return {"orderId": order_id}

    async def runner():
        monkeypatch.setattr(market_maker, "MIN_ORDER_INTERVAL", 0.0)
        state = market_maker.StrategyState()
        state.mid_price = 100.0
        runtime = _make_runtime()
        client = ReplacingClient()
        executor = market_maker.OrderExecutor(fast_replace=True)
        log = logging.getLogger("test")

        # Seed a live bid, then apply a quote set that moves it >5bps.
        state.side_orders["BUY"] = market_maker.SideOrderState(
            order_id=201, price=99.0, quantity=2.0, placed_at=9.0
        )
        command = market_maker.QuoteSetCommand(
            kind="quote_set",
            bid=market_maker.SideQuote(side="BUY", price=99.5, quantity=2.0),
            ask=None,
            trigger="test",
        )

        await market_maker.apply_quote_set(state, client, "BTCUSDT", runtime, log, executor, command)

        assert client.canceled == [201]
        assert 201 in state.pending_terminal_orders
        assert state.pending_terminal_orders[201].side == "BUY"
        assert state.side_orders["BUY"].order_id == client.next_order_id
        assert state.side_orders["BUY"].price == 99.5

    asyncio.run(runner())


def test_apply_quote_set_reuses_order_within_threshold(monkeypatch):
    class NoTouchClient:
        async def place_order(self, *args, **kwargs):
            raise AssertionError("order should have been reused")

        async def cancel_order(self, *args, **kwargs):
            raise AssertionError("order should not be cancelled")

    async def runner():
        monkeypatch.setattr(market_maker, "MIN_ORDER_INTERVAL", 0.0)
        state = market_maker.StrategyState()
        runtime = _make_runtime()
        executor = market_maker.OrderExecutor(fast_replace=True)
        log = logging.getLogger("test")

        state.side_orders["SELL"] = market_maker.SideOrderState(
            order_id=300, price=100.5, quantity=2.0, placed_at=9.0
        )
        command = market_maker.QuoteSetCommand(
            kind="quote_set",
            bid=None,
            ask=market_maker.SideQuote(side="SELL", price=100.51, quantity=2.0),  # ~1bps move
            trigger="test",
        )

        await market_maker.apply_quote_set(state, NoTouchClient(), "BTCUSDT", runtime, log, executor, command)

        assert state.side_orders["SELL"].order_id == 300

    asyncio.run(runner())


# ---------------------------------------------------------------------------
# Cancels, fills, shutdown
# ---------------------------------------------------------------------------

def test_wait_for_terminal_order_update_requeues_other_orders_events():
    """A fill on the OTHER side must survive a cancel-confirmation wait."""
    async def runner():
        queue = asyncio.Queue()
        other_fill = {"e": "ORDER_TRADE_UPDATE", "o": {"i": 99, "X": "FILLED", "z": "1.0"}}
        await queue.put(other_fill)
        await queue.put({"e": "ORDER_TRADE_UPDATE", "o": {"i": 42, "X": "CANCELED", "z": "0"}})

        terminal = await market_maker.wait_for_terminal_order_update(
            queue, 42, 1.0, logging.getLogger("test"), "Test"
        )

        assert terminal["status"] == "CANCELED"
        requeued = queue.get_nowait()
        assert requeued["o"]["i"] == 99  # other side's fill is back in the queue

    asyncio.run(runner())


def test_timeout_refresh_parks_expired_side_without_touching_other(monkeypatch):
    """Fast-replace timeout refresh must not block on the queue or disturb the other side."""
    class CancelClient:
        def __init__(self):
            self.cancelled = []

        async def cancel_order(self, symbol, order_id):
            self.cancelled.append(order_id)
            return {"orderId": order_id}

        async def get_order_status(self, symbol, order_id):
            raise AssertionError("should not need REST status during fast refresh")

    async def runner():
        monkeypatch.setattr(market_maker, "MIN_ORDER_INTERVAL", 0.0)
        state = market_maker.StrategyState()
        runtime = _make_runtime(clock_value=100.0)
        client = CancelClient()
        log = logging.getLogger("test")

        state.side_orders["BUY"] = market_maker.SideOrderState(
            order_id=10, price=99.0, quantity=1.0, placed_at=10.0  # 90s old -> expired
        )
        state.side_orders["SELL"] = market_maker.SideOrderState(
            order_id=11, price=101.0, quantity=1.0, placed_at=99.0  # fresh
        )

        ok = await market_maker.fast_cancel_side_to_pending(
            state, client, "BTCUSDT", runtime, log, "BUY", "Timed-out order refresh", "Timed-out order"
        )

        assert ok is True
        assert client.cancelled == [10]
        assert state.side_orders["BUY"].order_id is None
        assert 10 in state.pending_terminal_orders
        assert state.pending_terminal_orders[10].side == "BUY"
        assert state.side_orders["SELL"].order_id == 11  # untouched

    asyncio.run(runner())


def test_placement_failure_on_one_side_does_not_kill_the_other(monkeypatch):
    class HalfFailingClient:
        def __init__(self):
            self.placed = []

        async def place_order(self, symbol, price, quantity, side, reduce_only=False):
            if side == "BUY":
                raise RuntimeError("GTX reject")
            self.placed.append(side)
            return {"orderId": 500}

    async def runner():
        monkeypatch.setattr(market_maker, "MIN_ORDER_INTERVAL", 0.0)
        state = market_maker.StrategyState()
        state.mid_price = 100.0
        runtime = _make_runtime()
        executor = market_maker.OrderExecutor(fast_replace=True)
        log = logging.getLogger("test")

        command = market_maker.QuoteSetCommand(
            kind="quote_set",
            bid=market_maker.SideQuote(side="BUY", price=99.5, quantity=2.0),
            ask=market_maker.SideQuote(side="SELL", price=100.5, quantity=2.0),
            trigger="test",
        )

        # Must not raise out of apply_quote_set.
        await market_maker.apply_quote_set(
            state, HalfFailingClient(), "BTCUSDT", runtime, log, executor, command
        )

        assert state.side_orders["SELL"].order_id == 500   # healthy side placed
        assert state.side_orders["BUY"].order_id is None   # failed side empty
        assert len(state.order_failure_timestamps) == 1    # failure recorded
        assert state.quote_refresh_event.is_set()          # retry requested

    asyncio.run(runner())


def test_wait_for_terminal_order_update_ignores_non_terminal_events():
    async def runner():
        queue = asyncio.Queue()
        await queue.put({"e": "ORDER_TRADE_UPDATE", "o": {"i": 42, "X": "PARTIALLY_FILLED", "z": "0.05", "ap": "100"}})
        await queue.put({"e": "ORDER_TRADE_UPDATE", "o": {"i": 42, "X": "FILLED", "z": "0.2", "ap": "100"}})

        terminal = await market_maker.wait_for_terminal_order_update(
            queue,
            42,
            1.0,
            logging.getLogger("test"),
            "Test",
        )

        assert terminal["status"] == "FILLED"
        assert terminal["treat_as_fill"] is True

    asyncio.run(runner())


def test_wait_for_terminal_order_update_handles_partial_then_canceled_fill():
    async def runner():
        queue = asyncio.Queue()
        await queue.put({"e": "ORDER_TRADE_UPDATE", "o": {"i": 42, "X": "PARTIALLY_FILLED", "z": "0.05", "ap": "100"}})
        await queue.put({"e": "ORDER_TRADE_UPDATE", "o": {"i": 42, "X": "CANCELED", "z": "0.2", "ap": "100"}})

        terminal = await market_maker.wait_for_terminal_order_update(
            queue,
            42,
            1.0,
            logging.getLogger("test"),
            "Test",
        )

        assert terminal["status"] == "CANCELED"
        assert terminal["treat_as_fill"] is True
        assert terminal["filled_qty"] == 0.2

    asyncio.run(runner())


def test_cancel_and_finalize_side_order_reconciles_via_rest_after_cancel_timeout(monkeypatch):
    class DummyClient:
        def __init__(self):
            self.cancel_calls = 0
            self.status_calls = 0
            self.position_calls = 0

        async def cancel_order(self, symbol, order_id):
            self.cancel_calls += 1
            return {"orderId": order_id}

        async def get_order_status(self, symbol, order_id):
            self.status_calls += 1
            return {"status": "CANCELED", "executedQty": "0.2"}

        async def get_position_risk(self, symbol):
            self.position_calls += 1
            return [{"positionAmt": "0.2", "notional": "20.0"}]

    async def runner():
        state = market_maker.StrategyState()
        state.mid_price = 100.0
        state.side_orders["BUY"] = market_maker.SideOrderState(
            order_id=42, price=100.0, quantity=0.2, placed_at=9.0
        )
        client = DummyClient()

        monkeypatch.setattr(market_maker, "CANCEL_CONFIRM_TIMEOUT", 0.01)
        monkeypatch.setattr(market_maker, "POSITION_SYNC_TIMEOUT", 0.01)

        success = await market_maker.cancel_and_finalize_side_order(
            state,
            client,
            "BTCUSDT",
            logging.getLogger("test"),
            "BUY",
            "Timed-out order refresh",
            "Timed-out order",
        )

        assert success is True
        assert client.cancel_calls == 1
        assert client.status_calls == 1
        assert client.position_calls == 1
        assert state.side_orders["BUY"].order_id is None
        assert state.position_size == 0.2

    asyncio.run(runner())


def test_reconcile_fill_with_position_waits_for_ws_snapshot():
    class DummyClient:
        async def get_position_risk(self, symbol):
            raise AssertionError("REST fallback should not be used")

    async def runner():
        state = market_maker.StrategyState()
        state.mid_price = 100.0
        previous_seq = state.position_update_seq

        async def push_position_update():
            await asyncio.sleep(0.05)
            market_maker.apply_position_snapshot(state, 0.2)

        task = asyncio.create_task(push_position_update())
        synced_via_ws = await market_maker.reconcile_fill_with_position(
            state,
            DummyClient(),
            "BTCUSDT",
            logging.getLogger("test"),
            previous_seq,
            "Test fill",
        )
        await task

        assert synced_via_ws is True
        assert state.position_size == 0.2

    asyncio.run(runner())


def test_reconcile_fill_with_position_falls_back_to_rest():
    class DummyClient:
        async def get_position_risk(self, symbol):
            return [{"positionAmt": "0.2", "notional": "20.0"}]

    async def runner():
        state = market_maker.StrategyState()
        state.mid_price = 100.0
        previous_seq = state.position_update_seq

        synced_via_ws = await market_maker.reconcile_fill_with_position(
            state,
            DummyClient(),
            "BTCUSDT",
            logging.getLogger("test"),
            previous_seq,
            "Test fill",
        )

        assert synced_via_ws is False
        assert state.position_size == 0.2

    asyncio.run(runner())


def test_ensure_clean_startup_fails_closed_on_cancel_error():
    class FailingClient:
        async def cancel_all_orders(self, symbol):
            raise RuntimeError("boom")

    async def runner():
        success = await market_maker.ensure_clean_startup(FailingClient(), "BTCUSDT", timeout=0.01)
        assert success is False

    asyncio.run(runner())


def test_cancel_side_order_failure_preserves_tracking():
    class FailingClient:
        async def cancel_order(self, symbol, order_id):
            raise RuntimeError("boom")

    async def runner():
        state = market_maker.StrategyState()
        state.side_orders["BUY"] = market_maker.SideOrderState(order_id=42, price=100.0, quantity=0.1)

        success = await market_maker.cancel_side_order(
            state,
            FailingClient(),
            "BTCUSDT",
            logging.getLogger("test"),
            "BUY",
            "Test cancel",
        )

        assert success is False
        assert state.side_orders["BUY"].order_id == 42
        assert state.side_orders["BUY"].price == 100.0

    asyncio.run(runner())


def test_cancel_side_order_success_clears_tracking():
    class SuccessfulClient:
        async def cancel_order(self, symbol, order_id):
            return {"orderId": order_id}

    async def runner():
        state = market_maker.StrategyState()
        state.side_orders["BUY"] = market_maker.SideOrderState(order_id=42, price=100.0, quantity=0.1)

        success = await market_maker.cancel_side_order(
            state,
            SuccessfulClient(),
            "BTCUSDT",
            logging.getLogger("test"),
            "BUY",
            "Test cancel",
        )

        assert success is True
        assert state.side_orders["BUY"].order_id is None
        assert state.side_orders["BUY"].price is None

    asyncio.run(runner())


def test_initiate_graceful_order_shutdown_requests_cancel_all_and_waits_for_clear():
    async def runner():
        state = market_maker.StrategyState()
        state.side_orders["BUY"] = market_maker.SideOrderState(order_id=42, price=99.0, quantity=1.0)
        state.side_orders["SELL"] = market_maker.SideOrderState(order_id=43, price=101.0, quantity=1.0)
        runtime = market_maker.RuntimeContext("BTCUSDT")

        async def clear_soon():
            await asyncio.sleep(0.05)
            market_maker.clear_side_order(state, "BUY")
            market_maker.clear_side_order(state, "SELL")

        task = asyncio.create_task(clear_soon())
        cleared = await market_maker.initiate_graceful_order_shutdown(state, runtime, timeout=0.5)
        await task

        assert cleared is True
        command = state.order_commands.get_nowait()
        assert command.kind == "cancel_all"
        assert command.trigger == "Shutdown cleanup"

    asyncio.run(runner())


def test_cleanup_orders_prefers_existing_client():
    class DummySession:
        closed = False

    class DummyClient:
        def __init__(self):
            self.session = DummySession()
            self.cancel_calls = 0

        async def cancel_all_orders(self, symbol):
            self.cancel_calls += 1
            return {"symbol": symbol}

    async def runner():
        client = DummyClient()
        success = await market_maker.cleanup_orders(
            "BTCUSDT",
            "0x0000000000000000000000000000000000000000",
            "0x0000000000000000000000000000000000000000",
            "dummy",
            existing_client=client,
            timeout=0.1,
        )
        assert success is True
        assert client.cancel_calls == 1

    asyncio.run(runner())


def test_user_data_idle_timeout_does_not_reconnect(monkeypatch):
    class DummyClient:
        def __init__(self):
            self.listen_key_requests = 0

        async def create_listen_key(self):
            self.listen_key_requests += 1
            return {"listenKey": "dummy-listen-key"}

    class DummyWebSocket:
        async def recv(self):
            await asyncio.sleep(3600)

    class DummyConnection:
        def __init__(self, websocket):
            self.websocket = websocket

        async def __aenter__(self):
            return self.websocket

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_keepalive(state, client, runtime):
        await asyncio.Event().wait()

    wait_calls = {"count": 0}

    async def fake_wait_for(awaitable, timeout):
        wait_calls["count"] += 1
        awaitable.close()
        if wait_calls["count"] >= 2:
            runtime.request_shutdown()
        raise asyncio.TimeoutError

    state = market_maker.StrategyState()
    client = DummyClient()
    runtime = market_maker.RuntimeContext("BTCUSDT", clock=lambda: 61.0)

    monkeypatch.setattr(market_maker, "keepalive_balance_listen_key", fake_keepalive)
    monkeypatch.setattr(market_maker.websockets, "connect", lambda *args, **kwargs: DummyConnection(DummyWebSocket()))
    monkeypatch.setattr(market_maker.asyncio, "wait_for", fake_wait_for)

    asyncio.run(market_maker.websocket_user_data_updater(state, client, "BTCUSDT", runtime))

    assert client.listen_key_requests == 1
    assert wait_calls["count"] == 2


def test_wait_for_startup_inputs_waits_for_vol_obi_warmup(monkeypatch):
    state = market_maker.StrategyState()
    runtime = market_maker.RuntimeContext("ETHUSDT", clock=lambda: 10.0)

    async def fake_sleep(_seconds):
        state.vol_obi_snapshot = market_maker.VolObiSnapshot(
            warmed_up=True,
            ws_connected=True,
            last_updated=9.5,
        )

    monkeypatch.setattr(market_maker.asyncio, "sleep", fake_sleep)

    result = asyncio.run(market_maker.wait_for_startup_inputs(state, "ETHUSDT", runtime))

    assert result is True
