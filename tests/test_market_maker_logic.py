import asyncio
import logging

import market_maker


def _mark_binance_alpha_ready(state):
    state.binance_alpha_ws_connected = True
    state.binance_alpha_ready = True
    state.binance_alpha_shift_bps = 0.0
    state.binance_alpha_warmup_seconds = market_maker.BINANCE_OBI_WARMUP_SECONDS


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


def test_classify_order_update_keeps_small_partial_open():
    partial = market_maker.classify_order_update({"X": "PARTIALLY_FILLED", "z": "0.05", "ap": "100"})

    assert partial["is_terminal"] is False
    assert partial["treat_as_fill"] is False


def test_classify_order_update_keeps_large_partial_open_until_terminal():
    partial = market_maker.classify_order_update({"X": "PARTIALLY_FILLED", "z": "0.2", "ap": "100"})

    assert partial["is_terminal"] is False
    assert partial["treat_as_fill"] is False


def test_apply_fill_to_state_uses_position_snapshot_source_of_truth():
    state = market_maker.StrategyState(flip_mode=False)
    state.mid_price = 100.0
    state.position_size = 0.2
    state.mode = "BUY"

    previous_mode, new_mode = market_maker.apply_fill_to_state(state, "BUY", 0.2)
    assert previous_mode == "BUY"
    assert new_mode == "SELL"
    assert state.position_size == 0.2

    state = market_maker.StrategyState(flip_mode=False)
    state.mid_price = 100.0
    state.position_size = 0.05
    state.mode = "BUY"

    previous_mode, new_mode = market_maker.apply_fill_to_state(state, "BUY", 0.05)
    assert previous_mode == "BUY"
    assert new_mode == "BUY"
    assert state.position_size == 0.05


def test_sync_mode_with_position_respects_updated_flip_mode():
    state = market_maker.StrategyState(flip_mode=False)
    state.mode = "BUY"
    state.mid_price = 100.0
    state.flip_mode = True

    market_maker.sync_mode_with_position(state)

    assert state.mode == "SELL"
    assert state.position_size == 0.0


def test_apply_position_snapshot_preserves_small_residual_position():
    state = market_maker.StrategyState(flip_mode=False)
    state.mid_price = 100.0

    market_maker.apply_position_snapshot(state, 0.05, position_notional=5.0)

    assert state.mode == "BUY"
    assert state.position_size == 0.05


def test_small_residual_position_is_flattened_before_opening_new_inventory():
    state = market_maker.StrategyState(flip_mode=False)
    state.position_size = 0.05
    assert market_maker.get_close_side_for_trading(state) == "SELL"

    state.position_size = -0.05
    assert market_maker.get_close_side_for_trading(state) == "BUY"


def test_significant_inventory_closes_using_position_direction():
    state = market_maker.StrategyState(flip_mode=False)
    state.mid_price = 100.0

    market_maker.apply_position_snapshot(state, -0.2, position_notional=20.0)
    assert state.mode == "BUY"

    state = market_maker.StrategyState(flip_mode=True)
    state.mid_price = 100.0

    market_maker.apply_position_snapshot(state, 0.2, position_notional=20.0)
    assert state.mode == "SELL"


def test_round_price_to_tick_stays_passive_for_each_side():
    assert abs(market_maker.round_price_to_tick(100.06, 0.1, "BUY") - 100.0) < 1e-9
    assert abs(market_maker.round_price_to_tick(100.06, 0.1, "SELL") - 100.1) < 1e-9


def test_round_quantity_to_step_respects_non_decimal_lot_sizes():
    assert abs(market_maker.round_quantity_to_step(1.234, 0.005) - 1.23) < 1e-9
    assert abs(market_maker.round_quantity_to_step(0.0149, 0.005) - 0.01) < 1e-9


def test_clamp_offset_to_spread_limits_uses_runtime_guardrails():
    spread_limits = {"min": 5.0, "max": 200.0}

    low = market_maker.clamp_offset_to_spread_limits(0.000001, 100.0, spread_limits)
    high = market_maker.clamp_offset_to_spread_limits(10.0, 100.0, spread_limits)

    assert abs(low - 0.05) < 1e-9
    assert abs(high - 2.0) < 1e-9


def test_rolling_zscore_buffer_is_bounded_and_evicts_old_samples():
    buffer = market_maker.RollingZScoreBuffer(3)
    buffer.append(1000, 1.0)
    buffer.append(2000, 2.0)
    buffer.append(3000, 3.0)
    buffer.append(4000, 4.0)

    assert buffer.count == 3
    assert abs(buffer.mean() - 3.0) < 1e-9

    buffer.evict_older_than(3500)

    assert buffer.count == 1
    assert abs(buffer.mean() - 4.0) < 1e-9


def test_calculate_binance_orderbook_imbalance_uses_price_band():
    state = market_maker.StrategyState()
    state.binance_bid_book = {100.0: 5.0, 99.0: 4.0, 96.0: 100.0}
    state.binance_ask_book = {101.0: 3.0, 102.0: 2.0, 104.0: 200.0}

    imbalance = market_maker.calculate_binance_orderbook_imbalance(state)

    assert abs(imbalance - ((9.0 - 5.0) / 14.0)) < 1e-9


def test_build_order_plan_applies_binance_shift_bps_to_dynamic_quotes():
    params = {
        "source": "avellaneda_parameters_ETH.json",
        "gamma": 0.05,
        "sigma": 0.01,
        "k_buy": 0.3,
        "k_sell": 0.3,
        "time_horizon_days": 0.005,
        "spread_limits_bps": {"min": 5.0, "max": 200.0},
    }
    state = market_maker.StrategyState(flip_mode=False)
    state.mid_price = 100.0
    state.account_balance = 1000.0

    base_plan = market_maker.build_order_plan(state, "BUY", params)

    state.binance_alpha_ready = True
    state.binance_alpha_shift_bps = 10.0
    shifted_plan = market_maker.build_order_plan(state, "BUY", params)

    assert abs(shifted_plan["reservation_price"] - base_plan["reservation_price"] - 0.1) < 1e-9
    assert abs(shifted_plan["bid_price"] - base_plan["bid_price"] - 0.1) < 1e-9
    assert abs(shifted_plan["ask_price"] - base_plan["ask_price"] - 0.1) < 1e-9


def test_wait_for_startup_inputs_waits_for_binance_alpha(monkeypatch):
    async def fake_sleep(_seconds):
        state.binance_alpha_ready = True

    state = market_maker.StrategyState()
    state.quote_params = {"source": "avellaneda_parameters_ETH.json"}
    state.supertrend_signal = 1
    runtime = market_maker.RuntimeContext("ETHUSDT")

    monkeypatch.setattr(market_maker.asyncio, "sleep", fake_sleep)

    result = asyncio.run(market_maker.wait_for_startup_inputs(state, "ETHUSDT", runtime))

    assert result is True


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
    assert candidate["formatted_price"] == "100.0"
    assert candidate["formatted_quantity"] == "0.010"


def test_quote_engine_emits_requote_command_on_price_move_above_threshold(monkeypatch):
    class DummyClient:
        async def get_symbol_filters(self, symbol):
            return {
                "tick_size": 0.1,
                "price_precision": 1,
                "step_size": 0.001,
                "quantity_precision": 3,
                "min_qty": 0.001,
                "min_notional": 5.0,
            }

    async def runner():
        state = market_maker.StrategyState(flip_mode=False)
        state.price_ws_connected = True
        state.user_data_ws_connected = True
        _mark_binance_alpha_ready(state)
        state.account_balance = 1000.0
        state.balance_last_updated = 1.0
        state.mid_price = 100.0
        state.bid_price = 99.9
        state.ask_price = 100.1
        state.quote_params = {"buy_spread": 0.006, "sell_spread": 0.006, "source": "default"}
        state.active_order_id = 42
        state.last_order_side = "BUY"
        state.last_order_quantity = 2.0
        state.last_order_price = 98.0
        runtime = market_maker.RuntimeContext("BTCUSDT", clock=lambda: 10.0)
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(market_maker.market_making_loop(state, DummyClient(), "BTCUSDT", runtime))
        market_maker.request_quote_refresh(state)

        for _ in range(20):
            if not state.order_commands.empty():
                break
            await asyncio.sleep(0.01)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        command = state.order_commands.get_nowait()
        assert command.kind == "quote"
        assert command.side == "BUY"
        assert command.price == 99.4

    asyncio.run(runner())


def test_quote_engine_does_not_emit_limit_order_without_valid_quotes():
    class DummyClient:
        async def get_symbol_filters(self, symbol):
            return {
                "tick_size": 0.1,
                "price_precision": 1,
                "step_size": 0.001,
                "quantity_precision": 3,
                "min_qty": 0.001,
                "min_notional": 5.0,
            }

    async def runner():
        state = market_maker.StrategyState(flip_mode=False)
        state.price_ws_connected = True
        state.user_data_ws_connected = True
        _mark_binance_alpha_ready(state)
        state.account_balance = 1000.0
        state.balance_last_updated = 1.0
        state.mid_price = 100.0
        state.bid_price = 99.9
        state.ask_price = 100.1
        state.quote_params = {"source": "unavailable"}
        runtime = market_maker.RuntimeContext("BTCUSDT", clock=lambda: 10.0)
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(market_maker.market_making_loop(state, DummyClient(), "BTCUSDT", runtime))
        market_maker.request_quote_refresh(state)
        await asyncio.sleep(0.05)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        assert state.order_commands.empty()

    asyncio.run(runner())


def test_quote_engine_does_not_emit_opening_quote_without_enough_capital():
    class DummyClient:
        async def get_symbol_filters(self, symbol):
            return {
                "status": "TRADING",
                "tick_size": 0.1,
                "price_precision": 1,
                "step_size": 0.001,
                "quantity_precision": 3,
                "min_qty": 0.001,
                "min_notional": 10.0,
            }

    async def runner():
        state = market_maker.StrategyState(flip_mode=False)
        state.price_ws_connected = True
        state.user_data_ws_connected = True
        _mark_binance_alpha_ready(state)
        state.account_balance = 20.0
        state.balance_last_updated = 1.0
        state.mid_price = 100.0
        state.bid_price = 99.9
        state.ask_price = 100.1
        state.quote_params = {"buy_spread": 0.006, "sell_spread": 0.006, "source": "default"}
        runtime = market_maker.RuntimeContext("BTCUSDT", clock=lambda: 10.0)
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(market_maker.market_making_loop(state, DummyClient(), "BTCUSDT", runtime))
        market_maker.request_quote_refresh(state)
        await asyncio.sleep(0.05)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        assert state.order_commands.empty()

    asyncio.run(runner())


def test_quote_engine_cancels_active_order_when_quotes_become_unavailable():
    class DummyClient:
        async def get_symbol_filters(self, symbol):
            return {
                "tick_size": 0.1,
                "price_precision": 1,
                "step_size": 0.001,
                "quantity_precision": 3,
                "min_qty": 0.001,
                "min_notional": 5.0,
            }

    async def runner():
        state = market_maker.StrategyState(flip_mode=False)
        state.price_ws_connected = True
        state.user_data_ws_connected = True
        _mark_binance_alpha_ready(state)
        state.account_balance = 1000.0
        state.balance_last_updated = 1.0
        state.mid_price = 100.0
        state.bid_price = 99.9
        state.ask_price = 100.1
        state.active_order_id = 42
        state.last_order_side = "BUY"
        state.last_order_quantity = 2.0
        state.last_order_price = 99.0
        state.quote_params = {"source": "unavailable"}
        runtime = market_maker.RuntimeContext("BTCUSDT", clock=lambda: 10.0)
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(market_maker.market_making_loop(state, DummyClient(), "BTCUSDT", runtime))
        market_maker.request_quote_refresh(state)

        for _ in range(20):
            if not state.order_commands.empty():
                break
            await asyncio.sleep(0.01)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        command = state.order_commands.get_nowait()
        assert command.kind == "cancel"
        assert "Quotes unavailable" in command.trigger

    asyncio.run(runner())


def test_quote_engine_cancels_active_order_when_symbol_is_not_trading():
    class DummyClient:
        async def get_symbol_filters(self, symbol):
            return {
                "status": "HALT",
                "tick_size": 0.1,
                "price_precision": 1,
                "step_size": 0.001,
                "quantity_precision": 3,
                "min_qty": 0.001,
                "min_notional": 5.0,
            }

    async def runner():
        state = market_maker.StrategyState(flip_mode=False)
        state.price_ws_connected = True
        state.user_data_ws_connected = True
        _mark_binance_alpha_ready(state)
        state.account_balance = 1000.0
        state.balance_last_updated = 1.0
        state.mid_price = 100.0
        state.bid_price = 99.9
        state.ask_price = 100.1
        state.active_order_id = 42
        state.last_order_side = "BUY"
        state.last_order_quantity = 2.0
        state.last_order_price = 99.0
        state.quote_params = {"buy_spread": 0.006, "sell_spread": 0.006, "source": "default"}
        runtime = market_maker.RuntimeContext("BTCUSDT", clock=lambda: 10.0)
        runtime.price_last_updated = 10.0

        task = asyncio.create_task(market_maker.market_making_loop(state, DummyClient(), "BTCUSDT", runtime))
        market_maker.request_quote_refresh(state)

        for _ in range(20):
            if not state.order_commands.empty():
                break
            await asyncio.sleep(0.01)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        command = state.order_commands.get_nowait()
        assert command.kind == "cancel"
        assert "Symbol status HALT" in command.trigger

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


def test_cancel_and_finalize_active_order_reconciles_via_rest_after_cancel_timeout(monkeypatch):
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
        state.active_order_id = 42
        state.last_order_side = "BUY"
        state.last_order_price = 100.0
        state.last_order_quantity = 0.2
        client = DummyClient()

        monkeypatch.setattr(market_maker, "CANCEL_CONFIRM_TIMEOUT", 0.01)
        monkeypatch.setattr(market_maker, "POSITION_SYNC_TIMEOUT", 0.01)

        success = await market_maker.cancel_and_finalize_active_order(
            state,
            client,
            "BTCUSDT",
            logging.getLogger("test"),
            "Timed-out order refresh",
            "Timed-out order",
        )

        assert success is True
        assert client.cancel_calls == 1
        assert client.status_calls == 1
        assert client.position_calls == 1
        assert state.active_order_id is None
        assert state.position_size == 0.2
        assert state.mode == "SELL"

    asyncio.run(runner())


def test_order_manager_requotes_active_order_before_timeout(monkeypatch):
    class DummyClient:
        def __init__(self, state, runtime):
            self.state = state
            self.runtime = runtime
            self.placed = []
            self.canceled = []

        async def place_order(self, symbol, price, quantity, side, reduce_only=False):
            self.placed.append((price, quantity, side, reduce_only))
            order_id = 100 + len(self.placed)

            if len(self.placed) == 1:
                async def push_requote():
                    await asyncio.sleep(0.01)
                    market_maker.publish_latest_order_command(
                        self.state,
                        market_maker.OrderCommand(
                            kind="quote",
                            side="BUY",
                            reduce_only=False,
                            price=99.4,
                            quantity=2.0,
                            formatted_price="99.4",
                            formatted_quantity="2.000",
                            order_notional=198.8,
                            trigger="price",
                        ),
                    )

                asyncio.create_task(push_requote())
            else:
                self.runtime.request_shutdown()

            return {"orderId": order_id}

        async def cancel_order(self, symbol, order_id):
            self.canceled.append(order_id)

            async def push_terminal():
                await asyncio.sleep(0.01)
                await self.state.order_updates.put(
                    {"e": "ORDER_TRADE_UPDATE", "o": {"i": order_id, "X": "CANCELED", "z": "0"}}
                )

            asyncio.create_task(push_terminal())
            return {"orderId": order_id}

    async def runner():
        monkeypatch.setattr(market_maker, "MIN_ORDER_INTERVAL", 0.0)
        state = market_maker.StrategyState()
        state.mid_price = 100.0
        runtime = market_maker.RuntimeContext("BTCUSDT", clock=lambda: 10.0)
        client = DummyClient(state, runtime)

        market_maker.publish_latest_order_command(
            state,
            market_maker.OrderCommand(
                kind="quote",
                side="BUY",
                reduce_only=False,
                price=99.0,
                quantity=2.0,
                formatted_price="99.0",
                formatted_quantity="2.000",
                order_notional=198.0,
                trigger="startup",
            ),
        )

        await market_maker.order_manager_loop(state, client, "BTCUSDT", runtime)

        assert len(client.placed) == 2
        assert client.canceled == [101]
        assert state.active_order_id == 102
        assert state.last_order_price == 99.4

    asyncio.run(runner())


def test_opening_circuit_breaker_blocks_new_opening_quotes():
    class DummyClient:
        async def get_symbol_filters(self, symbol):
            return {
                "status": "TRADING",
                "tick_size": 0.1,
                "price_precision": 1,
                "step_size": 0.001,
                "quantity_precision": 3,
                "min_qty": 0.001,
                "min_notional": 5.0,
            }

    async def runner():
        state = market_maker.StrategyState(flip_mode=False)
        state.price_ws_connected = True
        state.user_data_ws_connected = True
        _mark_binance_alpha_ready(state)
        state.account_balance = 1000.0
        state.balance_last_updated = 1.0
        state.mid_price = 100.0
        state.bid_price = 99.9
        state.ask_price = 100.1
        runtime = market_maker.RuntimeContext("BTCUSDT", clock=lambda: 10.0)
        runtime.price_last_updated = 10.0
        state.opening_circuit_breaker_until = 20.0
        state.quote_params = {"buy_spread": 0.006, "sell_spread": 0.006, "source": "default"}

        task = asyncio.create_task(market_maker.market_making_loop(state, DummyClient(), "BTCUSDT", runtime))
        market_maker.request_quote_refresh(state)
        await asyncio.sleep(0.05)

        runtime.request_shutdown()
        market_maker.request_quote_refresh(state)
        await task

        assert state.order_commands.empty()

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


def test_supertrend_signal_updater_holds_previous_signal_on_invalid_payload(monkeypatch):
    async def fake_sleep(_seconds):
        runtime.request_shutdown()

    state = market_maker.StrategyState()
    state.supertrend_signal = -1
    runtime = market_maker.RuntimeContext("BTCUSDT")

    def fail_load(_symbol):
        raise ValueError("bad signal")

    monkeypatch.setattr(market_maker, "load_supertrend_signal", fail_load)
    monkeypatch.setattr(market_maker.asyncio, "sleep", fake_sleep)

    asyncio.run(market_maker.supertrend_signal_updater(state, "BTCUSDT", runtime))

    assert state.supertrend_signal == -1


def test_reconcile_fill_with_position_waits_for_ws_snapshot():
    class DummyClient:
        async def get_position_risk(self, symbol):
            raise AssertionError("REST fallback should not be used")

    async def runner():
        state = market_maker.StrategyState(flip_mode=False)
        state.mid_price = 100.0
        previous_seq = state.position_update_seq

        async def push_position_update():
            await asyncio.sleep(0.05)
            market_maker.apply_position_snapshot(state, 0.2, position_notional=20.0)

        task = asyncio.create_task(push_position_update())
        synced_via_ws, previous_mode, new_mode = await market_maker.reconcile_fill_with_position(
            state,
            DummyClient(),
            "BTCUSDT",
            logging.getLogger("test"),
            previous_seq,
            "Test fill",
        )
        await task

        assert synced_via_ws is True
        assert previous_mode == "BUY"
        assert new_mode == "SELL"
        assert state.position_size == 0.2

    asyncio.run(runner())


def test_reconcile_fill_with_position_falls_back_to_rest():
    class DummyClient:
        async def get_position_risk(self, symbol):
            return [{"positionAmt": "0.2", "notional": "20.0"}]

    async def runner():
        state = market_maker.StrategyState(flip_mode=False)
        state.mid_price = 100.0
        previous_seq = state.position_update_seq

        synced_via_ws, previous_mode, new_mode = await market_maker.reconcile_fill_with_position(
            state,
            DummyClient(),
            "BTCUSDT",
            logging.getLogger("test"),
            previous_seq,
            "Test fill",
        )

        assert synced_via_ws is False
        assert previous_mode == "BUY"
        assert new_mode == "SELL"
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


def test_cancel_active_order_failure_preserves_tracking():
    class FailingClient:
        async def cancel_order(self, symbol, order_id):
            raise RuntimeError("boom")

    async def runner():
        state = market_maker.StrategyState()
        state.active_order_id = 42
        state.last_order_price = 100.0
        state.last_order_side = "BUY"
        state.last_order_quantity = 0.1

        success = await market_maker.cancel_active_order(
            state,
            FailingClient(),
            "BTCUSDT",
            logging.getLogger("test"),
            "Test cancel",
        )

        assert success is False
        assert state.active_order_id == 42
        assert state.last_order_price == 100.0

    asyncio.run(runner())


def test_cancel_active_order_success_clears_tracking():
    class SuccessfulClient:
        async def cancel_order(self, symbol, order_id):
            return {"orderId": order_id}

    async def runner():
        state = market_maker.StrategyState()
        state.active_order_id = 42
        state.last_order_price = 100.0
        state.last_order_side = "BUY"
        state.last_order_quantity = 0.1

        success = await market_maker.cancel_active_order(
            state,
            SuccessfulClient(),
            "BTCUSDT",
            logging.getLogger("test"),
            "Test cancel",
        )

        assert success is True
        assert state.active_order_id is None
        assert state.last_order_price is None

    asyncio.run(runner())
