from market_maker import (
    DEFAULT_PRICE_CHANGE_THRESHOLD,
    StrategyState,
    should_reuse_order,
)


def build_state(price=100.0, side="BUY", quantity=1.0):
    state = StrategyState()
    state.active_order_id = 12345
    state.last_order_price = price
    state.last_order_side = side
    state.last_order_quantity = quantity
    return state


def test_reuses_order_below_one_bp_threshold():
    state = build_state()

    new_price = state.last_order_price * (1 + DEFAULT_PRICE_CHANGE_THRESHOLD * 0.9)
    assert should_reuse_order(state, new_price, "BUY", 1.0)


def test_refreshes_order_at_one_bp_threshold():
    state = build_state()

    new_price = state.last_order_price * (1 + DEFAULT_PRICE_CHANGE_THRESHOLD)
    assert not should_reuse_order(state, new_price, "BUY", 1.0)


def test_reuse_requires_matching_side_quantity_and_tracking():
    state = build_state()

    assert not should_reuse_order(state, 100.0, "SELL", 1.0)
    assert not should_reuse_order(state, 100.0, "BUY", 1.1)

    state.active_order_id = None
    assert not should_reuse_order(state, 100.0, "BUY", 1.0)
