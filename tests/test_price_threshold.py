from market_maker import (
    DEFAULT_PRICE_CHANGE_THRESHOLD,
    SideOrderState,
    should_reuse_side,
)


def build_side_state(price=100.0, quantity=1.0):
    return SideOrderState(order_id=12345, price=price, quantity=quantity)


def test_reuses_order_below_configured_threshold():
    side_state = build_side_state()

    new_price = side_state.price * (1 + DEFAULT_PRICE_CHANGE_THRESHOLD * 0.9)
    assert should_reuse_side(side_state, new_price)


def test_refreshes_order_at_configured_threshold():
    side_state = build_side_state()

    new_price = side_state.price * (1 + DEFAULT_PRICE_CHANGE_THRESHOLD * 1.0001)
    assert not should_reuse_side(side_state, new_price)


def test_reuse_requires_live_tracked_order():
    assert not should_reuse_side(SideOrderState(), 100.0)
    assert not should_reuse_side(SideOrderState(order_id=1, price=None), 100.0)
