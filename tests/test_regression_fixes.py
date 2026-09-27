"""Regression tests for bugs fixed in the Round 1/Round 2 review."""
import api_client
from api_client import ApiClient


def _make_client():
    return ApiClient(
        "0x0000000000000000000000000000000000000001",
        "0x0000000000000000000000000000000000000002",
        "0x" + ("11" * 32),
    )


def test_nonce_stays_monotonic_when_wall_clock_jumps_backward(monkeypatch):
    """NTP steps / DST adjustments must not produce equal or decreasing nonces."""
    client = _make_client()

    # Seed with a realistic microsecond value, then force the clock 1 second back.
    times = iter([1_700_000_000_000_000, 1_699_999_999_000_000])
    monkeypatch.setattr(api_client.time, "time_ns", lambda: next(times) * 1000)

    n1 = client._next_nonce()
    n2 = client._next_nonce()
    assert n2 > n1, "nonce must strictly increase even when time_ns goes backward"


def test_nonce_recovers_from_stale_seed():
    """Historical _last_nonce must not pin future nonces to a stale second."""
    client = _make_client()
    client._last_nonce = 1  # pretend we shipped with a tiny seed
    n1 = client._next_nonce()
    n2 = client._next_nonce()
    assert n1 > 1_000_000_000_000  # should jump to current wall-clock microseconds
    assert n2 > n1


def test_request_error_carries_exchange_body():
    """Rejects must keep the exchange's error body so logs show *why* (e.g. -5022)."""
    import asyncio

    import aiohttp
    from aiohttp import web

    body = '{"code":-5022,"msg":"Due to the order could not be executed as maker"}'

    async def runner():
        app = web.Application()
        app.router.add_post("/order", lambda request: web.Response(status=400, text=body))
        server = web.AppRunner(app)
        await server.setup()
        site = web.TCPSite(server, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        client = _make_client()
        client.session = aiohttp.ClientSession()
        try:
            await client._request_json("POST", f"http://127.0.0.1:{port}/order", {}, {})
        except aiohttp.ClientResponseError as exc:
            return exc
        finally:
            await client.session.close()
            await server.cleanup()

    exc = asyncio.run(runner())
    assert exc is not None and exc.status == 400
    assert "-5022" in str(exc)
