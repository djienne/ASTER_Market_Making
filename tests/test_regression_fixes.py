"""Regression tests for bugs fixed in the Round 1/Round 2 review."""
import importlib.util
import os

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


def _load_module_from_path(name, path):
    """Import a module by file path, bypassing any same-named installed package
    (some site-packages ship a top-level `tests` package that shadows ours)."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_websocket_orders_module_imports_cleanly():
    """Round 1 fixed a NameError at websocket_orders.py:180. The module must
    reach the early-return env-check branch without raising."""
    import asyncio

    saved = {k: os.environ.pop(k, None) for k in ("API_USER", "API_SIGNER", "API_PRIVATE_KEY")}
    try:
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Production module. Its import-time load_dotenv() may repopulate the
        # credentials from a local .env, so pop them again before running the
        # demo: this test must exercise the env-check branch, never go live.
        prod_module = _load_module_from_path(
            "websocket_orders_prod_copy", os.path.join(repo_root, "websocket_orders.py")
        )
        for k in saved:
            os.environ.pop(k, None)
        asyncio.run(prod_module.extended_demo())
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


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
