import asyncio
import types

import terminal_dashboard as td


def _bare_dashboard(stop_event):
    dashboard = td.TerminalDashboard.__new__(td.TerminalDashboard)
    dashboard.stop_event = stop_event
    dashboard.last_reason = ""
    dashboard.render = lambda *args, **kwargs: None
    return dashboard


def test_dashboard_keeps_running_until_duration_elapses():
    async def runner():
        stop_event = asyncio.Event()
        dashboard = _bare_dashboard(stop_event)
        dashboard.mark_stream_event = asyncio.Event()
        clients = []

        async def run_until_stopped(*args):
            clients.append(args)
            await stop_event.wait()

        dashboard.periodic_refresh = run_until_stopped
        dashboard.mark_price_listener = run_until_stopped
        dashboard.stream = run_until_stopped
        dashboard.listen_key_keepalive = run_until_stopped
        loop = asyncio.get_running_loop()
        started = loop.time()
        client = object()
        await td._run_dashboard_tasks(dashboard, client, stop_event, types.SimpleNamespace(duration=0.3))
        return loop.time() - started, client, clients

    elapsed, client, clients = asyncio.run(runner())
    # The dashboard ends with FIRST_COMPLETED, so any task that returned at once
    # (as the old optional spot worker did without keys) would exit in ~1 ms.
    assert elapsed >= 0.25
    # periodic_refresh, stream and listen_key_keepalive share the one long-lived client.
    assert sorted(len(args) for args in clients) == [0, 1, 1, 1]
    assert all(args[0] is client for args in clients if args)


def test_user_stream_reconnects_with_a_fresh_listen_key(monkeypatch):
    async def runner():
        stop_event = asyncio.Event()
        dashboard = _bare_dashboard(stop_event)
        listen_keys = []

        class Client:
            async def create_listen_key(self):
                listen_keys.append(f"key-{len(listen_keys)}")
                if len(listen_keys) == 3:
                    stop_event.set()
                return {"listenKey": listen_keys[-1]}

        class ExpiringStream:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def recv(self):
                return '{"e": "listenKeyExpired"}'

        monkeypatch.setattr(td, "USER_STREAM_RETRY", 0)
        monkeypatch.setattr(td.websockets, "connect", lambda url: ExpiringStream())
        await asyncio.wait_for(dashboard.stream(Client()), timeout=5)
        return listen_keys

    assert asyncio.run(runner()) == ["key-0", "key-1", "key-2"]


def test_mark_stream_reconnects_after_an_error_without_a_symbol_change(monkeypatch):
    async def runner():
        stop_event = asyncio.Event()
        dashboard = _bare_dashboard(stop_event)
        dashboard.mark_stream_event = asyncio.Event()
        dashboard.mark_stream_event.set()
        dashboard.mark_symbols = {"ETHUSDT"}
        dashboard.mark_prices = {}
        attempts = []

        def failing_connect(url):
            attempts.append(url)
            if len(attempts) == 2:
                stop_event.set()
            raise OSError("network down")

        monkeypatch.setattr(td, "MARK_STREAM_RETRY", 0)
        monkeypatch.setattr(td.websockets, "connect", failing_connect)
        # Before the fix the listener parked on mark_stream_event after the
        # first failure, so mark prices froze until the symbol set changed.
        await asyncio.wait_for(dashboard.mark_price_listener(), timeout=2)
        return attempts

    assert len(asyncio.run(runner())) == 2
