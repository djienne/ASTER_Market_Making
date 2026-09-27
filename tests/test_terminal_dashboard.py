import asyncio
import types

import terminal_dashboard as td


def _bare_dashboard(stop_event):
    dashboard = td.TerminalDashboard.__new__(td.TerminalDashboard)
    dashboard.stop_event = stop_event
    dashboard.last_reason = ""
    dashboard.render = lambda *args, **kwargs: None
    return dashboard


def test_dashboard_keeps_running_without_spot_keys():
    async def runner():
        stop_event = asyncio.Event()
        dashboard = _bare_dashboard(stop_event)
        dashboard.spot_fetcher = None
        dashboard.mark_stream_event = asyncio.Event()

        async def run_until_stopped(*args):
            await stop_event.wait()

        dashboard.periodic_refresh = run_until_stopped
        dashboard.mark_price_listener = run_until_stopped
        dashboard.stream = run_until_stopped
        dashboard.listen_key_keepalive = run_until_stopped
        loop = asyncio.get_running_loop()
        started = loop.time()
        await td._run_dashboard_tasks(dashboard, None, stop_event, types.SimpleNamespace(duration=0.3))
        return loop.time() - started

    # Previously the spot worker returned at once and FIRST_COMPLETED exited in ~1 ms.
    assert asyncio.run(runner()) >= 0.25


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
