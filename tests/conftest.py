import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


if os.getenv("RUN_LIVE_API_TESTS") != "1":
    collect_ignore_glob = [
        "test_account_balance.py",
        "test_balance.py",
        "test_cancel_order.py",
        "test_listen_key.py",
        "test_user_stream_step_by_step.py",
        "test_websocket_simple.py",
    ]
