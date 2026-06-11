"""Non-blocking logging setup (port of the lighter_MM pattern).

Records are routed through a ``QueueHandler`` into a ``QueueListener``
running on a background thread, so log emission never does disk/console
I/O on the caller's thread.  This is critical for the trading hot path:
the asyncio event loop must never block on logging.
"""

import atexit
import logging
import logging.handlers
import queue

# Keep a reference so repeated setup calls don't leak listener threads
# (each owns a daemon thread draining its queue).
_queue_listener = None

_FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def setup_root_logging(*, log_file, release_mode, file_log_level=logging.DEBUG):
    """Configure the root logger with non-blocking file + console handlers.

    In release mode only ERROR records are emitted (both sinks); otherwise
    the file gets *file_log_level* and the console gets INFO.
    """
    global _queue_listener

    root = logging.getLogger()
    root.setLevel(logging.ERROR if release_mode else file_log_level)

    # Rotating handler keeps the log bounded (50MB x 5) on long-running deployments.
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=50_000_000, backupCount=5, encoding='utf-8',
    )
    file_handler.setLevel(logging.ERROR if release_mode else file_log_level)
    file_handler.setFormatter(_FORMATTER)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR if release_mode else logging.INFO)
    console_handler.setFormatter(_FORMATTER)
    # Windows consoles often use cp1252 — render unencodable chars as
    # replacements instead of raising per-record logging errors.
    _stream = getattr(console_handler, 'stream', None)
    if hasattr(_stream, 'reconfigure'):
        try:
            _stream.reconfigure(errors='replace')
        except Exception:
            pass

    if _queue_listener is not None:
        try:
            _queue_listener.stop()
        except Exception:
            pass
        _queue_listener = None

    log_queue: queue.SimpleQueue = queue.SimpleQueue()
    queue_handler = logging.handlers.QueueHandler(log_queue)
    _queue_listener = logging.handlers.QueueListener(
        log_queue, file_handler, console_handler, respect_handler_level=True,
    )
    _queue_listener.start()
    atexit.register(stop_root_logging)

    root.handlers.clear()
    root.addHandler(queue_handler)

    # Silence noisy third-party loggers regardless of mode.
    for lib in ('websockets', 'asyncio', 'urllib3'):
        logging.getLogger(lib).setLevel(logging.WARNING)

    return root


def stop_root_logging():
    """Flush and stop the queue listener (idempotent)."""
    global _queue_listener
    listener = _queue_listener
    _queue_listener = None
    if listener is not None:
        try:
            listener.stop()
        except Exception:
            pass
