"""In-memory changestamp for cross-device polling.

Most successful mutating API responses bump the stamp immediately. Sync flows
manage it more precisely and only bump after work that actually changes data.
Clients poll ``GET /api/sync/changestamp`` and trigger a data reload when the
value they receive differs from the one they last saw.
"""

from __future__ import annotations

import time
import threading


_lock = threading.Lock()
_stamp: float = time.time()


def bump() -> float:
    """Advance the changestamp to *now* and return it."""
    global _stamp
    with _lock:
        _stamp = time.time()
        return _stamp


def get() -> float:
    """Return the current changestamp."""
    with _lock:
        return _stamp
