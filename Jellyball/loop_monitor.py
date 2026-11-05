"""Warn when the asyncio event loop is blocked.

Everything the app serves (dashboard, /api/status, live HLS segments) runs on
one event loop, so one synchronous call that takes seconds freezes all of it.
That is hard to diagnose from the outside: the process looks alive but pages
hang. A heartbeat task on the loop stamps the time every 0.25 s; a watchdog
THREAD (it keeps running while the loop is stuck) notices a stale stamp and
logs a WARNING naming the running task and the stack the loop thread is stuck
in, then logs how long the stall lasted once it clears.
"""

import asyncio
import logging
import sys
import threading
import time
import traceback
from typing import Optional

LOGGER = logging.getLogger("jellyball.loop_monitor")

BLOCK_WARN_SECONDS = 1.0
_HEARTBEAT_INTERVAL = 0.25
_WATCHDOG_INTERVAL = 0.25


def describe_loop_thread(loop: asyncio.AbstractEventLoop, thread_id: int) -> str:
    """The task running on `loop` and the innermost frames of its thread."""
    try:
        task: Optional[asyncio.Task] = asyncio.current_task(loop)
    except Exception:  # noqa: BLE001 - diagnostics only
        task = None
    task_name = task.get_name() if task is not None else "(none: loop callback)"
    frame = sys._current_frames().get(thread_id)
    where = ""
    if frame is not None:
        entries = traceback.extract_stack(frame)[-4:]
        where = " <- ".join(f"{e.name} ({e.filename.rsplit('/', 1)[-1].rsplit(chr(92), 1)[-1]}:{e.lineno})" for e in reversed(entries))
    return f"task={task_name} stack={where or 'unknown'}"


class LoopMonitor:
    def __init__(self, warn_after: float = BLOCK_WARN_SECONDS):
        self.warn_after = warn_after
        self._last_beat = time.monotonic()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread_id = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._beat_task: Optional[asyncio.Task] = None
        self.warnings = 0

    async def _beat(self) -> None:
        while True:
            self._last_beat = time.monotonic()
            await asyncio.sleep(_HEARTBEAT_INTERVAL)

    def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        self._last_beat = time.monotonic()
        self._stop.clear()
        self._beat_task = self._loop.create_task(self._beat(), name="loop heartbeat")
        self._thread = threading.Thread(target=self._watch, name="jellyball-loop-watchdog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._beat_task is not None:
            self._beat_task.cancel()
            self._beat_task = None
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=2.0)

    def _watch(self) -> None:
        loop = self._loop
        assert loop is not None
        stalled_since: Optional[float] = None
        warned_at = 0.0
        while not self._stop.wait(_WATCHDOG_INTERVAL):
            lag = time.monotonic() - self._last_beat - _HEARTBEAT_INTERVAL
            if lag >= self.warn_after:
                if stalled_since is None:
                    stalled_since = time.monotonic() - lag
                # Re-warn every 10 s of a continuing stall, not on every tick.
                if time.monotonic() - warned_at >= 10.0:
                    warned_at = time.monotonic()
                    self.warnings += 1
                    LOGGER.warning(
                        "Event loop blocked for %.1fs (pages and streams are frozen meanwhile): %s",
                        lag, describe_loop_thread(loop, self._loop_thread_id),
                    )
            elif stalled_since is not None:
                LOGGER.warning(
                    "Event loop responsive again after a stall of about %.1fs",
                    time.monotonic() - stalled_since,
                )
                stalled_since = None
                warned_at = 0.0
