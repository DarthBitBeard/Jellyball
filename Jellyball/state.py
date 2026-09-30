"""Process-wide runtime state shared by every layer.

The channel table (`stream_state`), the shared HTTP clients and the tracked
background-task set. SHARED_HTTP_CLIENT / MEDIA_HTTP_CLIENT are rebound by the
lifespan in main.py, so other modules read them as `state.SHARED_HTTP_CLIENT`
(a `from state import SHARED_HTTP_CLIENT` copy would keep seeing None).
"""

import asyncio
import logging
from typing import Dict, Optional, Set

import httpx

from config import _log_failure


stream_state: Dict[str, dict] = {}

SHARED_HTTP_CLIENT: Optional[httpx.AsyncClient] = None
MEDIA_HTTP_CLIENT: Optional[httpx.AsyncClient] = None

_BACKGROUND_TASKS: Set[asyncio.Task] = set()


def _spawn_background_task(coroutine, operation: str) -> asyncio.Task:
    """Track fire-and-forget work so failures are logged and shutdown is clean."""
    task = asyncio.create_task(coroutine, name=operation)
    _BACKGROUND_TASKS.add(task)

    def _task_finished(done: asyncio.Task) -> None:
        _BACKGROUND_TASKS.discard(done)
        if done.cancelled():
            return
        try:
            exception = done.exception()
        except asyncio.CancelledError:
            return
        if exception:
            _log_failure(operation, exception, logging.ERROR)

    task.add_done_callback(_task_finished)
    return task


async def _cancel_background_tasks() -> None:
    tasks = list(_BACKGROUND_TASKS)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


PLACEHOLDER_SESSION_ID = "__placeholder__"


def _media_client() -> Optional[httpx.AsyncClient]:
    """Dedicated pool for playlists/segments so scrape bursts can't starve playback."""
    return MEDIA_HTTP_CLIENT or SHARED_HTTP_CLIENT
