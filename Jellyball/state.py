"""Process-wide runtime state shared by every layer.

The channel table (`stream_state`), the shared HTTP clients and the tracked
background-task set. SHARED_HTTP_CLIENT / MEDIA_HTTP_CLIENT are rebound by the
lifespan in main.py, so other modules read them as `state.SHARED_HTTP_CLIENT`
(a `from state import SHARED_HTTP_CLIENT` copy would keep seeing None).
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Set, TypedDict

import httpx

from config import _log_failure


class ChannelState(TypedDict, total=False):
    """Typed shape for entries in `stream_state`.

    Not every key is present on every channel (Multi-View vs team, scrape
    lifecycle fields, failover bookkeeping). `total=False` keeps this a
    documentation/type-check aid without forcing every writer to populate
    the full set.
    """

    # Identity / catalog
    name: str
    query: str
    category: str
    source_id: str
    content_type: str
    search_terms: List[str]
    catalog_key: str
    tvg_id: str
    group_title: str
    logo_url: str
    always_live: bool
    auto_disable_after: str

    # Schedule / guide
    start_time: str
    stop_time: str
    schedule_status: str

    # Playback candidates
    candidates: List[dict]
    active_index: int
    is_healthy: bool
    exhausted: bool
    exhausted_since: float
    exhausted_probe_in_flight: bool
    last_candidate_refresh: float
    startup_placeholder_until: float
    self_retries: int
    self_retry_window_start: float

    # Failover / alerts
    failover_count: int
    last_failover: dict
    last_failover_alert: float
    suppressed_failover_alerts: int
    last_exhausted_alert: float
    last_emergency_scrape: float
    last_token_refresh: float
    window_close_pending_since: float

    # Scrape lifecycle
    scrape_in_progress: bool
    last_scrape_started: float
    last_scrape_completed: float
    scrape_result: str
    scrape_error: str

    # Multi-View
    type: str
    layout: str
    member_team_ids: List[str]
    active_audio_team_id: str


stream_state: Dict[str, ChannelState] = {}

SHARED_HTTP_CLIENT: Optional[httpx.AsyncClient] = None
MEDIA_HTTP_CLIENT: Optional[httpx.AsyncClient] = None

_BACKGROUND_TASKS: Set[asyncio.Task] = set()


def new_channel_state(**fields: Any) -> ChannelState:
    """Build a ChannelState dict with the common team-channel defaults filled in."""
    state: ChannelState = {
        "name": "",
        "query": "",
        "candidates": [],
        "active_index": 0,
        "is_healthy": False,
        "logo_url": "",
        "start_time": "",
        "stop_time": "",
        "category": "custom",
        "source_id": "",
        "content_type": "team",
        "search_terms": [],
        "always_live": False,
        "catalog_key": "",
        "tvg_id": "",
        "group_title": "",
        "auto_disable_after": "",
        "exhausted": False,
        "failover_count": 0,
    }
    state.update(fields)  # type: ignore[typeddict-item]
    return state


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
