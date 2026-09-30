"""Channel management shared by the dashboard and API routes: adding and
removing channels, catalog enable/disable, scheduled auto-disable.
"""

import asyncio
from typing import Optional

from config import _clean_label, _log_failure, _safe_team_id, _validate_upstream_url, LOGGER
from state import _spawn_background_task, new_channel_state, stream_state
from db import (
    _fetch_expired_scheduled_teams,
    delete_multiview_channel_async,
    delete_team_async,
    save_team_async,
)
from alerts import trigger_jellyfin_refresh
from ffmpeg_proc import _remove_tree_later
from sessions import _LAST_PLAYBACK_EVENT, SESSIONS
import multiview
from multiview import (
    _cancel_multiview_start,
    _forget_multiview_channel,
    _MULTIVIEW_PROCESSES,
    _multiview_start_in_progress,
    _multiview_view_session_ids,
    _restart_multiview,
    _stop_multiview_process,
)
from failover import _scrape_lifecycle_defaults, _start_team_scrape_loop, _stop_team_scrape_loop
from sports_matcher import get_team_search_terms


async def enforce_scheduled_disables_once() -> None:
    """Remove teams whose scheduled auto-disable date has passed."""
    try:
        expired = await asyncio.to_thread(_fetch_expired_scheduled_teams)
    except Exception as exc:
        _log_failure("query scheduled team disables", exc)
        return
    if not expired:
        return
    for team_id, name in expired:
        try:
            await _remove_channel(team_id)
            LOGGER.info("Auto-disabled scheduled team=%s name=%s", team_id, name)
        except Exception as exc:
            _log_failure(f"auto-disable team={team_id}", exc)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after scheduled disable")


async def enforce_scheduled_disables():
    while True:
        try:
            await enforce_scheduled_disables_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("scheduled disable enforcement", exc)
        await asyncio.sleep(3600)


async def _set_catalog_entry_enabled(entry: dict, enabled: bool) -> bool:
    team_id = entry["team_id"]
    if enabled:
        if team_id not in stream_state:
            await save_team_async(
                team_id,
                entry["name"],
                entry["query"],
                entry["logo_url"],
                "",
                "",
                entry["category"],
                entry["source_id"],
                entry["content_type"],
                entry["search_terms"],
                entry["always_live"],
                entry["catalog_key"],
            )
            stream_state[team_id] = new_channel_state(
                name=entry["name"],
                query=entry["query"],
                logo_url=entry["logo_url"],
                category=entry["category"],
                source_id=entry["source_id"],
                content_type=entry["content_type"],
                search_terms=entry["search_terms"],
                always_live=entry["always_live"],
                catalog_key=entry["catalog_key"],
                tvg_id=entry.get("tvg_id", ""),
                group_title=entry.get("group_title", ""),
                **_scrape_lifecycle_defaults(),
            )
            _start_team_scrape_loop(team_id)
            return True
    else:
        if team_id in stream_state:
            await _remove_channel(team_id)
            return True
        await delete_team_async(team_id)
    return False


TEAM_NAME_MAX_LENGTH = 120
TEAM_QUERY_MAX_LENGTH = 200


async def _add_manual_team(
    team_name: str,
    search_query: str,
    *,
    team_id: str = "",
    logo_url: str = "",
    category: str = "custom",
) -> Optional[str]:
    """Sanitize, persist and start one manually defined channel. Shared by the
    dashboard form and config import so both apply the same rules. Returns the
    team id, or None when the input is unusable."""
    team_name = _clean_label(team_name, TEAM_NAME_MAX_LENGTH)
    search_query = _clean_label(search_query, TEAM_QUERY_MAX_LENGTH)
    if not team_name or not search_query:
        return None
    team_id = _safe_team_id(team_id or team_name)
    # Jellyfin fetches channel logos server-side: only public http(s) URLs.
    logo_url = (_validate_upstream_url(logo_url.strip()) or "") if logo_url else ""
    category = _clean_label(category, 40) or "custom"
    search_terms = get_team_search_terms(team_name, search_query, team_id)
    await save_team_async(team_id, team_name, search_query, logo_url, category=category,
                          search_terms=search_terms, content_type="manual")
    stream_state[team_id] = new_channel_state(
        name=team_name,
        query=search_query,
        logo_url=logo_url,
        category=category,
        content_type="manual",
        search_terms=search_terms,
        **_scrape_lifecycle_defaults(),
    )
    _start_team_scrape_loop(team_id)
    return team_id


async def _remove_channel(channel_id: str) -> None:
    """Single removal path for every kind of channel: stops its scrape loop or
    Multi-View ffmpeg, closes its channel sessions, and deletes its DB row."""
    data = stream_state.get(channel_id)
    _LAST_PLAYBACK_EVENT.pop(channel_id, None)
    if data is not None and data.get("type") == "multiview":
        view_sessions = _multiview_view_session_ids(channel_id)
        # Out of stream_state first: a spawn already past its warm-up re-checks
        # this after every await and gives up instead of launching ffmpeg for
        # a deleted channel; then cancel it (it stops anything it launched).
        stream_state.pop(channel_id, None)
        await _cancel_multiview_start(channel_id)
        await _stop_multiview_process(channel_id)
        _forget_multiview_channel(channel_id)
        _remove_tree_later(multiview.MULTIVIEW_OUTPUT_ROOT / channel_id, f"multiview={channel_id}")
        await delete_multiview_channel_async(channel_id)
        for session_id in view_sessions:
            await SESSIONS.close(session_id)
        return
    if data is not None:
        await _stop_team_scrape_loop(channel_id)
        stream_state.pop(channel_id, None)
    await delete_team_async(channel_id)
    await SESSIONS.close(channel_id)
    dependents = [
        cid for cid, other in stream_state.items()
        if other.get("type") == "multiview" and channel_id in (other.get("member_team_ids") or [])
    ]
    for multiview_id in dependents:
        # The removed member's pane now shows "No Signal"; restart a running
        # (or starting) run so it picks up the placeholder input. A config
        # change, so it doesn't count toward the restart backoff.
        LOGGER.warning("Channel %s removed but used by Multi-View %s; its pane will show No Signal", channel_id, multiview_id)
        if multiview_id in _MULTIVIEW_PROCESSES or _multiview_start_in_progress(multiview_id):
            _spawn_background_task(
                _restart_multiview(multiview_id, f"member {channel_id} removed", count_toward_backoff=False),
                f"restart multiview channel={multiview_id}",
            )
