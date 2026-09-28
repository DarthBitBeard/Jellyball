"""The dashboard page and its form-POST endpoints (settings, catalog,
channels, Multi-View, favorites)."""

import asyncio
import time
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from config import _log_failure, _public_base_url, _resource_path, _safe_team_id, _validate_upstream_url
from state import _spawn_background_task, stream_state
from db import (
    _bulk_set_favorite_sync,
    _load_dashboard_metrics_async,
    _schedule_team_disable_sync,
    _toggle_favorite_sync,
    get_setting_async,
    save_multiview_channel_async,
    set_setting_async,
)
import security
from security import verify_dashboard_auth
from scrapers import (
    _provider_url_setting_key,
    _set_provider_url_override,
    ACTIVE_PROVIDERS,
    HtmlAggregatorScraper,
)
import catalog
from catalog import _season_resume_label, get_catalog_entries
from alerts import (
    get_jellyfin_config,
    get_notification_config,
    request_jellyfin_guide_refresh_if_changed,
    send_alert,
    trigger_jellyfin_refresh,
)
from updates import _update_available, _UPDATE_STATE, check_for_update
import ffmpeg_proc
from ffmpeg_proc import FFMPEG_PATH
from sessions import SESSIONS
from multiview import (
    _clear_multiview_manual_stop,
    _forget_multiview_channel,
    _multiview_cooldown_remaining,
    _MULTIVIEW_FAILURES,
    _MULTIVIEW_HOLDS,
    _MULTIVIEW_LAST_VIEWER,
    _multiview_member_validation,
    _MULTIVIEW_PROCESSES,
    _MULTIVIEW_REFUSALS,
    _request_multiview_start,
    _stop_multiview_manually,
    MULTIVIEW_LAYOUTS,
)
from failover import _scrape_lifecycle_defaults, _session_on_placeholder, trigger_scrape
from channels import _add_manual_team, _remove_channel, _set_catalog_entry_enabled
from tunables import _advanced_settings_html, _apply_tunable, _coerce_tunable, _TUNABLE_DEFAULTS, TUNABLES
from version import __version__

router = APIRouter()


# Dashboard templates/static assets. Resolved with _resource_path so both the
# source tree and the PyInstaller bundle (BUNDLE_DIR/_MEIPASS) find them; see
# jellyball.spec, which adds both directories to `datas`.
TEMPLATES = Jinja2Templates(directory=str(_resource_path("templates")))


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, tab: str = "channels", status: str = "", auth: bool = Depends(verify_dashboard_auth)):
    base_url = _public_base_url(request)
    dashboard_metrics = await _load_dashboard_metrics_async()
    metrics = dashboard_metrics
    favorites = dashboard_metrics["favorites"]
    provider_totals = dashboard_metrics["provider_totals"]
    notif_cfg = await get_notification_config()
    jellyfin_cfg = await get_jellyfin_config()
    provider_rotation_enabled = await get_setting_async("provider_rotation_mode", "0") == "1"
    provider_url_rows = [
        {
            "name": provider.name,
            "base_url": provider.base_url,
            "default_base_url": provider._default_base_url,
        }
        for provider in ACTIVE_PROVIDERS
        if isinstance(provider, HtmlAggregatorScraper)
    ]
    update_check_enabled = await get_setting_async("update_check_enabled", "0") == "1"
    update_banner = None
    if update_check_enabled and _update_available():
        update_banner = {
            "latest": _UPDATE_STATE["latest"],
            "url": _UPDATE_STATE.get("url") or "",
        }
    catalog_entries = await get_catalog_entries()
    active_catalog_keys = {
        data.get("catalog_key")
        for data in stream_state.values()
        if data.get("catalog_key")
    }

    provider_health = []
    if metrics["failovers_by_provider"]:
        provider_stats = {}
        for prov, count in metrics["failovers_by_provider"].items():
            total = provider_totals.get(prov, 0) or 1
            success_rate = max(0, 100 - (count * 100 / total)) if total > 0 else 100
            provider_stats[prov] = (success_rate, count, total)

        for prov in sorted(provider_stats.keys(), key=lambda p: provider_stats[p][0], reverse=True):
            rate, fails, total = provider_stats[prov]
            rate_color = "var(--success)" if rate > 90 else ("var(--warning)" if rate > 70 else "var(--danger)")
            provider_health.append({"provider": prov, "rate": rate, "rate_color": rate_color, "total": total})

    catalog_group_defs = (
        ("ncaaf", "College Football"),
        ("ncaam", "College Basketball"),
        ("nfl", "NFL"),
        ("mlb", "MLB"),
        ("nhl", "NHL"),
        ("nba", "NBA"),
        ("special", "Always-Live Sports Channels"),
    )
    catalog_groups = []
    for group_key, group_label in catalog_group_defs:
        group_entries = [
            entry for entry in catalog_entries
            if ("special" if entry["content_type"] == "channel" else entry["category"]) == group_key
        ]
        if not group_entries:
            continue
        catalog_groups.append({
            "label": group_label,
            "entries": [
                {
                    "catalog_key": entry["catalog_key"],
                    "name": entry["name"],
                    "checked": entry["catalog_key"] in active_catalog_keys,
                    "always_live": entry["always_live"],
                }
                for entry in group_entries
            ],
        })
    catalog_source_text = (
        "College directories refreshed from ESPN."
        if catalog._CATALOG_REMOTE_LOADED
        else "Using bundled teams; ESPN college directories will be retried on the next refresh."
    )

    failover_stats = [
        {"provider": prov, "count": count}
        for prov, count in metrics["failovers_by_provider"].items()
    ]

    events = []
    for ts, team_id, prov, ev_type, details in metrics["recent_events"]:
        badge_color = "var(--danger)" if ev_type in ["exhausted", "danger"] else ("var(--warning)" if ev_type == "failover" else "var(--success)")
        events.append({"ts": ts, "team_id": team_id, "prov": prov, "ev_type": ev_type, "details": details, "badge_color": badge_color})

    if security.DASHBOARD_AUTH_MODE == "open":
        auth_badge = {"text": "\U0001f513 Local Open Access", "title": "No DASHBOARD_PASSWORD; only reachable from this computer"}
    elif security.DASHBOARD_AUTH_MODE == "generated":
        auth_badge = {"text": "\U0001f512 Generated Password", "title": f"Generated password in {security.DASHBOARD_PASSWORD_FILE}"}
    else:
        auth_badge = {"text": "\U0001f512 Password Protected", "title": ""}
    webhook_discord_badge = {"text": "Discord Alert On" if notif_cfg["discord_webhook_url"] else "Discord Off"}
    webhook_telegram_badge = {"text": "Telegram Alert On" if (notif_cfg["telegram_bot_token"] and notif_cfg["telegram_chat_id"]) else "Telegram Off"}
    jellyfin_badge = {"text": "\U0001f347 Jellyfin Auto-Refresh On" if jellyfin_cfg["jellyfin_api_key"] else "\U0001f347 Jellyfin Manual"}

    def _channel_watch_fields(t_id: str, data: dict) -> dict:
        session = SESSIONS.peek(t_id)
        watching = bool(session is not None and session.is_watched())
        on_placeholder = bool(watching and _session_on_placeholder(session))
        return {
            "watching": watching,
            "on_placeholder": on_placeholder,
            "failover_count": int(data.get("failover_count", 0)),
        }

    def _build_channel(t_id: str, data: dict) -> dict:
        dot_class = "online" if data.get('is_healthy') else "offline"
        if data.get('is_healthy'):
            status_text = "Stream Stable & Active"
        elif data.get('schedule_status') == "off_season":
            resume_label = _season_resume_label(data.get('category', ''))
            status_text = f"Off-season (resumes {resume_label})" if resume_label else "Off-season"
        else:
            status_text = "Searching / Re-evaluating"
        candidates_list = data.get('candidates', [])

        candidates_options = []
        for idx, cand in enumerate(candidates_list):
            is_active = "★ " if idx == data.get('active_index', 0) else ""
            prov = cand.get('provider', 'Unknown')
            title_trunc = cand.get("match_title", "Stream")[:25]
            candidates_options.append({"idx": idx, "label": f"{is_active}{prov} - {title_trunc}"})

        channel_badge = "Always live" if data.get("always_live") else (
            str(data.get("category") or "manual").upper()
        )
        remove_label = "Disable" if data.get("catalog_key") else "Remove"

        active_provider = ""
        if candidates_list:
            active_index = data.get("active_index", 0)
            if 0 <= active_index < len(candidates_list):
                active_provider = candidates_list[active_index].get("provider", "")

        channel = {
            "team_id": t_id,
            "name": data["name"],
            "badge": channel_badge,
            "dot_class": dot_class,
            "status_text": status_text,
            "candidates_count": len(candidates_list),
            "has_candidates": bool(candidates_list),
            "candidates_options": candidates_options,
            "active_provider": active_provider,
            "is_favorite": t_id in favorites,
            "remove_label": remove_label,
            "auto_disable_after": data.get('auto_disable_after', ''),
        }
        channel.update(_channel_watch_fields(t_id, data))
        return channel

    favorites_channels = []
    other_channels = []
    for t_id, data in stream_state.items():
        if data.get('type') == "multiview":
            continue
        channel = _build_channel(t_id, data)
        if channel["is_favorite"]:
            favorites_channels.append(channel)
        else:
            other_channels.append(channel)

    multiview_checkbox_list = [
        {"team_id": mv_t_id, "name": mv_data.get("name", mv_t_id)}
        for mv_t_id, mv_data in stream_state.items()
        if mv_data.get("type") != "multiview"
    ]

    multiview_rows = []
    for mv_id, mv_data in stream_state.items():
        if mv_data.get("type") != "multiview":
            continue
        mv_entry = _MULTIVIEW_PROCESSES.get(mv_id)
        mv_running = bool(mv_entry and mv_entry["process"].returncode is None and not mv_entry.get("exited"))
        member_ids = mv_data.get("member_team_ids", [])
        member_names = ", ".join(stream_state.get(m, {}).get("name", m) for m in member_ids)
        audio_options = [
            {
                "team_id": m,
                "name": stream_state.get(m, {}).get("name", m),
                "selected": m == mv_data.get("active_audio_team_id"),
            }
            for m in member_ids
        ]
        mv_failure = _MULTIVIEW_FAILURES.get(mv_id)
        failure = None
        if not mv_running and mv_failure:
            failure = {
                "last_error": mv_failure["last_error"],
                "count": mv_failure["count"],
                "retry_in": round(_multiview_cooldown_remaining(mv_id)),
            }
        multiview_rows.append({
            "mv_id": mv_id,
            "name": mv_data.get("name", mv_id),
            "layout": mv_data.get("layout", ""),
            "status": "\U0001f7e2 Running" if mv_running else "⚪ Stopped",
            "member_names": member_names,
            "audio_options": audio_options,
            "failure": failure,
        })

    multiview = {
        "ffmpeg_available": ffmpeg_proc.FFMPEG_AVAILABLE,
        "ffmpeg_path": FFMPEG_PATH,
        "checkbox_list": multiview_checkbox_list,
        "rows": multiview_rows,
    }

    context = {
        "request": request,
        "tab": tab,
        "version": __version__,
        "base_url": base_url,
        "update_banner": update_banner,
        "auth_badge": auth_badge,
        "jellyfin_badge": jellyfin_badge,
        "webhook_discord_badge": webhook_discord_badge,
        "webhook_telegram_badge": webhook_telegram_badge,
        "channels_empty": not any(data.get('type') != "multiview" for data in stream_state.values()),
        "auth_mode": security.DASHBOARD_AUTH_MODE,
        "dashboard_password_file": str(security.DASHBOARD_PASSWORD_FILE),
        "catalog_source_text": catalog_source_text,
        "catalog_entries_count": len(catalog_entries),
        "active_catalog_count": len(active_catalog_keys),
        "catalog_groups": catalog_groups,
        "favorites": favorites_channels,
        "others": other_channels,
        "multiview": multiview,
        "provider_health": provider_health,
        "failover_stats": failover_stats,
        "events": events,
        "provider_url_rows": provider_url_rows,
        "jellyfin_cfg": jellyfin_cfg,
        "notif_cfg": notif_cfg,
        "jellyfin_api_key_secret": _secret_input("jellyfin_api_key", jellyfin_cfg['jellyfin_api_key']),
        "discord_webhook_secret": _secret_input("discord_webhook_url", notif_cfg['discord_webhook_url']),
        "telegram_bot_token_secret": _secret_input("telegram_bot_token", notif_cfg['telegram_bot_token']),
        "advanced_settings_groups": _advanced_settings_html(),
        "update_check_enabled": update_check_enabled,
        "show_offseason_channels": catalog.SHOW_OFFSEASON_CHANNELS,
        "provider_rotation_enabled": provider_rotation_enabled,
        "dashboard_data": {},
    }
    return TEMPLATES.TemplateResponse(request, "dashboard.html", context)

def _secret_input(name: str, saved_value: str) -> dict:
    """Data for a password-type input that never echoes a saved secret back
    into the page (rendered by the dashboard template's secret_input macro).
    Blank on submit keeps the saved value; the checkbox clears it."""
    return {
        "name": name,
        "has_value": bool(saved_value),
        "masked_tail": saved_value[-4:] if saved_value else "",
    }


def _submitted_secret(submitted: str, clear: str, current: str) -> str:
    """Resolve a _secret_input submission against the current value."""
    if clear:
        return ""
    return (submitted or "").strip() or current


@router.post("/settings/notifications")
async def update_notifications(
    discord_webhook_url: str = Form(""),
    telegram_bot_token: str = Form(""),
    telegram_chat_id: str = Form(""),
    clear_discord_webhook_url: str = Form(""),
    clear_telegram_bot_token: str = Form(""),
    auth: bool = Depends(verify_dashboard_auth),
):
    current = await get_notification_config()
    discord = _submitted_secret(discord_webhook_url, clear_discord_webhook_url, current["discord_webhook_url"])
    token = _submitted_secret(telegram_bot_token, clear_telegram_bot_token, current["telegram_bot_token"])
    await set_setting_async("discord_webhook_url", discord)
    await set_setting_async("telegram_bot_token", token)
    await set_setting_async("telegram_chat_id", telegram_chat_id.strip())
    return RedirectResponse(url="/?tab=alerts&status=saved", status_code=303)

@router.post("/settings/jellyfin")
async def update_jellyfin_settings(
    jellyfin_url: str = Form("http://localhost:8096"),
    jellyfin_api_key: str = Form(""),
    jellyfin_task_id: str = Form(""),
    clear_jellyfin_api_key: str = Form(""),
    auth: bool = Depends(verify_dashboard_auth),
):
    jellyfin_url = jellyfin_url.strip().rstrip("/")
    if not _validate_upstream_url(jellyfin_url, allow_private=True):
        return RedirectResponse(url="/?tab=alerts&status=jellyfin_url_invalid", status_code=303)
    current = await get_jellyfin_config()
    new_key = (jellyfin_api_key or "").strip()
    old_host = urllib.parse.urlsplit(current["jellyfin_url"]).netloc.lower()
    new_host = urllib.parse.urlsplit(jellyfin_url).netloc.lower()
    # The saved key must never follow a URL change to another host (that would
    # hand the key to whatever server the new URL names): require it again.
    if new_host != old_host and current["jellyfin_api_key"] and not new_key and not clear_jellyfin_api_key:
        return RedirectResponse(url="/?tab=alerts&status=jellyfin_key_required", status_code=303)
    api_key = "" if clear_jellyfin_api_key else (new_key or current["jellyfin_api_key"])
    await set_setting_async("jellyfin_url", jellyfin_url)
    await set_setting_async("jellyfin_api_key", api_key)
    await set_setting_async("jellyfin_task_id", jellyfin_task_id.strip())
    return RedirectResponse(url="/?tab=alerts&status=jellyfin_saved", status_code=303)

@router.post("/settings/test_jellyfin")
async def test_jellyfin_refresh_endpoint(auth: bool = Depends(verify_dashboard_auth)):
    success = await trigger_jellyfin_refresh()
    status_code = "jellyfin_success" if success else "jellyfin_failed"
    return RedirectResponse(url=f"/?tab=alerts&status={status_code}", status_code=303)

@router.post("/settings/test_alert")
async def test_alert(auth: bool = Depends(verify_dashboard_auth)):
    await send_alert("🧪 Jellyball Notification Test", "Proactive monitoring alerts are functioning correctly.", "info")
    return RedirectResponse(url="/?tab=alerts&status=test_sent", status_code=303)


def _catalog_selection_changes(
    entries: List[dict],
    selected_keys: Set[str],
    active_catalog_keys: Set[str],
) -> tuple[Dict[str, dict], Set[str], Set[str]]:
    entries_by_key = {entry["catalog_key"]: entry for entry in entries}
    unknown_keys = selected_keys - set(entries_by_key)
    if unknown_keys:
        raise ValueError("Invalid catalog selection")
    return (
        entries_by_key,
        selected_keys - active_catalog_keys,
        (active_catalog_keys & set(entries_by_key)) - selected_keys,
    )


@router.post("/catalog/apply")
async def apply_catalog(
    catalog_keys: Optional[List[str]] = Form(None),
    auth: bool = Depends(verify_dashboard_auth),
):
    entries = await get_catalog_entries()
    selected_keys = {key.strip() for key in (catalog_keys or []) if key.strip()}
    active_catalog_keys = {
        data.get("catalog_key")
        for data in stream_state.values()
        if data.get("catalog_key")
    }
    try:
        entries_by_key, keys_to_enable, keys_to_disable = _catalog_selection_changes(
            entries,
            selected_keys,
            active_catalog_keys,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    changed_count = 0
    for catalog_key in keys_to_enable:
        changed_count += int(await _set_catalog_entry_enabled(entries_by_key[catalog_key], True))
    for catalog_key in keys_to_disable:
        changed_count += int(await _set_catalog_entry_enabled(entries_by_key[catalog_key], False))

    if changed_count:
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after catalog apply")
    return RedirectResponse(url=f"/?tab=channels&status=catalog_applied&changed={changed_count}", status_code=303)


@router.post("/catalog/toggle")
async def toggle_catalog(
    catalog_key: str = Form(...),
    enabled: bool = Form(False),
    auth: bool = Depends(verify_dashboard_auth),
):
    entries = await get_catalog_entries()
    entry = next((item for item in entries if item["catalog_key"] == catalog_key), None)
    if not entry:
        raise HTTPException(status_code=404, detail="Catalog entry not found")

    await _set_catalog_entry_enabled(entry, enabled)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after catalog toggle")
    return RedirectResponse(url="/?tab=channels&status=catalog_toggled", status_code=303)


@router.post("/add_team")
async def add_team(team_name: str = Form(...), search_query: str = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    team_id = await _add_manual_team(team_name, search_query)
    if team_id is None:
        raise HTTPException(status_code=400, detail="Team name and search query are required")
    return RedirectResponse(url="/?tab=channels&status=team_added", status_code=303)

@router.post("/override/{team_id}")
async def override_stream(team_id: str, candidate_index: int = Form(...), auth: bool = Depends(verify_dashboard_auth)):
    if team_id in stream_state and stream_state[team_id].get("candidates"):
        max_idx = len(stream_state[team_id]["candidates"]) - 1
        if 0 <= candidate_index <= max_idx:
            stream_state[team_id]["active_index"] = candidate_index
            stream_state[team_id]["is_healthy"] = True
            stream_state[team_id]["exhausted"] = False
            stream_state[team_id]["self_retries"] = 0
            candidate = stream_state[team_id]["candidates"][candidate_index]
            candidate["consecutive_failures"] = 0
            candidate.pop("session_compatible", None)
            SESSIONS.poke(team_id)
            
            team_name = stream_state[team_id]["name"]
            prov = stream_state[team_id]["candidates"][candidate_index].get("provider", "Unknown")
            _spawn_background_task(
                send_alert("🛠️ Manual Override Activated", f"Stream for **{team_name}** forced to {prov}.", "info"),
                f"send manual-override alert team={team_id}",
            )
            
    return RedirectResponse(url="/?tab=channels&status=override_saved", status_code=303)


@router.post("/remove_team/{team_id}")
async def remove_team(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    if team_id in stream_state:
        await _remove_channel(team_id)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after team removal")
    return RedirectResponse(url="/?tab=channels&status=team_removed", status_code=303)


@router.post("/multiview/create")
async def create_multiview(
    name: str = Form(...),
    layout: str = Form("grid_2x2"),
    member_team_ids: str = Form(...),
    auth: bool = Depends(verify_dashboard_auth),
):
    name = name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")
    if layout not in MULTIVIEW_LAYOUTS:
        raise HTTPException(status_code=400, detail="Invalid layout")
    members = [m.strip() for m in member_team_ids.split(",") if m.strip()]
    error = _multiview_member_validation(members)
    if error:
        raise HTTPException(status_code=400, detail=error)
    if MULTIVIEW_LAYOUTS[layout]["count"] != len(members):
        raise HTTPException(status_code=400, detail=f"Layout {layout} requires exactly {MULTIVIEW_LAYOUTS[layout]['count']} channels")

    channel_id = _safe_team_id(f"multiview_{name}")
    if channel_id in stream_state:
        # INSERT OR REPLACE used to silently overwrite an existing Multi-View
        # (leaving its old ffmpeg running against the old members).
        raise HTTPException(status_code=409, detail="A channel with that name already exists")
    active_audio_team_id = members[0]
    _forget_multiview_channel(channel_id)
    await save_multiview_channel_async(channel_id, name, layout, members, active_audio_team_id)
    stream_state[channel_id] = {
        "name": name, "query": "", "type": "multiview",
        "candidates": [{"synthetic": True}],
        "active_index": 0, "is_healthy": False,
        "logo_url": "", "start_time": "", "stop_time": "",
        "category": "multiview", "source_id": "", "content_type": "multiview",
        "search_terms": [], "always_live": True, "catalog_key": "",
        "tvg_id": "", "group_title": "Multi-View",
        "layout": layout, "member_team_ids": members,
        "active_audio_team_id": active_audio_team_id,
        **_scrape_lifecycle_defaults(),
    }
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after multiview creation")
    return RedirectResponse(url="/?tab=channels&status=multiview_created", status_code=303)


@router.post("/multiview/{channel_id}/remove")
async def remove_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        await _remove_channel(channel_id)
        _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after multiview removal")
    return RedirectResponse(url="/?tab=channels&status=multiview_removed", status_code=303)


@router.post("/multiview/{channel_id}/stop")
async def stop_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        # Cancels a spawn in progress too, and keeps the grid stopped (viewers
        # get No Signal) until a new viewer tunes in or /start is posted.
        await _stop_multiview_manually(channel_id)
    return RedirectResponse(url="/?tab=channels&status=multiview_stopped", status_code=303)


@router.post("/multiview/{channel_id}/start")
async def start_multiview(channel_id: str, auth: bool = Depends(verify_dashboard_auth)):
    """Explicit start: clears a manual stop and any backoff, then starts the
    grid now (the idle monitor stops it again if nobody watches)."""
    data = stream_state.get(channel_id)
    if data and data.get("type") == "multiview":
        _clear_multiview_manual_stop(channel_id, "explicit start")
        for mapping in (_MULTIVIEW_FAILURES, _MULTIVIEW_HOLDS, _MULTIVIEW_REFUSALS):
            mapping.pop(channel_id, None)
        _MULTIVIEW_LAST_VIEWER[channel_id] = time.monotonic()
        _request_multiview_start(channel_id)
    return RedirectResponse(url="/?tab=channels&status=multiview_started", status_code=303)


@router.post("/multiview/{channel_id}/set-audio")
async def set_multiview_audio(
    channel_id: str,
    active_audio_team_id: str = Form(...),
    auth: bool = Depends(verify_dashboard_auth),
):
    data = stream_state.get(channel_id)
    if not data or data.get("type") != "multiview":
        raise HTTPException(status_code=404, detail="Multi-View channel not found")
    if active_audio_team_id not in data.get("member_team_ids", []):
        raise HTTPException(status_code=400, detail="Not a member of this Multi-View channel")
    data["active_audio_team_id"] = active_audio_team_id
    await save_multiview_channel_async(
        channel_id, data["name"], data["layout"], data["member_team_ids"], active_audio_team_id,
        data.get("tvg_id", ""), data.get("group_title", ""), data.get("logo_url", ""),
    )
    # Every member's audio is already in its own output (stream-copied), so
    # switching the main channel's audio is just pointing its session at
    # another output: no ffmpeg restart. The view's source key carries the
    # output's audio codec (see _resolve_multiview_view_source): same codec
    # continues seamlessly, a different codec becomes a clean discontinuity.
    SESSIONS.poke(channel_id)
    return RedirectResponse(url="/?tab=channels&status=multiview_audio_set", status_code=303)


@router.post("/favorite/{team_id}")
async def toggle_favorite(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    try:
        await asyncio.to_thread(_toggle_favorite_sync, team_id)
    except Exception as exc:
        _log_failure(f"toggle favorite for {team_id}", exc)
    return RedirectResponse(url="/?tab=channels&status=favorite_toggled", status_code=303)

@router.post("/rescrape/{team_id}")
async def manual_rescrape(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    _spawn_background_task(trigger_scrape(team_id, force=True), f"manual rescrape {team_id}")
    return RedirectResponse(url="/?tab=channels&status=rescrape_started", status_code=303)


@router.post("/bulk-favorite")
async def bulk_favorite(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        await asyncio.to_thread(_bulk_set_favorite_sync, ids, True)
    except Exception as exc:
        _log_failure("bulk favorite", exc)
    return RedirectResponse(url="/?tab=channels&status=bulk_favorited", status_code=303)

@router.post("/bulk-unfavorite")
async def bulk_unfavorite(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        await asyncio.to_thread(_bulk_set_favorite_sync, ids, False)
    except Exception as exc:
        _log_failure("bulk unfavorite", exc)
    return RedirectResponse(url="/?tab=channels&status=bulk_unfavorited", status_code=303)

@router.post("/bulk-remove")
async def bulk_remove(team_ids: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    ids = [t.strip() for t in team_ids.split(",") if t.strip()]
    try:
        for team_id in ids:
            await _remove_channel(team_id)
    except Exception as exc:
        _log_failure("bulk remove", exc)
    _spawn_background_task(trigger_jellyfin_refresh(), "refresh Jellyfin guide after bulk removal")
    return RedirectResponse(url="/?tab=channels&status=bulk_removed", status_code=303)


@router.post("/settings/advanced")
async def update_advanced_settings(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Blank field = back to the default (env var or built-in). Applied live."""
    form = await request.form()
    for tunable in TUNABLES:
        if tunable.name not in form:
            continue
        raw = str(form.get(tunable.name) or "").strip()
        if raw == "":
            await set_setting_async(tunable.key, "")
            _apply_tunable(tunable, _TUNABLE_DEFAULTS[tunable.name])
            continue
        value = _coerce_tunable(tunable, raw)
        if value is None:
            return RedirectResponse(url="/?tab=playback&status=advanced_invalid", status_code=303)
        await set_setting_async(tunable.key, str(value))
        _apply_tunable(tunable, value)
    return RedirectResponse(url="/?tab=playback&status=advanced_saved", status_code=303)


@router.post("/settings/provider-rotation")
async def set_provider_rotation(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("provider_rotation_mode", "1" if enabled else "0")
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


@router.post("/settings/update-check")
async def set_update_check(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("update_check_enabled", "1" if enabled else "0")
    if enabled:
        _spawn_background_task(check_for_update(), "check for updates")
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


@router.post("/settings/offseason")
async def set_offseason_listing(enabled: bool = Form(False), auth: bool = Depends(verify_dashboard_auth)):
    await set_setting_async("show_offseason_channels", "1" if enabled else "0")
    if catalog.SHOW_OFFSEASON_CHANNELS != bool(enabled):
        catalog.SHOW_OFFSEASON_CHANNELS = bool(enabled)
        request_jellyfin_guide_refresh_if_changed()
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


def _parse_disable_date(value: str) -> Optional[str]:
    """'' clears the schedule; anything else must be a real YYYY-MM-DD date
    (it is compared as text against date('now'), so other formats never fire)."""
    value = (value or "").strip()
    if not value:
        return ""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


@router.post("/team/{team_id}/schedule-disable")
async def schedule_team_disable(team_id: str, disable_date: str = Form(""), auth: bool = Depends(verify_dashboard_auth)):
    parsed = _parse_disable_date(disable_date)
    if parsed is None or team_id not in stream_state:
        return RedirectResponse(url="/?tab=channels&status=schedule_invalid", status_code=303)
    try:
        await asyncio.to_thread(_schedule_team_disable_sync, team_id, parsed)
        stream_state[team_id]["auto_disable_after"] = parsed
    except Exception as exc:
        _log_failure(f"schedule disable {team_id}", exc)
        return RedirectResponse(url="/?tab=channels&status=schedule_failed", status_code=303)
    return RedirectResponse(url="/?tab=channels&status=schedule_saved", status_code=303)


@router.post("/settings/providers")
async def update_provider_domains(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Save per-provider aggregator base-URL overrides (D1). A blank field
    resets that provider to its env/default URL. Every non-blank value must
    validate as an absolute public http(s) URL; if any doesn't, nothing is
    saved and the dashboard shows a validation error, matching how
    /settings/jellyfin reports an invalid URL."""
    form = await request.form()
    providers = [p for p in ACTIVE_PROVIDERS if isinstance(p, HtmlAggregatorScraper)]

    updates: List[Tuple["HtmlAggregatorScraper", str]] = []
    for provider in providers:
        raw_value = str(form.get(f"url_{provider.name}", "") or "").strip()
        if not raw_value:
            updates.append((provider, ""))
            continue
        validated = _validate_upstream_url(raw_value)
        if not validated:
            return RedirectResponse(url="/?tab=alerts&status=providers_invalid", status_code=303)
        updates.append((provider, validated.rstrip("/")))

    for provider, value in updates:
        await set_setting_async(_provider_url_setting_key(provider.name), value)
        _set_provider_url_override(provider, value)

    return RedirectResponse(url="/?tab=alerts&status=providers_saved", status_code=303)
