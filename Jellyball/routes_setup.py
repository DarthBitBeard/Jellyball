"""Guided first-run setup wizard (2.2.0): connect Jellyfin, pick teams, copy endpoints.

Three steps, all JSON except the page itself:

  GET  /setup                the wizard page (setup.html)
  GET  /setup/status         first-run detection + progress, as JSON
  POST /setup/jellyfin       save Jellyfin URL / API key / task id (same safety
                             rules as the dashboard's /settings/jellyfin)
  POST /setup/jellyfin/test  test supplied (unsaved) credentials end to end and
                             report the precise failure stage
  POST /setup/teams          apply a catalog selection (add/remove team channels)
  POST /setup/verify         trigger a Jellyfin guide refresh with the saved
                             config and report the outcome, as JSON

Rules (from the lane header):

- Every route is protected with `auth: bool = Depends(verify_dashboard_auth)`.
- The logic stays in its own modules; this file is the thin HTTP layer.
"""

import urllib.parse
from typing import Dict, List, Optional, Set

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from alerts import get_jellyfin_config, refresh_jellyfin_guide
from catalog import get_catalog_entries
from channels import _set_catalog_entry_enabled
from config import _public_base_url, _resource_path, _validate_upstream_url
from db import set_setting_async
from jellyfin_client import refresh_guide
from leagues import LEAGUES
from routes_dashboard import _catalog_selection_changes
from security import verify_dashboard_auth
from state import stream_state
from version import __version__

router = APIRouter()
TEMPLATES = Jinja2Templates(directory=str(_resource_path("templates")))


async def _setup_needed() -> bool:
    """First run: no channels configured and Jellyfin not yet connected."""
    if stream_state:
        return False
    cfg = await get_jellyfin_config()
    return not cfg.get("jellyfin_api_key")


def _catalog_groups(entries: List[dict], active_keys: Set[str]) -> List[dict]:
    """Same grouping as the dashboard catalog picker: one section per league."""
    group_defs = (*((league.key, league.group_title) for league in LEAGUES), ("special", "Always-Live Sports Channels"))
    groups = []
    for group_key, group_label in group_defs:
        group_entries = [
            entry
            for entry in entries
            if ("special" if entry["content_type"] == "channel" else entry["category"]) == group_key
        ]
        if not group_entries:
            continue
        groups.append(
            {
                "label": group_label,
                "entries": [
                    {
                        "catalog_key": entry["catalog_key"],
                        "name": entry["name"],
                        "checked": entry["catalog_key"] in active_keys,
                        "always_live": entry["always_live"],
                    }
                    for entry in sorted(group_entries, key=lambda e: e["name"].lower())
                ],
            }
        )
    return groups


@router.get("/setup")
async def setup_wizard(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Render the 3-step setup wizard page."""
    cfg = await get_jellyfin_config()
    entries = await get_catalog_entries()
    active_keys = {
        str(data["catalog_key"])
        for data in stream_state.values()
        if data.get("catalog_key")
    }
    base_url = _public_base_url(request)
    return TEMPLATES.TemplateResponse(
        request,
        "setup.html",
        {
            "request": request,
            "version": __version__,
            "jellyfin_url": cfg.get("jellyfin_url") or "",
            "jellyfin_task_id": cfg.get("jellyfin_task_id") or "",
            "jellyfin_configured": bool(cfg.get("jellyfin_api_key")),
            "channels_count": len(stream_state),
            "catalog_groups": _catalog_groups(entries, active_keys),
            "playlist_url": f"{base_url}/playlist.m3u",
            "epg_url": f"{base_url}/epg.xml",
        },
    )


@router.get("/setup/status")
async def setup_status(auth: bool = Depends(verify_dashboard_auth)):
    """First-run detection and wizard progress, as JSON (used by the dashboard banner)."""
    cfg = await get_jellyfin_config()
    return {
        "first_run": await _setup_needed(),
        "channels": len(stream_state),
        "jellyfin_configured": bool(cfg.get("jellyfin_api_key")),
    }


@router.post("/setup/jellyfin")
async def setup_save_jellyfin(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Save the Jellyfin connection. Mirrors the safety rules of the dashboard's
    /settings/jellyfin: the URL must validate, and the saved API key never follows
    a URL change to another host (the key must be re-entered)."""
    try:
        body = await request.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    url = str(body.get("url") or "").strip().rstrip("/")
    api_key = str(body.get("api_key") or "").strip()
    task_id = str(body.get("task_id") or "").strip()

    if not _validate_upstream_url(url, allow_private=True):
        return JSONResponse({"ok": False, "error": "That URL is not valid. Use http(s)://host:port, e.g. http://192.168.1.10:8096."}, status_code=400)
    current = await get_jellyfin_config()
    old_host = urllib.parse.urlsplit(current["jellyfin_url"]).netloc.lower()
    new_host = urllib.parse.urlsplit(url).netloc.lower()
    if new_host != old_host and current["jellyfin_api_key"] and not api_key:
        return JSONResponse(
            {"ok": False, "error": "The Jellyfin host changed, so the API key must be entered again (the old key is never sent to a new host)."},
            status_code=400,
        )
    # Keep the saved key when the field is left blank on the same host.
    saved_key = api_key or (current["jellyfin_api_key"] if new_host == old_host else "")
    await set_setting_async("jellyfin_url", url)
    await set_setting_async("jellyfin_api_key", saved_key)
    await set_setting_async("jellyfin_task_id", task_id)
    return {"ok": True}


@router.post("/setup/jellyfin/test")
async def setup_test_jellyfin(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Test Jellyfin credentials end to end WITHOUT saving them: reach, auth,
    Refresh Guide task discovery, and triggering the task. Returns the precise
    failure stage so the wizard can say what to fix."""
    try:
        body = await request.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    url = str(body.get("url") or "").strip().rstrip("/")
    api_key = str(body.get("api_key") or "").strip()
    cfg: Dict[str, str] = {"jellyfin_url": url, "jellyfin_api_key": api_key, "jellyfin_task_id": ""}
    result = await refresh_guide(cfg)
    # A discovered task id is cached to settings by the client; surface it so the
    # wizard can pre-fill the (optional) task-id field before saving.
    discovered = await get_jellyfin_config()
    return {
        "ok": result.ok,
        "stage": result.stage,
        "status": result.status,
        "reason": result.reason,
        "task_id": discovered.get("jellyfin_task_id") or "",
    }


@router.post("/setup/teams")
async def setup_apply_teams(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Apply the wizard's team selection: same add/remove semantics as the
    dashboard's /catalog/apply, driven by catalog keys."""
    try:
        body = await request.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    raw_keys = body.get("catalog_keys") or []
    selected_keys = {str(key).strip() for key in raw_keys if str(key).strip()}
    entries = await get_catalog_entries()
    active_keys = {
        str(data["catalog_key"])
        for data in stream_state.values()
        if data.get("catalog_key")
    }
    try:
        entries_by_key, keys_to_enable, keys_to_disable = _catalog_selection_changes(entries, selected_keys, active_keys)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    enabled = 0
    disabled = 0
    for key in sorted(keys_to_enable):
        enabled += int(await _set_catalog_entry_enabled(entries_by_key[key], True))
    for key in sorted(keys_to_disable):
        disabled += int(await _set_catalog_entry_enabled(entries_by_key[key], False))
    return {"ok": True, "enabled": enabled, "disabled": disabled, "channels": len(stream_state)}


@router.post("/setup/verify")
async def setup_verify(request: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Trigger a Jellyfin guide refresh with the SAVED config and report the
    outcome as JSON (the wizard's final "did it work" check)."""
    result = await refresh_jellyfin_guide()
    return {
        "ok": result.ok,
        "stage": result.stage,
        "status": result.status,
        "reason": result.reason,
    }
