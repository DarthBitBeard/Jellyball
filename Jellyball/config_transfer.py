"""Complete configuration export and import (X8).

Export version "2.1" holds the channels (as 2.0 did), the Multi-View
channels, and the dashboard settings that are safe to move between machines.
Secrets (Jellyfin API key, webhook URL, bot token, chat id, the dashboard
password hash) are never exported; the export lists which ones were set so
the user knows to re-enter them.

Import accepts both the 2.0 teams-only export and this one. It never touches
an existing channel, never imports a secret, applies every setting through
the same validation the dashboard forms use, and applies them live.
"""

import asyncio
import json
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import catalog
from alerts import get_jellyfin_config, request_jellyfin_guide_refresh_if_changed
from channels import _add_manual_team, _set_catalog_entry_enabled
from config import _clean_label, _safe_team_id, _validate_upstream_url
from db import (
    _bulk_set_favorite_sync,
    _export_teams_sync,
    get_setting_async,
    load_multiview_channels,
    save_multiview_channel_async,
    set_setting_async,
)
from diagnostics import SECRET_SETTING_KEYS
from failover import _scrape_lifecycle_defaults
from multiview import MULTIVIEW_LAYOUTS, _forget_multiview_channel, _multiview_member_validation
from scrapers import ACTIVE_PROVIDERS, HtmlAggregatorScraper, _provider_url_setting_key, _set_provider_url_override
from state import new_channel_state, stream_state
from tunables import TUNABLES, _apply_tunable, _coerce_tunable
from version import __version__

EXPORT_VERSION = "2.1"
IMPORT_MAX_TEAMS = 500
IMPORT_MAX_MULTIVIEW = 100
IMPORT_MAX_BYTES = 2 * 1024 * 1024

# Settings that are secrets: never exported, never imported.
SECRET_KEYS = SECRET_SETTING_KEYS
_FLAG_KEYS = ("show_offseason_channels", "provider_rotation_mode", "update_check_enabled")


def _aggregators() -> Dict[str, HtmlAggregatorScraper]:
    return {p.name: p for p in ACTIVE_PROVIDERS if isinstance(p, HtmlAggregatorScraper)}


def _exportable_keys() -> List[str]:
    keys = [t.key for t in TUNABLES] + list(_FLAG_KEYS) + ["jellyfin_url", "jellyfin_task_id"]
    keys += [_provider_url_setting_key(name) for name in _aggregators()]
    return keys


def _load_multiview_rows() -> List[dict]:
    rows = []
    for channel_id, name, layout, members, audio, tvg_id, group_title, logo_url in load_multiview_channels():
        try:
            member_ids = json.loads(members or "[]")
        except (TypeError, ValueError):
            member_ids = []
        rows.append({
            "channel_id": channel_id, "name": name, "layout": layout,
            "member_team_ids": [str(m) for m in member_ids] if isinstance(member_ids, list) else [],
            "active_audio_team_id": audio or "", "tvg_id": tvg_id or "",
            "group_title": group_title or "", "logo_url": logo_url or "",
        })
    return rows


async def build_export() -> Dict[str, Any]:
    teams = await asyncio.to_thread(_export_teams_sync)
    multiview_channels = await asyncio.to_thread(_load_multiview_rows)
    settings: Dict[str, str] = {}
    for key in _exportable_keys():
        value = await get_setting_async(key, "")
        if value != "":
            settings[key] = value
    redacted = sorted([key for key in SECRET_KEYS if await get_setting_async(key, "")])
    return {
        "version": EXPORT_VERSION,
        "app_version": __version__,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "teams": teams,
        "multiview_channels": multiview_channels,
        "settings": settings,
        "redacted_settings": redacted,
    }


class ConfigImportError(ValueError):
    """The body is not a Jellyball export."""


def parse_import_body(raw: bytes) -> dict:
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ConfigImportError("not JSON") from exc
    if not isinstance(body, dict):
        raise ConfigImportError("not an object")
    if not isinstance(body.get("teams", []), list):
        raise ConfigImportError("teams must be a list")
    for optional in ("multiview_channels",):
        if not isinstance(body.get(optional, []), list):
            raise ConfigImportError(f"{optional} must be a list")
    if not isinstance(body.get("settings", {}), dict):
        raise ConfigImportError("settings must be an object")
    return body


async def _import_teams(teams: list, catalog_entries: List[dict]) -> Dict[str, Any]:
    """The 2.0 behaviour, unchanged; also returns exported id -> live id."""
    catalog_by_key = {entry["catalog_key"]: entry for entry in catalog_entries}
    imported, skipped, favorites = 0, 0, []
    id_map: Dict[str, str] = {}
    for team in teams[:IMPORT_MAX_TEAMS]:
        if not isinstance(team, dict):
            skipped += 1
            continue
        exported_id = str(team.get("team_id") or "")
        requested_id = _safe_team_id(str(team.get("team_id") or team.get("name") or ""))
        if requested_id in stream_state:
            if exported_id:
                id_map[exported_id] = requested_id
            skipped += 1
            continue
        catalog_entry = catalog_by_key.get(str(team.get("catalog_key") or ""))
        if catalog_entry is not None:
            # A catalog channel: enable the real catalog entry (keeps its
            # search terms, schedule and guide metadata) instead of a copy.
            if exported_id:
                id_map[exported_id] = catalog_entry["team_id"]
            if catalog_entry["team_id"] in stream_state:
                skipped += 1
                continue
            await _set_catalog_entry_enabled(catalog_entry, True)
            imported += 1
            if team.get("is_favorite"):
                favorites.append(catalog_entry["team_id"])
            continue
        team_id = await _add_manual_team(
            str(team.get("name") or ""),
            str(team.get("query") or ""),
            team_id=requested_id,
            logo_url=str(team.get("logo_url") or ""),
            category=str(team.get("category") or "imported"),
        )
        if team_id is None:
            skipped += 1
            continue
        if exported_id:
            id_map[exported_id] = team_id
        imported += 1
        if team.get("is_favorite"):
            favorites.append(team_id)
    skipped += max(0, len(teams) - IMPORT_MAX_TEAMS)
    if favorites:
        await asyncio.to_thread(_bulk_set_favorite_sync, favorites, True)
    return {"imported": imported, "skipped": skipped, "id_map": id_map}


def _used_tvg_ids() -> set:
    return {str(d.get("tvg_id")) for d in stream_state.values() if d.get("tvg_id")} | set(stream_state)


async def _import_multiview(rows: list, id_map: Dict[str, str]) -> Dict[str, int]:
    imported = skipped = 0
    for row in rows[:IMPORT_MAX_MULTIVIEW]:
        if not isinstance(row, dict):
            skipped += 1
            continue
        name = _clean_label(row.get("name"), 80)
        layout = str(row.get("layout") or "")
        raw_members = row.get("member_team_ids")
        if not name or layout not in MULTIVIEW_LAYOUTS or not isinstance(raw_members, list):
            skipped += 1
            continue
        members = [id_map.get(str(m), str(m)) for m in raw_members]
        if _multiview_member_validation(members) or MULTIVIEW_LAYOUTS[layout]["count"] != len(members):
            skipped += 1
            continue
        channel_id = _safe_team_id(str(row.get("channel_id") or f"multiview_{name}"))
        if channel_id in stream_state:
            skipped += 1
            continue
        audio = id_map.get(str(row.get("active_audio_team_id") or ""), str(row.get("active_audio_team_id") or ""))
        audio = audio if audio in members else members[0]
        # Keep the exported guide id (Jellyfin's channel mapping) unless another channel has it.
        tvg_id = _clean_label(row.get("tvg_id"), 120)
        if tvg_id in _used_tvg_ids():
            tvg_id = ""
        group_title = _clean_label(row.get("group_title"), 80) or "Multi-View"
        logo_url = str(row.get("logo_url") or "").strip()
        logo_url = (_validate_upstream_url(logo_url) or "") if logo_url else ""
        _forget_multiview_channel(channel_id)
        await save_multiview_channel_async(channel_id, name, layout, members, audio, tvg_id, group_title, logo_url)
        stream_state[channel_id] = new_channel_state(
            name=name,
            query="",
            type="multiview",
            candidates=[{"synthetic": True}],
            logo_url=logo_url,
            category="multiview",
            content_type="multiview",
            always_live=True,
            tvg_id=tvg_id,
            group_title=group_title,
            layout=layout,
            member_team_ids=members,
            active_audio_team_id=audio,
            **_scrape_lifecycle_defaults(),
        )
        imported += 1
    skipped += max(0, len(rows) - IMPORT_MAX_MULTIVIEW)
    return {"imported": imported, "skipped": skipped}


async def _apply_setting(key: str, value: Any) -> Optional[str]:
    """Apply one imported setting; returns why it was skipped, or None."""
    if key in SECRET_KEYS:
        return "secret settings are never imported"
    if not isinstance(value, (str, int, float)) or isinstance(value, bool):
        return "not a plain value"
    text = str(value).strip()
    tunable = next((t for t in TUNABLES if t.key == key), None)
    if tunable is not None:
        if text == "":
            return "empty"
        coerced = _coerce_tunable(tunable, text)
        if coerced is None:
            return "not a number"
        await set_setting_async(tunable.key, str(coerced))
        _apply_tunable(tunable, coerced)
        return None
    if key in _FLAG_KEYS:
        if text not in ("0", "1"):
            return "must be 0 or 1"
        await set_setting_async(key, text)
        if key == "show_offseason_channels" and catalog.SHOW_OFFSEASON_CHANNELS != (text == "1"):
            catalog.SHOW_OFFSEASON_CHANNELS = text == "1"
            request_jellyfin_guide_refresh_if_changed()
        return None
    providers = {_provider_url_setting_key(name): provider for name, provider in _aggregators().items()}
    if key in providers:
        validated = _validate_upstream_url(text) if text else ""
        if text and not validated:
            return "not a public http(s) URL"
        url = (validated or "").rstrip("/")
        await set_setting_async(key, url)
        _set_provider_url_override(providers[key], url)
        return None
    if key == "jellyfin_url":
        url = text.rstrip("/")
        if not _validate_upstream_url(url, allow_private=True):
            return "not an http(s) URL"
        current = await get_jellyfin_config()
        old_host = urllib.parse.urlsplit(current["jellyfin_url"]).netloc.lower()
        new_host = urllib.parse.urlsplit(url).netloc.lower()
        # As on the dashboard form: a saved API key must never follow the URL to another host.
        if current["jellyfin_api_key"] and new_host != old_host:
            return "another Jellyfin server: change it on the dashboard and re-enter the API key"
        await set_setting_async("jellyfin_url", url)
        return None
    if key == "jellyfin_task_id":
        await set_setting_async(key, _clean_label(text, 80))
        return None
    return "unknown setting"


async def _import_settings(settings: dict) -> Dict[str, Any]:
    applied: List[str] = []
    skipped: Dict[str, str] = {}
    for key, value in list(settings.items())[:500]:
        reason = await _apply_setting(str(key), value)
        if reason is None:
            applied.append(str(key))
        else:
            skipped[_clean_label(key, 80)] = reason
    return {"applied": sorted(applied), "skipped": skipped}


async def apply_import(body: dict, catalog_entries: List[dict]) -> Dict[str, Any]:
    """Import settings, then channels, then Multi-View channels (whose members
    may be channels this same import created)."""
    settings_result = await _import_settings(body.get("settings") or {})
    teams_result = await _import_teams(body.get("teams") or [], catalog_entries)
    multiview_result = await _import_multiview(body.get("multiview_channels") or [], teams_result["id_map"])
    if teams_result["imported"] or multiview_result["imported"]:
        request_jellyfin_guide_refresh_if_changed()
    return {
        "status": "imported",
        "count": teams_result["imported"],
        "skipped": teams_result["skipped"],
        "multiview": multiview_result,
        "settings": settings_result,
    }
