"""JSON / text API routes: /healthz, /api/*, /metrics."""

import asyncio
import json
import os
import shutil
import time
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

import config
from config import _log_failure, _safe_team_id, LOG_FILE
from state import is_multiview, stream_state
import provider_tools
from db import (
    _bulk_set_favorite_sync,
    _connect_db,
    _export_teams_sync,
    _performance_stats_sync,
    _playback_stats_sync,
    _record_stream_test_sync,
)
from security import verify_dashboard_auth
from catalog import _season_resume_label, get_catalog_entries, is_stream_window_active
from alerts import request_jellyfin_guide_refresh_if_changed
from jellyfin_client import load_status as load_jellyfin_status
from updates import _update_available, _UPDATE_STATE
import engine_stats
import epg
import ffmpeg_proc
import placeholder
from sessions import SESSIONS
from multiview import _multiview_cooldown_remaining, _MULTIVIEW_FAILURES, _MULTIVIEW_PROCESSES
from failover import (
    _session_on_placeholder,
    EMERGENCY_RESCRAPE_COUNTS,
    EMERGENCY_RESCRAPE_SECONDS_TOTAL,
    FAILOVER_DEFERRED_COUNTS,
    FAILOVER_REASON_COUNTS,
)
from channels import _add_manual_team, _set_catalog_entry_enabled
from tunables import _advanced_settings_snapshot
from scrapers import provider_breaker_snapshot
from stream_extractor import PLAYWRIGHT_MAX_PAGES, playwright_pages_in_use
from version import __version__

router = APIRouter()


@router.get("/healthz")
async def healthz():
    """Unauthenticated liveness probe (Docker healthcheck, installer, tray).

    Deliberately in-memory only: if this answers, the process is alive.
    Use /readyz when you need to know the app can actually serve traffic.
    """
    return {"status": "ok", "app": "jellyball", "version": __version__}


# --- Readiness probe -----------------------------------------------------------
# /readyz is the counterpart to /healthz: liveness says "the process is up",
# readiness says "the process can do its job". Checks are ordered cheapest
# first and the whole probe is bounded by _READYZ_TIMEOUT_SECONDS so a wedged
# subsystem degrades the answer instead of hanging the probe.

_READYZ_TIMEOUT_SECONDS = 10.0
_READYZ_DISK_WARN_BYTES = 500 * 1024 * 1024
_READYZ_DISK_FAIL_BYTES = 50 * 1024 * 1024


def _check_database() -> dict:
    """The DB must be readable AND writable (take and release the write lock)."""
    try:
        conn = _connect_db()
        conn.execute("SELECT 1").fetchone()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("ROLLBACK")
        return {"status": "ok"}
    except Exception as exc:  # locked, corrupt, or unwritable data dir
        return {"status": "fail", "detail": type(exc).__name__}


def _check_disk() -> dict:
    """Free space on the data volume: fail when critically low, warn before that."""
    try:
        free = shutil.disk_usage(str(config.DATA_DIR)).free
    except Exception as exc:
        return {"status": "fail", "detail": type(exc).__name__}
    if free < _READYZ_DISK_FAIL_BYTES:
        return {"status": "fail", "detail": f"only {free // (1024 * 1024)} MiB free"}
    if free < _READYZ_DISK_WARN_BYTES:
        return {"status": "warn", "detail": f"only {free // (1024 * 1024)} MiB free"}
    return {"status": "ok", "free_bytes": free}


def _check_ffmpeg() -> dict:
    """ffmpeg is only needed for Multi-View compositing, so a miss degrades."""
    path = ffmpeg_proc.FFMPEG_PATH
    found = bool(ffmpeg_proc.FFMPEG_AVAILABLE) or bool(shutil.which(path))
    if found:
        return {"status": "ok", "path": path}
    return {"status": "warn", "detail": f"not found at {path}"}


def _run_readiness_checks() -> dict:
    return {
        "database": _check_database(),
        "disk": _check_disk(),
        "ffmpeg": _check_ffmpeg(),
    }


@router.get("/readyz")
async def readyz():
    """Unauthenticated readiness probe: DB writability, disk space, ffmpeg.

    Returns 200 with per-check detail; 503 when a critical check (database,
    critically low disk) fails so orchestrators stop routing traffic here.
    """
    try:
        checks = await asyncio.wait_for(
            asyncio.to_thread(_run_readiness_checks), timeout=_READYZ_TIMEOUT_SECONDS
        )
    except Exception as exc:
        checks = {"probe": {"status": "fail", "detail": type(exc).__name__}}
    status = "ok"
    for check in checks.values():
        if check.get("status") == "fail":
            status = "fail"
            break
        if check.get("status") in ("warn", "degraded"):
            status = "degraded"
    code = 503 if status == "fail" else 200
    return JSONResponse(
        status_code=code,
        content={"status": status, "app": "jellyball", "version": __version__, "checks": checks},
    )


@router.get("/api/status")
async def api_status(auth: bool = Depends(verify_dashboard_auth)):
    """Return a compact status snapshot for local dashboards and health checks."""
    channels = []
    for team_id, data in stream_state.items():
        if data.get("type") == "multiview":
            # Multi-View cards are rendered separately (no .channel-card class,
            # a different status model - running/stopped rather than
            # healthy/searching) and are polled via /api/ffmpeg-status instead.
            # Including them here would make applyStatusSnapshot()'s channel
            # count permanently mismatch the .channel-card DOM count, which
            # forces a reload-loop on every 5s poll for as long as any
            # Multi-View channel exists.
            continue
        candidates = data.get("candidates", [])
        active_index = data.get("active_index", 0)
        active = candidates[active_index] if 0 <= active_index < len(candidates) else None
        session = SESSIONS.peek(team_id)
        guide = epg.now_next_for_channel(team_id, data)
        # Per-candidate quality for the 5s snapshot patcher, so it can render
        # the same quality badges the server-side channel cards show.
        candidate_entries = [
            {
                "provider": cand.get("provider"),
                "match_title": cand.get("match_title"),
                "quality": dict(cand.get("quality") or {}) or None,
                "active": idx == active_index,
            }
            for idx, cand in enumerate(candidates)
        ]
        channels.append({
            "team_id": team_id,
            "name": data.get("name", team_id),
            "now_title": guide["now"],
            "next_title": guide["next"],
            "healthy": bool(data.get("is_healthy")),
            "watching": bool(session is not None and session.is_watched()),
            "on_placeholder": bool(session is not None and session.is_watched() and _session_on_placeholder(session)),
            "exhausted": bool(data.get("exhausted")),
            "failover_count": int(data.get("failover_count", 0)),
            "candidate_count": len(candidates),
            "candidates": candidate_entries,
            "active_provider": active.get("provider") if active else None,
            "stream_window_active": is_stream_window_active(data),
            "start_time": data.get("start_time", ""),
            "stop_time": data.get("stop_time", ""),
            "schedule_status": data.get("schedule_status", ""),
            "season_resume_label": _season_resume_label(data.get("category", "")),
            "category": data.get("category", "custom"),
            "content_type": data.get("content_type", "team"),
            "always_live": bool(data.get("always_live")),
            "catalog_key": data.get("catalog_key", ""),
            "scrape_in_progress": bool(data.get("scrape_in_progress", False)),
            "last_scrape_started": data.get("last_scrape_started", 0.0),
            "last_scrape_completed": data.get("last_scrape_completed", 0.0),
            "scrape_result": data.get("scrape_result", "pending"),
            "scrape_error": data.get("scrape_error", ""),
        })

    return {
        "status": "ok",
        "port": config.PORT,
        "channels": channels,
        "channel_count": len(channels),
    }


def _session_status(channel_id: str, snapshot: dict) -> dict:
    data = stream_state.get(channel_id) or {}
    candidates = data.get("candidates") or []
    active_index = data.get("active_index", 0)
    active = candidates[active_index] if 0 <= active_index < len(candidates) else {}
    source_key = snapshot.get("source_key") or []
    return {
        **snapshot,
        "name": data.get("name", channel_id),
        "on_placeholder": bool(source_key and source_key[0] == "placeholder"),
        "exhausted": bool(data.get("exhausted")),
        "active_provider": active.get("provider"),
        "codec_signature": list(active.get("codec_signature") or []) or None,
        "failover_count": int(data.get("failover_count", 0)),
        "last_failover": data.get("last_failover"),
        "candidate_count": len(candidates),
    }


@router.get("/api/sessions")
async def api_sessions(auth: bool = Depends(verify_dashboard_auth)):
    """Live per-channel playback state: what each running channel session is
    playing, its throughput/latency, and its failover history."""
    return {
        "sessions": {cid: _session_status(cid, snap) for cid, snap in SESSIONS.snapshot().items()},
    }


def _prometheus_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", " ").replace('"', '\\"')


@router.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics(auth: bool = Depends(verify_dashboard_auth)):
    """Prometheus text exposition of the main health numbers (dashboard auth applies)."""
    lines = [
        "# HELP jellyball_channels Configured channels.",
        "# TYPE jellyball_channels gauge",
        f"jellyball_channels {len(stream_state)}",
        "# HELP jellyball_channel_healthy 1 when the channel's active source is healthy.",
        "# TYPE jellyball_channel_healthy gauge",
    ]
    for team_id, data in stream_state.items():
        lines.append(f'jellyball_channel_healthy{{channel="{_prometheus_label(team_id)}"}} {1 if data.get("is_healthy") else 0}')
    lines += [
        "# HELP jellyball_channel_failovers_total Failovers since start.",
        "# TYPE jellyball_channel_failovers_total counter",
    ]
    for team_id, data in stream_state.items():
        lines.append(f'jellyball_channel_failovers_total{{channel="{_prometheus_label(team_id)}"}} {int(data.get("failover_count", 0))}')
    snapshots = SESSIONS.snapshot()
    lines += [
        "# HELP jellyball_session_watched 1 while someone is watching the channel.",
        "# TYPE jellyball_session_watched gauge",
    ]
    for cid, snap in snapshots.items():
        lines.append(f'jellyball_session_watched{{channel="{_prometheus_label(cid)}"}} {1 if snap.get("watched") else 0}')
    lines += [
        "# HELP jellyball_session_bitrate_kbps Recent segment bitrate.",
        "# TYPE jellyball_session_bitrate_kbps gauge",
    ]
    for cid, snap in snapshots.items():
        if snap.get("bitrate_kbps") is not None:
            lines.append(f'jellyball_session_bitrate_kbps{{channel="{_prometheus_label(cid)}"}} {snap["bitrate_kbps"]}')
    lines += [
        "# HELP jellyball_session_segment_seconds_p95 95th percentile segment download time.",
        "# TYPE jellyball_session_segment_seconds_p95 gauge",
    ]
    for cid, snap in snapshots.items():
        if snap.get("segment_ms_p95") is not None:
            lines.append(
                f'jellyball_session_segment_seconds_p95{{channel="{_prometheus_label(cid)}"}} {snap["segment_ms_p95"] / 1000:.3f}'
            )

    memory_mb = sum(float(snap.get("memory_mb") or 0.0) for snap in snapshots.values())
    lines += [
        "# HELP jellyball_sessions_memory_mb Aggregate session buffer memory.",
        "# TYPE jellyball_sessions_memory_mb gauge",
        f"jellyball_sessions_memory_mb {memory_mb:.3f}",
        "# HELP jellyball_failover_reasons_total Failovers by reason since start.",
        "# TYPE jellyball_failover_reasons_total counter",
    ]
    for reason, count in sorted(FAILOVER_REASON_COUNTS.items()):
        lines.append(
            f'jellyball_failover_reasons_total{{reason="{_prometheus_label(reason)}"}} {int(count)}'
        )
    lines += [
        "# HELP jellyball_failover_deferred_total Failovers deferred (e.g. token refresh-first).",
        "# TYPE jellyball_failover_deferred_total counter",
    ]
    for reason, count in sorted(FAILOVER_DEFERRED_COUNTS.items()):
        lines.append(
            f'jellyball_failover_deferred_total{{reason="{_prometheus_label(reason)}"}} {int(count)}'
        )
    lines += [
        "# HELP jellyball_emergency_rescrape_total Emergency rescrapes since start.",
        "# TYPE jellyball_emergency_rescrape_total counter",
        f'jellyball_emergency_rescrape_total{{result="success"}} {int(EMERGENCY_RESCRAPE_COUNTS.get("success", 0))}',
        f'jellyball_emergency_rescrape_total{{result="empty_or_failed"}} {int(EMERGENCY_RESCRAPE_COUNTS.get("empty_or_failed", 0))}',
        "# HELP jellyball_emergency_rescrape_seconds_total Time spent in emergency rescrape.",
        "# TYPE jellyball_emergency_rescrape_seconds_total counter",
        f"jellyball_emergency_rescrape_seconds_total {EMERGENCY_RESCRAPE_SECONDS_TOTAL:.3f}",
        "# HELP jellyball_provider_breaker_open 1 when the provider circuit breaker is open.",
        "# TYPE jellyball_provider_breaker_open gauge",
    ]
    for provider, info in sorted(provider_breaker_snapshot().items()):
        lines.append(
            f'jellyball_provider_breaker_open{{provider="{_prometheus_label(provider)}"}} '
            f'{1 if info.get("open") else 0}'
        )
    lines += [
        "# HELP jellyball_remux_total Remux ingest events (3.0: fMP4/demuxed sources via ffmpeg), by event and provider.",
        "# TYPE jellyball_remux_total counter",
    ]
    for row in engine_stats.remux_counts():
        lines.append(
            f'jellyball_remux_total{{event="{_prometheus_label(row["event"])}",'
            f'provider="{_prometheus_label(row["provider"])}"}} {row["count"]}'
        )
    lines += [
        "# HELP jellyball_playwright_pages_in_use Open Playwright pages/contexts.",
        "# TYPE jellyball_playwright_pages_in_use gauge",
        f"jellyball_playwright_pages_in_use {playwright_pages_in_use()}",
        "# HELP jellyball_playwright_pages_max Configured Playwright page cap.",
        "# TYPE jellyball_playwright_pages_max gauge",
        f"jellyball_playwright_pages_max {PLAYWRIGHT_MAX_PAGES}",
    ]
    race_stats = engine_stats.cold_race_stats()
    tune_in = engine_stats.tune_in_latency_stats()
    lines += [
        "# HELP jellyball_cold_race_total Cold-start candidate races since start.",
        "# TYPE jellyball_cold_race_total counter",
        f"jellyball_cold_race_total {race_stats['total']}",
        "# HELP jellyball_cold_race_won_total Cold-start races that picked a healthy candidate.",
        "# TYPE jellyball_cold_race_won_total counter",
        f"jellyball_cold_race_won_total {race_stats['won']}",
        "# HELP jellyball_placeholder_fallbacks_total Tune-ins that started on the No-Signal placeholder.",
        "# TYPE jellyball_placeholder_fallbacks_total counter",
        f"jellyball_placeholder_fallbacks_total {engine_stats.placeholder_fallback_total()}",
        "# HELP jellyball_tune_in_latency_ms_avg Average time to first segment per session run.",
        "# TYPE jellyball_tune_in_latency_ms_avg gauge",
        f"jellyball_tune_in_latency_ms_avg {tune_in['avg_ms']}",
        "# HELP jellyball_tune_in_latency_ms_max Worst time to first segment per session run.",
        "# TYPE jellyball_tune_in_latency_ms_max gauge",
        f"jellyball_tune_in_latency_ms_max {tune_in['max_ms']}",
        "# HELP jellyball_tune_in_latency_ms Last time-to-first-segment per channel.",
        "# TYPE jellyball_tune_in_latency_ms gauge",
    ]
    for cid, snap in snapshots.items():
        if snap.get("tune_in_latency_ms") is not None:
            lines.append(f'jellyball_tune_in_latency_ms{{channel="{_prometheus_label(cid)}"}} {snap["tune_in_latency_ms"]}')
    ph = placeholder.placeholder_health()
    lines += [
        "# HELP jellyball_placeholder_healthy 1 when the No-Signal placeholder ffmpeg is running and ready.",
        "# TYPE jellyball_placeholder_healthy gauge",
        f"jellyball_placeholder_healthy {1 if ph['ready'] else 0}",
        "# HELP jellyball_placeholder_failures_total Consecutive No-Signal placeholder start failures.",
        "# TYPE jellyball_placeholder_failures_total counter",
        f"jellyball_placeholder_failures_total {ph['consecutive_failures']}",
    ]
    return "\n".join(lines) + "\n"


@router.get("/api/logs")
async def api_logs(limit: int = 200, auth: bool = Depends(verify_dashboard_auth)):
    """Return recent application log lines for the local dashboard."""
    limit = max(1, min(limit, 1000))
    lines = await asyncio.to_thread(_tail_log_file, limit)
    return {"logs": lines, "count": len(lines)}


def _tail_log_file(limit: int) -> List[str]:
    """Read only the end of the log (off the event loop) instead of the whole file."""
    try:
        with LOG_FILE.open("rb") as log_file:
            log_file.seek(0, os.SEEK_END)
            size = log_file.tell()
            chunk = min(size, max(64 * 1024, limit * 400))
            log_file.seek(size - chunk)
            data = log_file.read(chunk)
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").splitlines()
    if chunk < size and lines:
        lines = lines[1:]  # first line is probably partial
    return lines[-limit:]


@router.get("/api/ffmpeg-status", response_class=JSONResponse)
async def ffmpeg_status(auth: bool = Depends(verify_dashboard_auth)):
    channels = []
    for channel_id, data in stream_state.items():
        if data.get("type") != "multiview":
            continue
        entry = _MULTIVIEW_PROCESSES.get(channel_id)
        running = bool(entry and entry["process"].returncode is None and not entry.get("exited"))
        failure = _MULTIVIEW_FAILURES.get(channel_id)
        channels.append({
            "channel_id": channel_id,
            "name": data.get("name", channel_id),
            "running": running,
            "exit_code": entry.get("exit_code") if entry else None,
            "uptime_seconds": (time.monotonic() - entry["started_at"]) if entry and running else 0,
            "recent_log_lines": list(entry.get("log_lines", []))[-20:] if entry else [],
            "last_error": failure["last_error"] if failure else None,
            "failure_count": failure["count"] if failure else 0,
            "retry_in_seconds": round(_multiview_cooldown_remaining(channel_id)) if failure else 0,
        })
    return {"available": ffmpeg_proc.FFMPEG_AVAILABLE, "version": ffmpeg_proc.FFMPEG_VERSION_INFO, "channels": channels}


@router.get("/api/export-config", response_class=JSONResponse)
async def export_config(auth: bool = Depends(verify_dashboard_auth)):
    try:
        teams = await asyncio.to_thread(_export_teams_sync)
        config = {
            "version": "2.0",
            "app_version": __version__,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "teams": teams
        }
        return JSONResponse(config)
    except Exception as exc:
        _log_failure("export config", exc)
        raise HTTPException(status_code=500, detail="Export failed")


IMPORT_MAX_TEAMS = 500
IMPORT_MAX_BYTES = 2 * 1024 * 1024


@router.post("/api/import-config")
async def import_config(file: Request, auth: bool = Depends(verify_dashboard_auth)):
    """Import channels from an export. Goes through the same sanitizing path as
    the dashboard form and starts each new channel right away (imports used to
    sit inert in the DB until a restart). Existing channels are left alone."""
    try:
        raw = await file.body()
        if len(raw) > IMPORT_MAX_BYTES:
            raise HTTPException(status_code=413, detail="Import file too large")
        body = json.loads(raw.decode("utf-8"))
        teams = body.get("teams", []) if isinstance(body, dict) else None
        if not isinstance(teams, list):
            raise ValueError("teams must be a list")
    except HTTPException:
        raise
    except Exception as exc:
        _log_failure("import config", exc)
        raise HTTPException(status_code=400, detail="Import failed: not a Jellyball export")

    catalog_by_key = {entry["catalog_key"]: entry for entry in await get_catalog_entries()}
    imported, skipped, favorites = 0, 0, []
    for team in teams[:IMPORT_MAX_TEAMS]:
        if not isinstance(team, dict):
            skipped += 1
            continue
        requested_id = _safe_team_id(str(team.get("team_id") or team.get("name") or ""))
        if requested_id in stream_state:
            skipped += 1
            continue
        catalog_entry = catalog_by_key.get(str(team.get("catalog_key") or ""))
        if catalog_entry is not None:
            # A catalog channel: enable the real catalog entry (keeps its
            # search terms, schedule and guide metadata) instead of a copy.
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
        imported += 1
        if team.get("is_favorite"):
            favorites.append(team_id)
    skipped += max(0, len(teams) - IMPORT_MAX_TEAMS)
    if favorites:
        await asyncio.to_thread(_bulk_set_favorite_sync, favorites, True)
    if imported:
        request_jellyfin_guide_refresh_if_changed()
    return JSONResponse({"status": "imported", "count": imported, "skipped": skipped})


@router.get("/api/settings/advanced", response_class=JSONResponse)
async def get_advanced_settings(auth: bool = Depends(verify_dashboard_auth)):
    return {"settings": _advanced_settings_snapshot()}


@router.get("/api/cache-metrics", response_class=JSONResponse)
async def get_cache_metrics(auth: bool = Depends(verify_dashboard_auth)):
    # 3.0 removed the legacy chunk cache; keep the endpoint shape for old
    # dashboard builds, reporting the remux counters instead.
    return {"remux": engine_stats.remux_counts(), "hit_rate": 0, "total_hits": 0, "total_misses": 0}


@router.get("/api/performance-stats", response_class=JSONResponse)
async def get_performance_stats(auth: bool = Depends(verify_dashboard_auth)):
    try:
        return await asyncio.to_thread(_performance_stats_sync)
    except Exception as exc:
        _log_failure("get performance stats", exc)
        return {"playback_sessions_hour": 0, "failovers_hour": 0, "db_size_mb": 0, "provider_health": []}


@router.get("/api/playback-stats", response_class=JSONResponse)
async def get_playback_stats(auth: bool = Depends(verify_dashboard_auth)):
    try:
        return await asyncio.to_thread(_playback_stats_sync)
    except Exception as exc:
        _log_failure("get playback stats", exc)
        return {"top_watched_teams": [], "total_playbacks_week": 0}


@router.post("/api/test-stream/{team_id}")
async def test_stream(team_id: str, auth: bool = Depends(verify_dashboard_auth)):
    """Probe the channel's candidates for real (playlist plus one segment) instead of
    echoing the in-memory health flag; the flag is still returned for comparison."""
    try:
        data = stream_state.get(team_id)
        candidates = data.get("candidates", []) if data else []
        probe = await provider_tools.probe_candidates(candidates)
        # A Multi-View has no candidates of its own; it is live while its grid is healthy.
        is_live = probe["live"] > 0 or bool(is_multiview(data) and data.get("is_healthy", False))
        await asyncio.to_thread(_record_stream_test_sync, team_id, is_live, len(candidates))
        return {
            "team_id": team_id,
            "is_live": is_live,
            "candidate_count": len(candidates),
            "probed": probe["probed"],
            "live_candidates": probe["live"],
            "reported_healthy": bool(data and data.get("is_healthy", False)),
            "status": "✅ Live" if is_live else "❌ Offline",
        }
    except Exception as exc:
        _log_failure(f"test stream {team_id}", exc)
        raise HTTPException(status_code=500, detail="Stream test failed")


@router.get("/api/version")
async def api_version(auth: bool = Depends(verify_dashboard_auth)):
    return {
        "version": __version__,
        "latest": _UPDATE_STATE.get("latest") or None,
        "update_available": _update_available(),
        "release_url": _UPDATE_STATE.get("url") or None,
        # Jellyfin version last seen and the outcome of the last guide refresh
        # (both null until an attempt has been made; no secrets).
        "jellyfin": await load_jellyfin_status(),
    }
