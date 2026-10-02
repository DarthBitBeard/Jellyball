"""Jellyfin REST client for the few things Jellyball asks of Jellyfin: trigger the
"Refresh Guide" scheduled task and read the server version. Every attempt records
what happened (which step failed and why) so a broken integration is visible on
the dashboard and in the log instead of failing silently.

Authentication: Jellyfin documents `Authorization: MediaBrowser Token="<key>"`;
Jellyball up to 2.0.0 only sent the legacy `X-Emby-Token` header, and Jellyfin 12
deprecated legacy authorization mechanisms. The modern header is therefore tried
first with the legacy one as the fallback, and whichever works is remembered.
"""

import json
import re
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import httpx

from config import _log_failure, _validate_upstream_url, LOGGER
from db import get_setting_async, log_metric_event_async, set_setting_async
from version import __version__


AUTH_MODERN = "Authorization"
AUTH_LEGACY = "X-Emby-Token"
REQUEST_TIMEOUT = 8.0
LAST_REFRESH_KEY = "jellyfin_last_refresh"
SERVER_INFO_KEY = "jellyfin_server_info"
TESTED_RANGE_TEXT = "10.11-12.x"
# A repeating failure is logged again at most this often (it is always visible on
# the dashboard); a change in what fails is logged immediately.
REPEAT_LOG_SECONDS = 3600.0

# Which header style last worked, per Jellyfin host. In memory only: one extra
# request after a restart is cheaper than another persisted setting to keep right.
_PREFERRED_AUTH: Dict[str, str] = {}
_LOG_STATE: Dict[str, object] = {"key": None, "at": 0.0, "failing": False}


@dataclass
class RefreshResult:
    """Outcome of one guide-refresh attempt.

    `stage` names the step that failed: "config" (nothing to talk to), "reach"
    (cannot connect, timed out or was redirected), "auth" (API key rejected),
    "discover" (no Refresh Guide task) or "trigger" (the task would not start).
    """

    ok: bool
    stage: str = ""
    status: Optional[int] = None
    header: str = ""
    reason: str = ""
    task_id: str = ""


def parse_version(value: object) -> Tuple[int, ...]:
    numbers = re.findall(r"\d+", str(value or "").split("-", 1)[0])
    return tuple(int(n) for n in numbers[:3])


def is_tested_version(value: object) -> Optional[bool]:
    """True inside the range Jellyball is tested against (10.11 - 12.x), False
    outside it, None when the version cannot be read."""
    parts = parse_version(value)
    if len(parts) < 2:
        return None
    return (10, 11) <= (parts[0], parts[1]) and parts[0] <= 12


def auth_headers(mode: str, api_key: str) -> Dict[str, str]:
    headers = {"User-Agent": f"Jellyball/{__version__}", "Accept": "application/json"}
    if mode == AUTH_LEGACY:
        headers["X-Emby-Token"] = api_key
    else:
        headers["Authorization"] = (
            f'MediaBrowser Client="Jellyball", Device="Jellyball", DeviceId="jellyball", '
            f'Version="{__version__}", Token="{api_key}"'
        )
    return headers


def _make_client() -> httpx.AsyncClient:
    # No redirects: the API key must never follow a redirect to another host.
    return httpx.AsyncClient(timeout=REQUEST_TIMEOUT, follow_redirects=False)


def _describe_error(exc: Exception, base: str) -> str:
    # Host only, and never the exception text: nothing here may echo the API key.
    host = urllib.parse.urlsplit(base).netloc
    if isinstance(exc, httpx.TimeoutException):
        return f"Timed out talking to Jellyfin at {host}"
    if isinstance(exc, httpx.ConnectError):
        return f"Cannot connect to Jellyfin at {host}"
    return f"Request to Jellyfin at {host} failed ({type(exc).__name__})"


def _status_failure(status: int, phase: str, header: str) -> RefreshResult:
    if status in (401, 403):
        return RefreshResult(
            False, "auth", status, header,
            f"Jellyfin rejected the API key (HTTP {status}); both the Authorization and X-Emby-Token headers were tried",
        )
    if 300 <= status < 400:
        return RefreshResult(
            False, "reach", status, header,
            f"Jellyfin answered with a redirect (HTTP {status}); check the URL's scheme (http/https) and port",
        )
    if status == 404 and phase == "trigger":
        return RefreshResult(False, "trigger", status, header, "The Refresh Guide task was not found (HTTP 404)")
    return RefreshResult(False, phase, status, header, f"Jellyfin returned HTTP {status}")


async def fetch_public_info(client: httpx.AsyncClient, base: str) -> dict:
    """GET /System/Info/Public (needs no authentication): proves the server is
    reachable and says which version it is."""
    try:
        response = await client.get(
            f"{base}/System/Info/Public",
            headers={"User-Agent": f"Jellyball/{__version__}", "Accept": "application/json"},
        )
    except httpx.HTTPError as exc:
        return {"ok": False, "status": None, "reason": _describe_error(exc, base)}
    if response.status_code != 200:
        failure = _status_failure(response.status_code, "reach", "")
        return {"ok": False, "status": response.status_code, "reason": failure.reason}
    try:
        data = response.json()
    except ValueError:
        data = {}
    if not isinstance(data, dict):
        data = {}
    return {
        "ok": True,
        "status": 200,
        "version": str(data.get("Version") or ""),
        "server_name": str(data.get("ServerName") or ""),
        "product": str(data.get("ProductName") or ""),
    }


async def _authed_request(
    client: httpx.AsyncClient, method: str, url: str, api_key: str, base_key: str
) -> Tuple[httpx.Response, str]:
    """Send with the header style that last worked; if Jellyfin answers 401/403,
    retry once with the other style. Returns the response and the style used."""
    first = _PREFERRED_AUTH.get(base_key, AUTH_MODERN)
    order = (first, AUTH_LEGACY if first == AUTH_MODERN else AUTH_MODERN)
    response: Optional[httpx.Response] = None
    mode = first
    for mode in order:
        response = await client.request(method, url, headers=auth_headers(mode, api_key))
        if response.status_code not in (401, 403):
            _PREFERRED_AUTH[base_key] = mode
            break
    assert response is not None
    return response, mode


async def _discover_task(
    client: httpx.AsyncClient, base: str, base_key: str, api_key: str
) -> Tuple[str, Optional[RefreshResult]]:
    response, mode = await _authed_request(client, "GET", f"{base}/ScheduledTasks", api_key, base_key)
    if response.status_code != 200:
        return "", _status_failure(response.status_code, "discover", mode)
    try:
        tasks = response.json()
    except ValueError:
        return "", RefreshResult(False, "discover", 200, mode, "Jellyfin returned an unreadable task list")
    for task in tasks if isinstance(tasks, list) else []:
        if not isinstance(task, dict):
            continue
        if task.get("Key") == "RefreshGuide" or "refresh guide" in str(task.get("Name", "")).lower():
            task_id = str(task.get("Id") or "")
            if task_id:
                return task_id, None
    return "", RefreshResult(
        False, "discover", 200, mode,
        'No "Refresh Guide" scheduled task was found (is Live TV set up in Jellyfin?)',
    )


async def _run_refresh(
    client: httpx.AsyncClient, base: str, base_key: str, api_key: str, task_id: str
) -> RefreshResult:
    rediscovered = False
    while True:
        if not task_id:
            task_id, failure = await _discover_task(client, base, base_key, api_key)
            if failure is not None:
                return failure
            await set_setting_async("jellyfin_task_id", task_id)
        response, mode = await _authed_request(
            client, "POST", f"{base}/ScheduledTasks/Running/{task_id}", api_key, base_key
        )
        if response.status_code in (200, 204):
            return RefreshResult(True, "", response.status_code, mode, "", task_id)
        if response.status_code == 404 and not rediscovered:
            # The saved task id is stale (Jellyfin was reinstalled or upgraded):
            # look the task up again once instead of failing forever.
            rediscovered = True
            task_id = ""
            continue
        return _status_failure(response.status_code, "trigger", mode)


async def refresh_guide(cfg: dict) -> RefreshResult:
    """Ask Jellyfin to refresh its Live TV guide and record how that went.

    `cfg` is alerts.get_jellyfin_config(): jellyfin_url, jellyfin_api_key and
    jellyfin_task_id. An unconfigured integration returns quietly (it is the
    normal state for anyone refreshing the guide by hand)."""
    base = _validate_upstream_url(str(cfg.get("jellyfin_url") or ""), allow_private=True)
    api_key = str(cfg.get("jellyfin_api_key") or "").strip()
    task_id = str(cfg.get("jellyfin_task_id") or "").strip()
    if not base or not api_key:
        return RefreshResult(False, "config", reason="Jellyfin URL or API key is not configured")
    if any(ch in api_key for ch in '"\\\r\n'):
        result = RefreshResult(False, "config", reason="The Jellyfin API key contains invalid characters")
        await _record(result, None)
        return result
    base = base.rstrip("/")
    base_key = urllib.parse.urlsplit(base).netloc.lower()

    info: Optional[dict] = None
    try:
        async with _make_client() as client:
            info = await fetch_public_info(client, base)
            if info["ok"] or info["status"] is not None:
                # Reachable. An HTTP error from /System/Info/Public alone does not stop
                # the refresh: a reverse proxy may block /System/* yet allow the
                # authenticated endpoints, and those report their own precise errors.
                result = await _run_refresh(client, base, base_key, api_key, task_id)
            else:
                result = RefreshResult(False, "reach", None, "", info["reason"])
    except httpx.HTTPError as exc:
        result = RefreshResult(False, "reach", None, "", _describe_error(exc, base))
    except Exception as exc:
        _log_failure("refresh Jellyfin guide", exc)
        result = RefreshResult(False, "trigger", None, "", f"Unexpected error ({type(exc).__name__})")
    await _record(result, info)
    return result


def _log_outcome(result: RefreshResult) -> None:
    now = time.monotonic()
    if result.ok:
        if _LOG_STATE["failing"]:
            LOGGER.info("Jellyfin guide refresh works again (auth header: %s)", result.header)
        _LOG_STATE.update(key=None, at=0.0, failing=False)
        return
    key = (result.stage, result.status, result.reason)
    if _LOG_STATE["key"] == key and now - float(_LOG_STATE["at"]) < REPEAT_LOG_SECONDS:
        return
    LOGGER.warning("Jellyfin guide refresh failed stage=%s status=%s: %s", result.stage, result.status, result.reason)
    _LOG_STATE.update(key=key, at=now, failing=True)


async def _record(result: RefreshResult, info: Optional[dict]) -> None:
    now = time.time()
    _log_outcome(result)
    try:
        if info and info.get("ok"):
            await set_setting_async(SERVER_INFO_KEY, json.dumps({
                "version": info.get("version", ""),
                "server_name": info.get("server_name", ""),
                "product": info.get("product", ""),
                "checked_at": now,
            }))
        await set_setting_async(LAST_REFRESH_KEY, json.dumps({
            "ts": now,
            "ok": result.ok,
            "stage": result.stage,
            "status": result.status,
            "header": result.header,
            "reason": result.reason,
        }))
        if result.ok:
            await log_metric_event_async("Jellyfin", "API", "guide_refresh", "Triggered Live TV Guide Refresh task")
    except Exception as exc:
        _log_failure("record Jellyfin refresh status", exc)


def _loads(text: str) -> Optional[dict]:
    try:
        data = json.loads(text) if text else None
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


async def load_status() -> dict:
    """What the dashboard shows: the Jellyfin version last seen and the outcome
    of the last guide-refresh attempt (None for either until one has happened)."""
    last = _loads(await get_setting_async(LAST_REFRESH_KEY, ""))
    info = _loads(await get_setting_async(SERVER_INFO_KEY, ""))
    server = None
    if info and info.get("version"):
        server = {
            "version": str(info["version"]),
            "name": str(info.get("server_name") or ""),
            "tested": is_tested_version(info["version"]),
            "checked_at": info.get("checked_at"),
        }
    if last:
        ts = last.get("ts")
        last["when"] = time.strftime("%Y-%m-%d %H:%M", time.localtime(ts)) if isinstance(ts, (int, float)) else ""
    return {"server": server, "last_refresh": last}
