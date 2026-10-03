"""Jellyball entry point and composition root.

Builds the FastAPI app (lifespan, middleware, static files, routers) and runs
it headless (console, Docker, Windows service) or under the system tray. The
feature code lives in the modules imported below; see their docstrings.
"""
# First: config resolves DATA_DIR, loads .env and starts logging before the
# other imports (jellyball_launcher.py sets JELLYBALL_DATA_DIR/TEMP before it
# imports this module).
import config

import asyncio
import json
import logging
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from playwright.async_api import async_playwright

from config import (
    _find_available_port,
    _log_failure,
    _LOG_LISTENER,
    _PLAYWRIGHT_PATH_SET_BY_APP,
    _resource_path,
    LOGGER,
)
import state
from state import _cancel_background_tasks, _spawn_background_task, new_channel_state, stream_state
from db import (
    _METRIC_WRITER,
    close_all_db_connections,
    get_setting,
    init_db,
    load_multiview_channels,
    load_teams,
    prune_database_logs,
)
import security
from security import _configure_dashboard_auth, CsrfOriginMiddleware
import scrapers
import catalog
from catalog import _special_channel_for, _sport_labeled_name, resolve_espn_logo
from updates import update_check_loop
import legacy_proxy
from legacy_proxy import _STARTUP_BUFFER_TASKS, PREFETCH_CONCURRENCY
from ffmpeg_proc import _check_ffmpeg_available, _child_process_creationflags, run_roots
import placeholder
from placeholder import _stop_placeholder_process
from sessions import SESSIONS
import multiview
from multiview import (
    _kill_orphaned_ffmpeg,
    _MULTIVIEW_PROCESSES,
    _stop_multiview_process,
    multiview_idle_monitor,
    multiview_output_sweeper,
    multiview_watchdog,
)
from failover import (
    _SCRAPE_IN_FLIGHT,
    _scrape_lifecycle_defaults,
    _start_team_scrape_loop,
    _TEAM_SCRAPE_TASKS,
    _TEAM_SCRAPE_WAKE_EVENTS,
    _TEAM_STATE_LOCKS,
    failover_monitor,
    STARTUP_SCRAPE_SPREAD_SECONDS,
)
from channels import enforce_scheduled_disables
import epg
from tunables import _load_tunable_overrides
import routes_stream
import routes_api
import routes_dashboard
# Feature-lane routers (empty until each lane adds its endpoints and dashboard
# cards). Imported statically: PyInstaller cannot see dynamic discovery.
import routes_engine
import routes_jellyfin
import routes_providers
import routes_setup
import routes_sports
from network_safety import bounded_float
from sports_matcher import get_team_search_terms
from version import __version__


def _install_loop_exception_filter() -> None:
    """Windows' Proactor event loop reports every client that drops its TCP
    connection abruptly (Jellyfin stopping a stream, ffmpeg reconnecting) as an
    unhandled ConnectionResetError traceback from _call_connection_lost. On a
    24/7 server that is pure log noise; keep every other loop error."""
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()

    def handler(event_loop, context):
        exc = context.get("exception")
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        if previous is not None:
            previous(event_loop, context)
        else:
            event_loop.default_exception_handler(context)

    loop.set_exception_handler(handler)


@asynccontextmanager
async def lifespan(app: FastAPI):

    _install_loop_exception_filter()
    init_db()
    _METRIC_WRITER.start()
    # Before wiping the run dirs: their pid files identify ffmpeg left running
    # by a crashed previous instance (and those would keep the files locked).
    _kill_orphaned_ffmpeg()
    for run_root in run_roots():
        shutil.rmtree(run_root.path, ignore_errors=True)
    multiview.MULTIVIEW_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    # Advanced settings saved from the dashboard override the env defaults.
    _load_tunable_overrides()
    catalog.SHOW_OFFSEASON_CHANNELS = get_setting("show_offseason_channels", "0") == "1"
    _spawn_background_task(update_check_loop(), "update check")
    stream_state.clear()
    _TEAM_SCRAPE_TASKS.clear()
    _SCRAPE_IN_FLIGHT.clear()
    _TEAM_STATE_LOCKS.clear()
    _STARTUP_BUFFER_TASKS.clear()
    legacy_proxy._PREFETCH_SEMAPHORE = asyncio.Semaphore(PREFETCH_CONCURRENCY)
    try:
        scrapers.PLAYWRIGHT_CLIENT = await async_playwright().start()
        scrapers.SHARED_BROWSER = await scrapers.PLAYWRIGHT_CLIENT.chromium.launch(headless=True)
    except Exception as exc:
        # HTTP scraping remains available when Chromium is not installed or cannot start.
        _log_failure("start Playwright; using HTTP extraction only", exc)
        if scrapers.PLAYWRIGHT_CLIENT:
            await scrapers.PLAYWRIGHT_CLIENT.stop()
        scrapers.PLAYWRIGHT_CLIENT = None
        scrapers.SHARED_BROWSER = None
    
    state.SHARED_HTTP_CLIENT = httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=150),
        timeout=12.0,
        follow_redirects=True,
        http2=True
    )
    # Separate pool for playback (playlists + segments): scrape bursts used to
    # exhaust the shared pool and make segment fetches hit PoolTimeout. Long
    # keepalive because segment polls are 2-6s apart (the 5s default expired
    # between polls, paying a new TCP+TLS handshake on most requests). Redirects
    # are followed manually so every hop is SSRF-validated.
    state.MEDIA_HTTP_CLIENT = httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=64, max_connections=200, keepalive_expiry=60.0),
        timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=3.0),
        follow_redirects=False,
        http2=True,
    )
    SESSIONS.sessions.clear()
    await _check_ffmpeg_available()
    for team in load_teams():
        team_id, name, query, category, source_id = team.team_id, team.name, team.query, team.category, team.source_id
        try:
            stored_search_terms = json.loads(team.search_terms or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            stored_search_terms = []
        if not isinstance(stored_search_terms, list):
            stored_search_terms = []
        search_terms = [str(term).strip() for term in stored_search_terms if str(term).strip()]
        if not search_terms:
            search_terms = get_team_search_terms(name, query, team_id)
        display_name = _sport_labeled_name(name, category or "")
        special_channel = _special_channel_for({"catalog_key": team.catalog_key})
        stream_state[team_id] = new_channel_state(
            name=display_name,
            query=query,
            logo_url=team.logo_url or resolve_espn_logo(name, category, source_id),
            start_time=team.start_time or "",
            stop_time=team.stop_time or "",
            category=category or "custom",
            source_id=source_id or "",
            content_type=team.content_type or "team",
            search_terms=search_terms,
            always_live=bool(team.always_live) or bool(special_channel),
            catalog_key=team.catalog_key or "",
            tvg_id=special_channel.tvg_id if special_channel else "",
            group_title=special_channel.group_title if special_channel else "",
            auto_disable_after=team.auto_disable_after or "",
            **_scrape_lifecycle_defaults(),
        )
        _start_team_scrape_loop(team_id, initial_delay=random.uniform(0.0, STARTUP_SCRAPE_SPREAD_SECONDS))

    for mv in load_multiview_channels():
        channel_id = mv.channel_id
        try:
            member_team_ids = json.loads(mv.member_team_ids or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            member_team_ids = []
        member_team_ids = [str(t) for t in member_team_ids if str(t).strip()]
        if len(member_team_ids) not in (2, 4):
            LOGGER.warning("Skipping multiview channel with invalid member count: %s", channel_id)
            continue
        missing = [t for t in member_team_ids if t not in stream_state]
        if missing:
            # Keep the Multi-View: removed members render as a "No Signal" pane.
            LOGGER.warning("Multi-View %s references removed channels %s; showing No Signal for them", channel_id, missing)
        stream_state[channel_id] = new_channel_state(
            name=mv.name,
            query="",
            type="multiview",
            logo_url=mv.logo_url,
            category="multiview",
            content_type="multiview",
            always_live=True,
            tvg_id=mv.tvg_id,
            group_title=mv.group_title or "Multi-View",
            layout=mv.layout,
            member_team_ids=member_team_ids,
            active_audio_team_id=(
                mv.active_audio_team_id if mv.active_audio_team_id in member_team_ids else member_team_ids[0]
            ),
            **_scrape_lifecycle_defaults(),
        )

    monitor_task = asyncio.create_task(failover_monitor(), name="failover monitor")
    prune_task = asyncio.create_task(prune_database_logs(), name="database log pruning")
    schedule_task = asyncio.create_task(enforce_scheduled_disables(), name="scheduled disable enforcement")
    multiview_idle_task = asyncio.create_task(multiview_idle_monitor(), name="multiview idle monitor")
    multiview_watchdog_task = asyncio.create_task(multiview_watchdog(), name="multiview watchdog")
    # Tracked background task: _cancel_background_tasks() stops it at shutdown.
    _spawn_background_task(multiview_output_sweeper(), "multiview output sweep")
    yield

    monitor_task.cancel()
    prune_task.cancel()
    schedule_task.cancel()
    multiview_idle_task.cancel()
    multiview_watchdog_task.cancel()
    scrape_tasks = list(_TEAM_SCRAPE_TASKS.values())
    for t in scrape_tasks:
        t.cancel()
    # Wait for tasks to safely wind down before closing shared clients.
    await asyncio.gather(
        monitor_task, prune_task, schedule_task, multiview_idle_task, multiview_watchdog_task, *scrape_tasks,
        return_exceptions=True,
    )
    _TEAM_SCRAPE_TASKS.clear()
    _TEAM_SCRAPE_WAKE_EVENTS.clear()
    _TEAM_STATE_LOCKS.clear()
    await SESSIONS.close_all()
    for channel_id in list(_MULTIVIEW_PROCESSES):
        await _stop_multiview_process(channel_id)
    await _stop_placeholder_process()
    # Ensure tracked prefetchers and other background work are stopped too.
    await _cancel_background_tasks()
    await _METRIC_WRITER.stop()
    close_all_db_connections()
    _STARTUP_BUFFER_TASKS.clear()
    for run_root in run_roots():
        shutil.rmtree(run_root.path, ignore_errors=True)
        
    if scrapers.SHARED_BROWSER:
        try:
            await scrapers.SHARED_BROWSER.close()
        except Exception as exc:
            _log_failure("close Playwright browser", exc)
    if scrapers.PLAYWRIGHT_CLIENT:
        try:
            await scrapers.PLAYWRIGHT_CLIENT.stop()
        except Exception as exc:
            _log_failure("stop Playwright", exc)
    if state.SHARED_HTTP_CLIENT:
        try:
            await state.SHARED_HTTP_CLIENT.aclose()
        except Exception as exc:
            _log_failure("close shared HTTP client", exc)
        state.SHARED_HTTP_CLIENT = None
    if state.MEDIA_HTTP_CLIENT:
        try:
            await state.MEDIA_HTTP_CLIENT.aclose()
        except Exception as exc:
            _log_failure("close media HTTP client", exc)
        state.MEDIA_HTTP_CLIENT = None
    scrapers.SHARED_BROWSER = None
    scrapers.PLAYWRIGHT_CLIENT = None
    legacy_proxy._PREFETCH_SEMAPHORE = None
    stream_state.clear()

def _is_expected_slow_request(scope) -> bool:
    """A channel playlist's first request waits for the session to start (up to
    STREAM_STARTUP_TIMEOUT); that's normal, not worth a warning each time."""
    path = scope.get("path") or ""
    return path.endswith(".m3u8") and (path.startswith("/stream/") or path.startswith("/multiview/"))


class RequestDiagnosticsMiddleware:
    """Pure ASGI middleware: logs 5xx and slow responses, turns unhandled errors
    into a 500. Replaces @app.middleware("http") (BaseHTTPMiddleware), which
    pushed every streamed body chunk through an extra memory stream and task
    hop - measurable overhead on the segment relay path."""

    def __init__(self, asgi_app) -> None:
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        status_holder = {"status": None}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                elapsed_ms = (time.perf_counter() - started) * 1000
                if message["status"] >= 500:
                    LOGGER.warning(
                        "HTTP failure method=%s path=%s status=%d duration_ms=%.0f",
                        scope.get("method"), scope.get("path"), message["status"], elapsed_ms,
                    )
                elif elapsed_ms >= 5000 and not _is_expected_slow_request(scope):
                    LOGGER.warning(
                        "Slow request method=%s path=%s status=%d duration_ms=%.0f",
                        scope.get("method"), scope.get("path"), message["status"], elapsed_ms,
                    )
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            if status_holder["status"] is not None:
                # Response already started (e.g. an upstream segment died
                # mid-body): re-raise so the server aborts the connection and
                # the client retries, instead of seeing a clean truncated body.
                raise
            elapsed_ms = (time.perf_counter() - started) * 1000
            _log_failure(f"HTTP {scope.get('method')} {scope.get('path')} ({elapsed_ms:.0f}ms)", exc, logging.ERROR)
            response = JSONResponse(status_code=500, content={"detail": "Internal server error"})
            await response(scope, receive, send)


app = FastAPI(title="Jellyball", version=__version__, lifespan=lifespan)
app.add_middleware(CsrfOriginMiddleware)
app.add_middleware(RequestDiagnosticsMiddleware)

# Dashboard static assets (the templates are routes_dashboard.TEMPLATES). Resolved
# with _resource_path so the source tree and the PyInstaller bundle both work.
app.mount("/static", StaticFiles(directory=str(_resource_path("static"))), name="static")

# Route registration order: routes_stream registers /stream/{team_id}.m3u8 and
# /stream/{team_id}/seg/... before the catch-all /stream/{team_id}.
app.include_router(routes_api.router)
app.include_router(routes_stream.router)
app.include_router(legacy_proxy.router)
app.include_router(epg.router)
app.include_router(routes_dashboard.router)
# Included last, so a lane can never shadow an established route.
for _lane_router in (
    routes_jellyfin.router,
    routes_sports.router,
    routes_providers.router,
    routes_setup.router,
    routes_engine.router,
):
    app.include_router(_lane_router)


# pystray/PIL are imported lazily in tray mode only: on a headless Linux host
# `import pystray` tries to open an X display at import time and crashes.
def _create_tray_image():
    from PIL import Image, ImageDraw

    logo_path = _resource_path("assets/jellyball-icon.png")
    try:
        return Image.open(logo_path).convert("RGBA").resize((64, 64), Image.Resampling.LANCZOS)
    except (OSError, ValueError) as exc:
        LOGGER.warning("Could not load JellyBall tray icon error=%s", type(exc).__name__)
    image = Image.new("RGBA", (64, 64), (15, 23, 42, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((8, 8, 56, 56), radius=12, fill=(37, 99, 235, 255))
    draw.ellipse((20, 20, 44, 44), fill=(255, 255, 255, 255))
    return image


PORT_BIND_WAIT_SECONDS = bounded_float(os.getenv("PORT_BIND_WAIT_SECONDS", "30"), 30.0, 0.0, 600.0)


def _port_is_free(host: str, port: int) -> bool:
    bind_host = "0.0.0.0" if host in ("", "0.0.0.0") else host
    family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.bind((bind_host, port))
        return True
    except OSError:
        return False


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    """A restart can race the previous instance releasing the port; wait for it
    instead of silently moving to another port (which broke Jellyfin's saved
    tuner and guide URLs)."""
    deadline = time.monotonic() + timeout
    while True:
        if _port_is_free(host, port):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.5)


def _existing_jellyball_instance(port: int) -> bool:
    """True if a Jellyball (e.g. the installed Windows service) already answers
    on this port."""
    try:
        response = httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=2.0)
        return response.status_code == 200 and response.json().get("app") == "jellyball"
    except Exception:
        return False


def build_server(host: str, port: int):
    import uvicorn

    _configure_dashboard_auth(host)

    try:
        import httptools  # noqa: F401
        http_impl = "httptools"
    except ImportError:
        http_impl = "auto"
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        reload=False,
        log_config=None,
        access_log=False,
        http=http_impl,
        lifespan="on",
        # Jellyfin's ffmpeg re-polls playlists every 2-6s; keep its connection
        # open between polls instead of reconnecting each time.
        timeout_keep_alive=30,
        # Streaming responses would otherwise hold shutdown open indefinitely.
        timeout_graceful_shutdown=10,
    )
    return uvicorn.Server(config)


def _headless_host() -> str:
    return os.getenv("JELLYBALL_HOST", "0.0.0.0" if sys.platform != "win32" else "127.0.0.1").strip() or "127.0.0.1"


def run_headless(stop_event: Optional[threading.Event] = None) -> int:
    """Run the server in the foreground (console mode, Docker, Windows service).
    Returns a process exit code; non-zero lets a service manager restart us."""
    if os.getenv("WEB_CONCURRENCY", "1") not in {"", "1"}:
        LOGGER.error("Jellyball must run with a single worker (WEB_CONCURRENCY=1)")
        return 2
    host = _headless_host()
    if not _wait_for_port(host, config.PORT, PORT_BIND_WAIT_SECONDS):
        LOGGER.error("Port %s on %s is in use; refusing to start on a different port", config.PORT, host)
        return 3
    server = build_server(host, config.PORT)
    if stop_event is not None:
        def _watch_stop() -> None:
            stop_event.wait()
            server.should_exit = True

        threading.Thread(target=_watch_stop, name="jellyball-stop-watch", daemon=True).start()
    LOGGER.info("Jellyball %s running headless at http://%s:%s", __version__, host, config.PORT)
    try:
        server.run()
    finally:
        if _LOG_LISTENER is not None:
            _LOG_LISTENER.stop()
    return 0 if server.started else 1


class TrayApplication:
    def __init__(self):
        import pystray

        self.server = None
        self.server_thread = None
        self.host = "127.0.0.1"
        self._pending_notices: List[str] = []
        self.icon = pystray.Icon(
            "jellyball",
            _create_tray_image(),
            "Jellyball Sports Proxy",
            pystray.Menu(
                pystray.MenuItem("View", self.open_gui, default=True),
                pystray.MenuItem("Restart", self.restart),
                pystray.MenuItem("Quit", self.quit),
            ),
        )

    def open_gui(self, icon, item):
        webbrowser.open(f"http://127.0.0.1:{config.PORT}/")

    def quit(self, icon, item):
        if self.server:
            self.server.should_exit = True
        icon.stop()

    def restart(self, icon, item):
        if self.server:
            self.server.should_exit = True

        def relaunch():
            if self.server_thread:
                self.server_thread.join(timeout=15)
            if getattr(sys, "frozen", False):
                command = [sys.executable, *sys.argv[1:]]
            else:
                command = [sys.executable, str(Path(sys.argv[0]).resolve()), *sys.argv[1:]]
            env = dict(os.environ)
            # PyInstaller 6 treats a child launched with the parent's environment
            # as a worker sharing the parent's (about to be deleted) extraction
            # directory; reset it. Also drop the browser path we derived from it.
            env["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
            if _PLAYWRIGHT_PATH_SET_BY_APP:
                env.pop("PLAYWRIGHT_BROWSERS_PATH", None)
            child = subprocess.Popen(command, close_fds=True, env=env, creationflags=_child_process_creationflags())
            # Only hand over once the new instance answers; if it dies (e.g. a
            # broken .env edit), keep this tray alive and bring the server back.
            deadline = time.monotonic() + 45.0
            while time.monotonic() < deadline:
                if _existing_jellyball_instance(config.PORT):
                    self.icon.stop()
                    return
                if child.poll() is not None:
                    break
                time.sleep(1.0)
            LOGGER.error("Restarted Jellyball did not come up (exit=%s); keeping this instance", child.poll())
            if child.poll() is None:
                child.terminate()
            self._notify("Restart failed - Jellyball kept running the previous instance. See jellyball.log.")
            self._start_server()

        threading.Thread(target=relaunch, name="jellyball-restart", daemon=True).start()

    def _notify(self, message: str) -> None:
        try:
            self.icon.notify(message, "Jellyball")
        except Exception as exc:  # not every tray backend supports notifications
            _log_failure("tray notification", exc, logging.DEBUG)

    def _on_icon_ready(self, icon) -> None:
        icon.visible = True
        for message in self._pending_notices:
            self._notify(message)
        self._pending_notices.clear()

    def _start_server(self) -> None:
        self.server = build_server(self.host, config.PORT)
        self.server_thread = threading.Thread(target=self.server.run, name="jellyball-server", daemon=True)
        self.server_thread.start()

    def run(self):
        host = os.getenv("JELLYBALL_HOST", "127.0.0.1").strip() or "127.0.0.1"
        self.host = host
        if not _port_is_free(host, config.PORT):
            if _existing_jellyball_instance(config.PORT):
                # The Windows service (or another tray instance) already runs
                # Jellyball here: just open its dashboard.
                LOGGER.info("Jellyball already running on port %s; opening its dashboard", config.PORT)
                webbrowser.open(f"http://127.0.0.1:{config.PORT}/")
                return
            if not _wait_for_port(host, config.PORT, 10.0):
                selected_port = _find_available_port(config.PORT)
                LOGGER.warning(
                    "Configured port %s is in use by another program; using %s for this desktop session "
                    "(Jellyfin tuner URLs pointing at %s will not work until it is free)",
                    config.PORT, selected_port, config.PORT,
                )
                self._pending_notices.append(
                    f"Port {config.PORT} is in use, so Jellyball is on port {selected_port} for now. "
                    f"Jellyfin URLs using port {config.PORT} won't work until it's free."
                )
                config.PORT = selected_port

        self._start_server()
        if security.DASHBOARD_AUTH_MODE == "generated":
            self._pending_notices.append(
                f"Dashboard password generated (user {security.DASHBOARD_USERNAME}); it is in {security.DASHBOARD_PASSWORD_FILE}."
            )
        LOGGER.info("Jellyball %s web GUI available at http://127.0.0.1:%s", __version__, config.PORT)
        threading.Timer(1.0, lambda: webbrowser.open(f"http://127.0.0.1:{config.PORT}/")).start()
        try:
            self.icon.run(setup=self._on_icon_ready)
        finally:
            self.server.should_exit = True
            self.server_thread.join(timeout=15)
            if _LOG_LISTENER is not None:
                _LOG_LISTENER.stop()


def _wants_headless() -> bool:
    return sys.platform != "win32" or os.getenv("JELLYBALL_HEADLESS", "").strip().lower() in {"1", "true", "yes"}


if __name__ == "__main__":
    if _wants_headless() or "--console" in sys.argv[1:]:
        sys.exit(run_headless())
    TrayApplication().run()
