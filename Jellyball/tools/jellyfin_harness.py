"""Disposable real-Jellyfin harness: run a REAL Jellyfin server in Docker, finish its
setup wizard through the API, mint an API key, and point Jellyball's Jellyfin
client (or a developer's curl) at it.

    python tools/jellyfin_harness.py probe [--tag 12.1] [--out fixtures/jellyfin]
    python tools/jellyfin_harness.py up          # a ready server until Ctrl-C
    python tools/jellyfin_harness.py cleanup     # remove leftover containers

    from jellyfin_harness import JellyfinHarness
    with JellyfinHarness() as jf:
        jf.base_url, jf.api_key, jf.version      # a ready, authenticated server

Why it exists: the unit tests talk to a hand-written fake of Jellyfin, so they can
only prove Jellyball agrees with our own assumptions. This runs the official
`jellyfin/jellyfin` image so those assumptions can be checked against the real thing
(test_jellyfin_integration.py) and so the real request/response shapes can be
recorded (`probe`, fixtures/jellyfin/).

Image: `jellyfin/jellyfin:12.1`. JELLYBALL_JELLYFIN_IMAGE overrides it; a bare value
such as `10.11` is a tag of jellyfin/jellyfin, anything containing `/` or `:` is a
full image reference. A missing image is pulled on first use (about 745 MB).

Hygiene: containers are named `jellyball-it-<random>`, labelled `jellyball-it=1`,
publish only 127.0.0.1:<port Docker picks>:8096, keep their data on tmpfs, and are
removed (with their volumes) when the context exits, by an atexit hook, or by the
`cleanup` command.

Docker that is missing, stopped, or running Windows containers raises
HarnessUnavailable (tests turn that into a skip). Everything else that goes wrong
raises HarnessError, with the tail of the container log when there is one.
"""
from __future__ import annotations

import argparse
import atexit
import base64
import http.server
import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

IMAGE_REPO = "jellyfin/jellyfin"
DEFAULT_TAG = "12.1"
IMAGE_ENV = "JELLYBALL_JELLYFIN_IMAGE"
BIND_ENV = "JELLYBALL_HARNESS_BIND"
CONTAINER_PREFIX = "jellyball-it-"
CONTAINER_LABEL = "jellyball-it=1"
JELLYFIN_PORT = 8096
HOST_ALIAS = "host.docker.internal"
STARTUP_TIMEOUT = 120.0
PULL_TIMEOUT = 1800.0
API_KEY_APP = "jellyball-it"

CLIENT_NAME = "jellyball-it"
CLIENT_DEVICE = "jellyball-it-harness"
CLIENT_VERSION = "1.0.0"
SERVER_NAME = "jellyball-it"
ADMIN_NAME = "jellyball-admin"


class HarnessUnavailable(RuntimeError):
    """Docker (or the Jellyfin image) cannot be used here. Tests skip on this."""


class HarnessError(RuntimeError):
    """Docker works but the Jellyfin container or its API did not behave."""


# --------------------------------------------------------------------------- docker


def _docker(args: List[str], *, timeout: float = 60.0) -> "subprocess.CompletedProcess[str]":
    try:
        return subprocess.run(
            ["docker", *args], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout
        )
    except FileNotFoundError as exc:
        raise HarnessUnavailable("the docker CLI is not installed or not on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise HarnessUnavailable(f"`docker {args[0]}` did not answer within {timeout:.0f}s (is the daemon stuck?)") from exc
    except OSError as exc:
        raise HarnessUnavailable(f"the docker CLI could not be started ({exc})") from exc


def _first_line(text: str) -> str:
    for line in (text or "").splitlines():
        if line.strip():
            return line.strip()[:300]
    return ""


def check_docker() -> None:
    """Raise HarnessUnavailable unless a Linux-engine Docker daemon answers."""
    proc = _docker(["version", "--format", "{{.Server.Os}}"], timeout=30.0)
    if proc.returncode != 0:
        raise HarnessUnavailable(f"Docker is not usable: {_first_line(proc.stderr) or 'daemon not reachable'}")
    engine = proc.stdout.strip().lower()
    if engine != "linux":
        raise HarnessUnavailable(f"Docker is using the {engine or 'unknown'} container engine; the Jellyfin image needs Linux")


def resolve_image(tag: Optional[str] = None) -> str:
    value = (tag or os.environ.get(IMAGE_ENV, "") or DEFAULT_TAG).strip()
    return value if ("/" in value or ":" in value) else f"{IMAGE_REPO}:{value}"


def ensure_image(image: str) -> None:
    """Pull the image when it is not present locally (first use only)."""
    if _docker(["image", "inspect", "--format", "{{.Id}}", image]).returncode == 0:
        return
    print(f"[harness] pulling {image} (first use; this can take a few minutes)", file=sys.stderr, flush=True)
    proc = _docker(["pull", "--quiet", image], timeout=PULL_TIMEOUT)
    if proc.returncode != 0:
        raise HarnessUnavailable(f"could not pull {image}: {_first_line(proc.stderr) or 'unknown error'}")


_LIVE: Dict[str, None] = {}
_LIVE_LOCK = threading.Lock()
_ATEXIT_ARMED = False


def _arm_atexit() -> None:
    global _ATEXIT_ARMED
    if not _ATEXIT_ARMED:
        atexit.register(_remove_all_live)
        _ATEXIT_ARMED = True


def _remove_container(name: str) -> None:
    """Force-remove one container and its anonymous volumes. Never raises."""
    try:
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True, timeout=90)
    except (OSError, subprocess.SubprocessError):
        pass
    with _LIVE_LOCK:
        _LIVE.pop(name, None)


def _remove_all_live() -> None:
    with _LIVE_LOCK:
        names = list(_LIVE)
    for name in names:
        _remove_container(name)


def cleanup_leftovers() -> List[str]:
    """Remove every `jellyball-it-*` container (left by a killed run); returns their names."""
    proc = _docker(["ps", "-a", "--filter", f"name={CONTAINER_PREFIX}", "--format", "{{.Names}}"])
    if proc.returncode != 0:
        raise HarnessUnavailable(f"Docker is not usable: {_first_line(proc.stderr)}")
    names = [n.strip() for n in proc.stdout.splitlines() if n.strip().startswith(CONTAINER_PREFIX)]
    for name in names:
        _remove_container(name)
    return names


def _docker_bridge_gateway() -> str:
    proc = _docker(["network", "inspect", "bridge", "--format", "{{(index .IPAM.Config 0).Gateway}}"], timeout=20.0)
    value = proc.stdout.strip() if proc.returncode == 0 else ""
    return value if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", value) else ""


# ----------------------------------------------------------------------- local feeds

_PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
)

# (tvg-id, name, group-title). The ids mix the shapes Jellyball really emits: a plain
# slug, a dotted broadcaster id, and one ending in digits (which Jellyfin's M3U parser
# may read as a channel number).
FEED_CHANNELS: Tuple[Tuple[str, str, str], ...] = (
    ("nfl_buf", "Harness Bills", "NFL"),
    ("ESPN.us", "Harness ESPN", "24/7 Sports"),
    ("ncaaf_130", "Harness Wolverines", "College Football"),
)
FEED_PROGRAMMES_PER_CHANNEL = 36


def _xmltv_ts(moment: datetime) -> str:
    return moment.strftime("%Y%m%d%H%M%S +0000")


def build_m3u(base_url: str, channels: Tuple[Tuple[str, str, str], ...] = FEED_CHANNELS) -> str:
    """A playlist shaped like Jellyball's /playlist.m3u."""
    lines = ["#EXTM3U"]
    for tvg_id, name, group in channels:
        lines.append(
            f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{name}" tvg-logo="{base_url}/logo/{tvg_id}.png" '
            f'group-title="{group}",{name}'
        )
        lines.append(f"{base_url}/stream/{tvg_id}.m3u8")
    return "\n".join(lines) + "\n"


def build_xmltv(now: Optional[datetime] = None, channels: Tuple[Tuple[str, str, str], ...] = FEED_CHANNELS) -> str:
    """A guide shaped like Jellyball's /epg.xml: hourly programmes starting at the top
    of the previous hour, so the first ones always straddle "now" inside Jellyfin's
    guide window."""
    now = now or datetime.now(timezone.utc)
    start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    xml = ['<?xml version="1.0" encoding="UTF-8"?>', "<tv>"]
    for tvg_id, name, _group in channels:
        xml.append(f'  <channel id="{tvg_id}">')
        xml.append(f"    <display-name>{name}</display-name>")
        xml.append("  </channel>")
    for tvg_id, name, group in channels:
        for hour in range(FEED_PROGRAMMES_PER_CHANNEL):
            begin = start + timedelta(hours=hour)
            xml.append(
                f'  <programme channel="{tvg_id}" start="{_xmltv_ts(begin)}" stop="{_xmltv_ts(begin + timedelta(hours=1))}">'
            )
            xml.append(f"    <title>{name} Game {hour + 1}</title>")
            xml.append(f"    <category>{group}</category>")
            xml.append(f"    <desc>Synthetic programme {hour + 1} for {name}.</desc>")
            xml.append("  </programme>")
    xml.append("</tv>")
    return "\n".join(xml) + "\n"


class FeedServer:
    """Serves a tiny Jellyball-shaped M3U playlist, XMLTV guide, logos and stream stubs
    from this machine so a Jellyfin container can fetch them.

    The container reaches it as `http://host.docker.internal:<port>`. That name is
    built into Docker Desktop and is added with `--add-host=host.docker.internal:
    host-gateway` on Linux, where the server therefore has to listen on the Docker
    bridge address instead of 127.0.0.1 (or on 0.0.0.0 when that cannot be found).
    `requests` records every fetch, so a test can see whether and when Jellyfin
    really downloaded a feed.
    """

    def __init__(self, *, bind_host: Optional[str] = None) -> None:
        self.bind_host = bind_host or os.environ.get(BIND_ENV, "")
        self.requests: List[Tuple[str, str, str]] = []  # (method, path, user-agent)
        self.port = 0
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @staticmethod
    def default_bind_host() -> str:
        if sys.platform.startswith("linux"):
            return _docker_bridge_gateway() or "0.0.0.0"
        return "127.0.0.1"

    def fetches(self, path: str) -> List[str]:
        """User-agents of the GET requests seen for `path` so far."""
        with self._lock:
            return [agent for method, seen, agent in self.requests if method == "GET" and seen == path]

    def _handler(self) -> type:
        feeds = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def _serve(self, head_only: bool) -> None:
                path = self.path.split("?", 1)[0]
                with feeds._lock:
                    feeds.requests.append((self.command, path, self.headers.get("User-Agent", "")))
                base = f"http://{HOST_ALIAS}:{feeds.port}"
                status = 200
                if path == "/playlist.m3u":
                    body, ctype = build_m3u(base).encode(), "audio/x-mpegurl"
                elif path == "/epg.xml":
                    body, ctype = build_xmltv().encode(), "application/xml"
                elif path.startswith("/logo/"):
                    body, ctype = _PNG_1X1, "image/png"
                elif path.startswith("/stream/"):
                    body, ctype = b"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-ENDLIST\n", "application/vnd.apple.mpegurl"
                else:
                    status, body, ctype = 404, b"not found\n", "text/plain"
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if not head_only:
                    self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802 (http.server naming)
                self._serve(False)

            def do_HEAD(self) -> None:  # noqa: N802
                self._serve(True)

            def log_message(self, *args: Any) -> None:
                pass

        return Handler

    def start(self) -> "FeedServer":
        bind = self.bind_host or self.default_bind_host()
        try:
            self._server = http.server.ThreadingHTTPServer((bind, 0), self._handler())
        except OSError:
            if bind == "0.0.0.0":
                raise
            bind = "0.0.0.0"  # e.g. WSL2 reporting a bridge address that is not local
            self._server = http.server.ThreadingHTTPServer((bind, 0), self._handler())
        self._server.daemon_threads = True
        self.bind_host = bind
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, name="jellyball-it-feeds", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.shutdown()
            server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def __enter__(self) -> "FeedServer":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def container_url(self, path: str) -> str:
        """The URL the Jellyfin container uses to reach this server."""
        return f"http://{HOST_ALIAS}:{self.port}{path}"

    @property
    def m3u_url(self) -> str:
        return self.container_url("/playlist.m3u")

    @property
    def xmltv_url(self) -> str:
        return self.container_url("/epg.xml")


# ---------------------------------------------------------------------------- harness


def media_browser_header(device_id: str, token: str = "") -> str:
    """The `Authorization` value Jellyfin 12 reads: `MediaBrowser Client=..., Token=...`."""
    parts = [f'Client="{CLIENT_NAME}"', f'Device="{CLIENT_DEVICE}"', f'DeviceId="{device_id}"', f'Version="{CLIENT_VERSION}"']
    if token:
        parts.append(f'Token="{token}"')
    return "MediaBrowser " + ", ".join(parts)


class JellyfinHarness:
    """A throwaway, fully set-up Jellyfin server. Use as a context manager.

    After start(): base_url (http://127.0.0.1:<port>), api_key (an admin-level API
    key), version (e.g. "12.1.0"), server_id, plus admin_name/admin_password/admin_token
    for the user the wizard created. `timings` says where startup time went.
    """

    def __init__(self, tag: Optional[str] = None, *, startup_timeout: float = STARTUP_TIMEOUT) -> None:
        self.image = resolve_image(tag)
        self.startup_timeout = startup_timeout
        self.container_name = f"{CONTAINER_PREFIX}{secrets.token_hex(4)}"
        self.container_id = ""
        self.base_url = ""
        self.api_key = ""
        self.version = ""
        self.server_id = ""
        self.user_id = ""
        self.admin_name = ADMIN_NAME
        self.admin_password = secrets.token_urlsafe(18)  # random per run: a disposable server keeps no stored secret
        self.admin_token = ""
        self.device_id = f"jellyball-it-{secrets.token_hex(4)}"
        self.timings: Dict[str, float] = {}
        self.wizard_log: List[Dict[str, Any]] = []
        self._http: Optional[httpx.Client] = None
        self._feeds: Optional[FeedServer] = None

    # -- lifecycle ----------------------------------------------------------------

    def __enter__(self) -> "JellyfinHarness":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def start(self) -> "JellyfinHarness":
        began = time.monotonic()
        check_docker()
        ensure_image(self.image)
        self.timings["docker_ready"] = time.monotonic() - began
        _arm_atexit()
        try:
            self._run_container()
            self.timings["container_started"] = time.monotonic() - began
            self._wait_until_up()
            self.timings["server_up"] = time.monotonic() - began
            self._complete_wizard()
            self._authenticate()
            self._mint_api_key()
            self.timings["ready"] = time.monotonic() - began
        except BaseException as exc:
            tail = self.logs(40)
            self.stop()
            if isinstance(exc, (KeyboardInterrupt, SystemExit)) or not tail:
                raise
            raise HarnessError(f"{exc}\n--- container log (tail) ---\n{tail}") from exc
        return self

    def stop(self) -> None:
        if self._feeds is not None:
            self._feeds.stop()
            self._feeds = None
        if self._http is not None:
            self._http.close()
            self._http = None
        _remove_container(self.container_name)

    def logs(self, tail: int = 100) -> str:
        try:
            proc = _docker(["logs", "--tail", str(tail), self.container_name], timeout=30.0)
        except HarnessUnavailable:
            return ""
        return (proc.stdout + proc.stderr).strip() if proc.returncode == 0 else ""

    def _run_container(self) -> None:
        with _LIVE_LOCK:
            _LIVE[self.container_name] = None  # registered first: a half-created container is still removed
        proc = _docker([
            "run", "-d",
            "--name", self.container_name,
            "--label", CONTAINER_LABEL,
            "--add-host", f"{HOST_ALIAS}:host-gateway",
            "-p", f"127.0.0.1::{JELLYFIN_PORT}",
            # Jellyfin's data and cache are declared volumes; tmpfs keeps them off disk
            # and means nothing outlives the container.
            "--tmpfs", "/config", "--tmpfs", "/cache",
            self.image,
        ], timeout=120.0)
        if proc.returncode != 0:
            raise HarnessError(f"docker run failed: {_first_line(proc.stderr)}")
        self.container_id = proc.stdout.strip()
        deadline = time.monotonic() + 15.0
        port = ""
        while time.monotonic() < deadline and not port:
            mapped = _docker(["port", self.container_name, f"{JELLYFIN_PORT}/tcp"], timeout=20.0)
            found = re.search(r"127\.0\.0\.1:(\d+)", mapped.stdout)
            if found:
                port = found.group(1)
            else:
                time.sleep(0.25)
        if not port:
            raise HarnessError("Docker did not publish the Jellyfin port on 127.0.0.1")
        self.base_url = f"http://127.0.0.1:{port}"
        # No keep-alive: Jellyfin answers from a temporary start-up host and then swaps
        # to the real one, which kills any pooled connection. trust_env=False so a
        # corporate HTTP(S)_PROXY is never asked to reach 127.0.0.1.
        self._http = httpx.Client(
            base_url=self.base_url, timeout=30.0, trust_env=False,
            limits=httpx.Limits(max_keepalive_connections=0),
        )

    def _container_running(self) -> bool:
        proc = _docker(["inspect", "--format", "{{.State.Running}}", self.container_name], timeout=20.0)
        return proc.returncode == 0 and proc.stdout.strip() == "true"

    def _read_public_info(self) -> Optional[dict]:
        """GET /System/Info/Public; the real server's answer, or None while it is not up.

        Observed on 12.1: while starting, Jellyfin first answers 200 from a temporary
        host with camelCase keys (`version`, `id`), then drops the connection, then
        answers 503 "loading", and only then gives the real PascalCase 200. Only the
        PascalCase answer counts."""
        assert self._http is not None
        try:
            response = self._http.get("/System/Info/Public", timeout=5.0)
            info = response.json() if response.status_code == 200 else None
        except (httpx.HTTPError, ValueError):
            return None
        if isinstance(info, dict) and info.get("Version"):
            self.version = str(info["Version"])
            self.server_id = str(info.get("Id") or "")
            return info
        return None

    def _wait_until_up(self) -> None:
        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            if not self._container_running():
                raise HarnessError("the Jellyfin container stopped during startup")
            if self._read_public_info() is not None:
                return
            time.sleep(0.5)
        raise HarnessError(f"Jellyfin did not answer /System/Info/Public within {self.startup_timeout:.0f}s")

    # -- requests -----------------------------------------------------------------

    def request(self, method: str, path: str, *, auth: str = "modern", token: Optional[str] = None, **kwargs: Any) -> httpx.Response:
        """One request with exactly one kind of credentials.

        auth: "modern"  `Authorization: MediaBrowser Token="<token>"`
              "legacy"  `X-Emby-Token: <token>`
              "apikey"  `?ApiKey=<token>`
              "none"    no credentials (pass your own `headers` / `params`)
        `token` defaults to the API key. Other keyword arguments go to httpx.
        """
        assert self._http is not None, "harness not started"
        headers = dict(kwargs.pop("headers", None) or {})
        params = dict(kwargs.pop("params", None) or {})
        value = self.api_key if token is None else token
        if auth == "modern":
            headers["Authorization"] = f'MediaBrowser Token="{value}"'
        elif auth == "legacy":
            headers["X-Emby-Token"] = value
        elif auth == "apikey":
            params["ApiKey"] = value
        elif auth != "none":
            raise ValueError(f"unknown auth style {auth!r}")
        return self._http.request(method, path, headers=headers, params=params, **kwargs)

    def api(self, method: str, path: str, *, expect: Tuple[int, ...] = (200, 204), **kwargs: Any) -> Any:
        """An authenticated call that must succeed; returns the parsed JSON (None for 204)."""
        response = self.request(method, path, **kwargs)
        if response.status_code not in expect:
            raise HarnessError(f"{method} {path} -> HTTP {response.status_code}: {response.text[:300]}")
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    # -- setup steps --------------------------------------------------------------

    def _wizard(self, method: str, path: str, body: Optional[dict] = None) -> Any:
        """A startup-wizard call (no credentials needed until it completes). Right after
        Jellyfin first listens it can answer 503 or drop the connection while it swaps
        its start-up host for the real one, so both are retried for a while."""
        assert self._http is not None
        deadline = time.monotonic() + 30.0
        while True:
            try:
                response = self._http.request(method, path, json=body)
            except httpx.TransportError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)
                continue
            if response.status_code == 503 and time.monotonic() < deadline:
                time.sleep(0.5)
                continue
            if response.status_code not in (200, 204):
                raise HarnessError(f"{method} {path} -> HTTP {response.status_code}: {response.text[:300]}")
            data = response.json() if response.content else None
            step: Dict[str, Any] = {"step": f"{method} {path}", "status": response.status_code}
            if body is not None:
                step["request"] = body
            if data is not None:
                step["response"] = data
            self.wizard_log.append(step)
            return data

    def _complete_wizard(self) -> None:
        info = self._wizard("GET", "/System/Info/Public")
        if info.get("StartupWizardCompleted"):
            raise HarnessError("the fresh Jellyfin container reports its setup wizard as already completed")
        self._wizard("GET", "/Startup/Configuration")
        self._wizard("POST", "/Startup/Configuration", {
            "ServerName": SERVER_NAME, "UICulture": "en-US", "MetadataCountryCode": "US", "PreferredMetadataLanguage": "en",
        })
        # Reading the first user is what makes Jellyfin create it ("root"); POST renames it.
        self._wizard("GET", "/Startup/User")
        self._wizard("POST", "/Startup/User", {"Name": self.admin_name, "Password": self.admin_password})
        self._wizard("POST", "/Startup/RemoteAccess", {"EnableRemoteAccess": True})
        self._wizard("POST", "/Startup/Complete")

    def _authenticate(self) -> None:
        assert self._http is not None
        response = self._http.post(
            "/Users/AuthenticateByName",
            json={"Username": self.admin_name, "Pw": self.admin_password},
            headers={"Authorization": media_browser_header(self.device_id)},
        )
        if response.status_code != 200:
            raise HarnessError(f"AuthenticateByName -> HTTP {response.status_code}: {response.text[:300]}")
        data = response.json()
        self.admin_token = str(data["AccessToken"])
        self.user_id = str(data["User"]["Id"])

    def _mint_api_key(self) -> None:
        assert self._http is not None
        headers = {"Authorization": media_browser_header(self.device_id, self.admin_token)}
        created = self._http.post("/Auth/Keys", params={"app": API_KEY_APP}, headers=headers)
        if created.status_code != 204:
            raise HarnessError(f"POST /Auth/Keys -> HTTP {created.status_code}: {created.text[:300]}")
        listed = self._http.get("/Auth/Keys", headers=headers)
        if listed.status_code != 200:
            raise HarnessError(f"GET /Auth/Keys -> HTTP {listed.status_code}: {listed.text[:300]}")
        mine = [k for k in listed.json().get("Items", []) if k.get("AppName") == API_KEY_APP]
        if not mine:
            raise HarnessError("the API key just created is not in GET /Auth/Keys")
        self.api_key = str(max(mine, key=lambda k: str(k.get("DateCreated", "")))["AccessToken"])

    # -- Live TV and scheduled tasks ----------------------------------------------

    def start_feeds(self) -> FeedServer:
        """Start (once) the local M3U/XMLTV server and return it."""
        if self._feeds is None:
            self._feeds = FeedServer().start()
        return self._feeds

    def livetv_config(self) -> dict:
        """The Live TV configuration. Its TunerHosts and ListingProviders lists are how a
        configured tuner or guide source is found again: there is no GET /LiveTv/TunerHosts."""
        return self.api("GET", "/System/Configuration/livetv")

    def add_m3u_tuner(self, url: str, name: str = "Jellyball Harness", **extra: Any) -> dict:
        return self.api("POST", "/LiveTv/TunerHosts", json={"Type": "m3u", "Url": url, "FriendlyName": name, **extra})

    def add_xmltv_listing(self, path: str, **extra: Any) -> dict:
        return self.api("POST", "/LiveTv/ListingProviders", json={"Type": "xmltv", "Path": path, **extra})

    def delete_tuner(self, tuner_id: str) -> None:
        self.api("DELETE", "/LiveTv/TunerHosts", params={"id": tuner_id})

    def delete_listing(self, listing_id: str) -> None:
        self.api("DELETE", "/LiveTv/ListingProviders", params={"id": listing_id})

    def clear_livetv(self) -> None:
        """Delete every tuner and listing provider."""
        config = self.livetv_config()
        for tuner in config.get("TunerHosts", []):
            self.delete_tuner(tuner["Id"])
        for listing in config.get("ListingProviders", []):
            self.delete_listing(listing["Id"])

    def task(self, key: str) -> Optional[dict]:
        for task in self.api("GET", "/ScheduledTasks"):
            if task.get("Key") == key:
                return task
        return None

    def last_run_end(self, key: str) -> str:
        """EndTimeUtc of the task's last run ("" if it never ran): take it before starting
        the task and hand it to wait_task_finished()."""
        return str(((self.task(key) or {}).get("LastExecutionResult") or {}).get("EndTimeUtc") or "")

    def wait_task_finished(self, key: str, previous_end: str = "", timeout: float = 90.0) -> dict:
        """Wait until the task is Idle and has a run that ended after `previous_end`."""
        deadline = time.monotonic() + timeout
        task: Optional[dict] = None
        while time.monotonic() < deadline:
            task = self.task(key)
            result = (task or {}).get("LastExecutionResult") or {}
            if task and task.get("State") == "Idle" and result.get("EndTimeUtc") and result["EndTimeUtc"] != previous_end:
                return task
            time.sleep(0.4)
        raise HarnessError(f"scheduled task {key} did not finish within {timeout:.0f}s (last seen: {task and task.get('State')})")


# ------------------------------------------------------------------------------ probe


class Sanitizer:
    """Replaces every secret or instance-specific value in a JSON sample with a
    placeholder, and refuses to hand back text that still contains a secret."""

    def __init__(self) -> None:
        self._replacements: List[Tuple[str, str]] = []
        self._secrets: List[str] = []

    def add(self, value: str, placeholder: str, *, secret: bool = True) -> None:
        if value and len(value) >= 3:
            self._replacements.append((value, placeholder))
            if secret:
                self._secrets.append(value)

    def text(self, text: str) -> str:
        for value, placeholder in sorted(self._replacements, key=lambda item: -len(item[0])):
            text = text.replace(value, placeholder)
        return text

    def json_text(self, data: Any) -> str:
        text = self.text(json.dumps(data, indent=2, ensure_ascii=False))
        for value in self._secrets:
            if value in text:
                raise HarnessError("a secret survived sanitising; refusing to write the sample")
        return text + "\n"


class Probe:
    """Exercises the endpoints Jellyball cares about on a fresh server and records what
    the real thing answers, as sanitised JSON samples plus a short list of findings."""

    def __init__(self, jf: JellyfinHarness) -> None:
        self.jf = jf
        self.samples: Dict[str, Any] = {}
        self.findings: List[str] = []
        self.sanitizer = Sanitizer()
        self.sanitizer.add(jf.api_key, "<API_KEY>")
        self.sanitizer.add(jf.admin_token, "<USER_ACCESS_TOKEN>")
        self.sanitizer.add(jf.admin_password, "<PASSWORD>")
        self.sanitizer.add(jf.server_id, "<SERVER_ID>")
        self.sanitizer.add(jf.user_id, "<USER_ID>")
        self.sanitizer.add(jf.device_id, "<DEVICE_ID>")
        self.sanitizer.add(jf.container_name, "<CONTAINER>")
        self.sanitizer.add(jf.container_id[:12], "<CONTAINER_HOSTNAME>", secret=False)  # Jellyfin's default ServerName
        self.sanitizer.add(jf.base_url.removeprefix("http://"), "127.0.0.1:<PORT>", secret=False)

    def note(self, text: str) -> None:
        self.findings.append(text)
        print(f"  - {text}", flush=True)

    def save(self, name: str, data: Any) -> Any:
        self.samples[name] = data
        return data

    def get(self, name: str, path: str, **kwargs: Any) -> Any:
        """GET `path` with the API key, keep the JSON under `name`, return it."""
        return self.save(name, self.jf.api("GET", path, **kwargs))

    def run(self) -> "Probe":
        jf = self.jf
        print(f"Jellyfin {jf.version} at {jf.base_url} (container {jf.container_name})", flush=True)
        self._startup()
        self._auth()
        self._system()
        feeds = jf.start_feeds()
        self.sanitizer.add(f"{HOST_ALIAS}:{feeds.port}", f"{HOST_ALIAS}:<FEED_PORT>", secret=False)
        self._livetv(feeds)
        self._livetv_errors(feeds)
        return self

    # -- sections -----------------------------------------------------------------

    def _startup(self) -> None:
        self.save("Startup.wizard", self.jf.wizard_log)
        statuses = ", ".join(f"{s['step']} -> {s['status']}" for s in self.jf.wizard_log if s["step"].startswith("POST"))
        self.note(f"setup wizard over the API: {statuses}")

    def _auth(self) -> None:
        """Which credential styles does the real server accept? (GET /ScheduledTasks)"""
        jf = self.jf
        jellyball = 'Client="Jellyball", Device="Jellyball", DeviceId="jellyball", Version="2.1.0"'
        styles = [
            ('Authorization: MediaBrowser Token="<t>"', "headers", lambda t: {"Authorization": f'MediaBrowser Token="{t}"'}),
            (f'Authorization: MediaBrowser {jellyball}, Token="<t>"  (Jellyball modern)', "headers",
             lambda t: {"Authorization": f'MediaBrowser {jellyball}, Token="{t}"'}),
            ("Authorization: MediaBrowser Token=<t>  (unquoted)", "headers", lambda t: {"Authorization": f"MediaBrowser Token={t}"}),
            ("Authorization: Bearer <t>", "headers", lambda t: {"Authorization": f"Bearer {t}"}),
            ("X-Emby-Token: <t>  (Jellyball legacy)", "headers", lambda t: {"X-Emby-Token": t}),
            ("X-MediaBrowser-Token: <t>", "headers", lambda t: {"X-MediaBrowser-Token": t}),
            (f"X-Emby-Authorization: MediaBrowser {jellyball}, Token=\"<t>\"", "headers",
             lambda t: {"X-Emby-Authorization": f'MediaBrowser {jellyball}, Token="{t}"'}),
            ("?ApiKey=<t>", "params", lambda t: {"ApiKey": t}),
            ("?api_key=<t>", "params", lambda t: {"api_key": t}),
        ]
        rows: List[Dict[str, Any]] = []
        for credential, token in (("valid API key", jf.api_key), ("valid user token", jf.admin_token), ("wrong key", "0" * 32)):
            for label, where, build in styles:
                r = jf.request("GET", "/ScheduledTasks", auth="none", **{where: build(token)})
                rows.append({"credential": credential, "style": label, "status": r.status_code})
        rows.append({"credential": "none", "style": "(no credentials)",
                     "status": jf.request("GET", "/ScheduledTasks", auth="none").status_code})
        self.save("Auth.matrix", {"endpoint": "GET /ScheduledTasks", "results": rows})
        key_rows = {r["style"]: r["status"] for r in rows if r["credential"] == "valid API key"}
        accepted = [s for s, status in key_rows.items() if status == 200]
        rejected = [s for s, status in key_rows.items() if status != 200]
        self.note("a valid API key is ACCEPTED via: " + "; ".join(accepted))
        self.note("a valid API key is REJECTED (401) via: " + "; ".join(rejected))
        self.get("Auth.Keys", "/Auth/Keys", token=jf.admin_token)

    def _system(self) -> None:
        jf = self.jf
        public = self.save("System.Info.Public", jf.api("GET", "/System/Info/Public", auth="none"))
        self.sanitizer.add(urllib.parse.urlsplit(str(public.get("LocalAddress", ""))).hostname or "", "<CONTAINER_IP>", secret=False)
        self.get("System.Info", "/System/Info")
        tasks = self.get("ScheduledTasks", "/ScheduledTasks")
        guide = next((t for t in tasks if t.get("Key") == "RefreshGuide"), None)
        self.save("ScheduledTasks.RefreshGuide", guide)
        if guide:
            self.note(f"fresh server, no Live TV: GET /ScheduledTasks lists {len(tasks)} tasks and RefreshGuide IS there "
                      f"(Id {guide['Id']}, State {guide['State']}, Category {guide['Category']}, IsHidden {guide['IsHidden']}, "
                      f"24h IntervalTrigger)")
        else:
            self.note(f"fresh server: {len(tasks)} tasks and NO RefreshGuide")
        encoding = self.get("System.Configuration.encoding", "/System/Configuration/encoding")
        self.note(f"encoding config: EncoderAppPath {'present' if 'EncoderAppPath' in encoding else 'ABSENT (not user-set)'}, "
                  f"EncoderAppPathDisplay {encoding.get('EncoderAppPathDisplay')!r}; "
                  f"System.Info EncoderLocation {self.samples['System.Info'].get('EncoderLocation')!r}")

    def _livetv(self, feeds: FeedServer) -> None:
        jf = self.jf
        self.get("LiveTv.TunerHosts.Types", "/LiveTv/TunerHosts/Types")
        self.get("System.Configuration.livetv.initial", "/System/Configuration/livetv")
        self.get("LiveTv.Channels.initial", "/LiveTv/Channels")
        # -- M3U tuner
        tuner_body = {"Type": "m3u", "Url": feeds.m3u_url, "FriendlyName": "Jellyball Harness"}
        self.save("LiveTv.TunerHosts.add.request", tuner_body)
        tuner = self.save("LiveTv.TunerHosts.add.response", jf.api("POST", "/LiveTv/TunerHosts", json=tuner_body))
        agents = feeds.fetches("/playlist.m3u")
        self.note(f"POST /LiveTv/TunerHosts {{Type,Url,FriendlyName}} -> 200 with the stored tuner (server-assigned Id; "
                  f"defaults filled in); Jellyfin fetched the M3U immediately: {bool(agents)} (User-Agent {agents[0] if agents else '-'})")
        channels = jf.api("GET", "/LiveTv/Channels")["TotalRecordCount"]
        self.note(f"GET /LiveTv/Channels straight after adding the tuner: {channels} channels (they appear only after RefreshGuide)")
        # -- duplicates, update in place
        jf.api("POST", "/LiveTv/TunerHosts", json=tuner_body)
        count_dup = len(jf.livetv_config()["TunerHosts"])
        jf.api("POST", "/LiveTv/TunerHosts", json=dict(tuner, FriendlyName="Renamed"))
        config = jf.livetv_config()
        self.note(f"same tuner POSTed again without an Id: {count_dup} tuners (no de-duplication); POSTed with its Id: "
                  f"{len(config['TunerHosts'])} tuners, FriendlyName updated in place: "
                  f"{[t.get('FriendlyName') for t in config['TunerHosts'] if t['Id'] == tuner['Id']] == ['Renamed']}")
        for extra in [t for t in config["TunerHosts"] if t["Id"] != tuner["Id"]]:
            jf.delete_tuner(extra["Id"])
        jf.api("POST", "/LiveTv/TunerHosts", json=dict(tuner, FriendlyName="Jellyball Harness"))
        # -- XMLTV listing
        listing_body = {"Type": "xmltv", "Path": feeds.xmltv_url, "EnableAllTuners": True}
        self.save("LiveTv.ListingProviders.add.request", listing_body)
        listing = self.save("LiveTv.ListingProviders.add.response", jf.api("POST", "/LiveTv/ListingProviders", json=listing_body))
        self.note(f"POST /LiveTv/ListingProviders {{Type,Path,EnableAllTuners}} -> 200 with the stored provider; Jellyfin fetched "
                  f"the XMLTV immediately: {bool(feeds.fetches('/epg.xml'))} (it is fetched by RefreshGuide)")
        jf.api("POST", "/LiveTv/ListingProviders", json=listing_body)
        count_dup = len(jf.livetv_config()["ListingProviders"])
        jf.api("POST", "/LiveTv/ListingProviders", json=dict(listing, MoviePrefix="x"))
        config = jf.livetv_config()
        self.note(f"same listing POSTed again without an Id: {count_dup} providers; with its Id: {len(config['ListingProviders'])}")
        for extra in [p for p in config["ListingProviders"] if p["Id"] != listing["Id"]]:
            jf.delete_listing(extra["Id"])
        jf.api("POST", "/LiveTv/ListingProviders", json=listing)
        self.save("System.Configuration.livetv", jf.livetv_config())
        self.get("LiveTv.Info", "/LiveTv/Info")
        # -- refresh the guide
        task = jf.task("RefreshGuide")
        previous = jf.last_run_end("RefreshGuide")
        xmltv_before = len(feeds.fetches("/epg.xml"))
        started = jf.request("POST", f"/ScheduledTasks/Running/{task['Id']}")
        finished = self.save("ScheduledTasks.RefreshGuide.after-run", jf.wait_task_finished("RefreshGuide", previous))
        result = finished["LastExecutionResult"]
        self.note(f"POST /ScheduledTasks/Running/<RefreshGuide> -> {started.status_code}; afterwards State {finished['State']}, "
                  f"LastExecutionResult.Status {result['Status']}, IsHidden {finished['IsHidden']}; the refresh fetched the XMLTV: "
                  f"{len(feeds.fetches('/epg.xml')) > xmltv_before}")
        channels = self.get("LiveTv.Channels", "/LiveTv/Channels")
        programs = self.get("LiveTv.Programs.first3", "/LiveTv/Programs", params={"limit": 3})
        self.note(f"after RefreshGuide: {channels['TotalRecordCount']} channels ("
                  f"{', '.join(c['Name'] for c in channels['Items'])}); "
                  f"{programs['TotalRecordCount']} programmes; CurrentProgram present on each channel: "
                  f"{all('CurrentProgram' in c for c in channels['Items'])}")
        # -- removing it again
        jf.clear_livetv()
        stale = jf.api("GET", "/LiveTv/Channels")["TotalRecordCount"]
        previous = jf.last_run_end("RefreshGuide")
        jf.request("POST", f"/ScheduledTasks/Running/{task['Id']}")
        after = jf.wait_task_finished("RefreshGuide", previous)
        gone = jf.api("GET", "/LiveTv/Channels")["TotalRecordCount"]
        self.note(f"DELETE /LiveTv/TunerHosts?id= and /LiveTv/ListingProviders?id= -> 204 each; channels still listed straight "
                  f"after: {stale}; after the next RefreshGuide: {gone} (task IsHidden again: {after['IsHidden']})")

    def _livetv_errors(self, feeds: FeedServer) -> None:
        """What Jellyfin answers to inputs a 'Connect Jellyfin' feature could send by mistake."""
        jf = self.jf
        cases = [
            ("tuner: empty body", "POST", "/LiveTv/TunerHosts", {}),
            ("tuner: no Type", "POST", "/LiveTv/TunerHosts", {"Url": feeds.m3u_url}),
            ("tuner: unknown Type", "POST", "/LiveTv/TunerHosts", {"Type": "bogus", "Url": feeds.m3u_url}),
            ("tuner: no Url", "POST", "/LiveTv/TunerHosts", {"Type": "m3u"}),
            ("tuner: Url refuses connections", "POST", "/LiveTv/TunerHosts", {"Type": "m3u", "Url": f"http://{HOST_ALIAS}:1/x.m3u"}),
            ("tuner: Url answers 404", "POST", "/LiveTv/TunerHosts", {"Type": "m3u", "Url": feeds.container_url("/nope.m3u")}),
            ("tuner: Url serves XMLTV, not M3U", "POST", "/LiveTv/TunerHosts", {"Type": "m3u", "Url": feeds.xmltv_url}),
            ("listing: no Type", "POST", "/LiveTv/ListingProviders", {"Path": feeds.xmltv_url}),
            ("listing: unknown Type", "POST", "/LiveTv/ListingProviders", {"Type": "bogus", "Path": feeds.xmltv_url}),
            ("listing: no Path", "POST", "/LiveTv/ListingProviders", {"Type": "xmltv"}),
            ("listing: missing local file", "POST", "/LiveTv/ListingProviders", {"Type": "xmltv", "Path": "/nonexistent/guide.xml"}),
            ("listing: Path refuses connections", "POST", "/LiveTv/ListingProviders", {"Type": "xmltv", "Path": f"http://{HOST_ALIAS}:1/x.xml"}),
            ("listing: Type and Path only", "POST", "/LiveTv/ListingProviders", {"Type": "xmltv", "Path": feeds.xmltv_url}),
            ("delete tuner: unknown id", "DELETE", "/LiveTv/TunerHosts?id=doesnotexist", None),
            ("delete tuner: no id", "DELETE", "/LiveTv/TunerHosts", None),
            ("delete listing: unknown id", "DELETE", "/LiveTv/ListingProviders?id=doesnotexist", None),
            ("start task: unknown id", "POST", "/ScheduledTasks/Running/00000000000000000000000000000000", None),
        ]
        rows = []
        for label, method, path, body in cases:
            r = jf.request(method, path, **({"json": body} if body is not None else {}))
            text = r.text.replace("\n", " ")
            if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
                saved = r.json()
                text = "stored: " + json.dumps({k: saved.get(k) for k in ("Type", "Url", "Path", "EnableAllTuners") if k in saved})
            rows.append({"case": label, "request": f"{method} {path}", "body_sent": body, "status": r.status_code,
                         "content_type": r.headers.get("content-type", ""), "response": text[:160]})
        self.save("LiveTv.errors", rows)
        by_case = {r["case"]: r["status"] for r in rows}
        self.note("bad tuner input: " + ", ".join(f"{k.split(': ', 1)[1]} -> {v}" for k, v in by_case.items() if k.startswith("tuner")))
        self.note("bad listing input: " + ", ".join(f"{k.split(': ', 1)[1]} -> {v}" for k, v in by_case.items() if k.startswith("listing")))
        self.note("deletes and unknown ids: " + ", ".join(f"{k} -> {v}" for k, v in by_case.items() if k.startswith(("delete", "start"))))
        jf.clear_livetv()

    def write(self, out_dir: Path) -> List[Path]:
        out_dir.mkdir(parents=True, exist_ok=True)
        written = []
        for name, data in self.samples.items():
            path = out_dir / f"{name}.json"
            path.write_text(self.sanitizer.json_text(data), encoding="utf-8", newline="\n")
            written.append(path)
        return written


# -------------------------------------------------------------------------------- CLI


def _cmd_probe(args: argparse.Namespace) -> int:
    began = time.monotonic()
    with JellyfinHarness(args.tag) as jf:
        print("harness ready in " + ", ".join(f"{k} {v:.1f}s" for k, v in jf.timings.items()))
        probe = Probe(jf).run()
        if args.out:
            files = probe.write(Path(args.out))
            print(f"wrote {len(files)} sanitised samples to {args.out}")
        else:
            print("(no --out given: samples not written)")
    print(f"done in {time.monotonic() - began:.1f}s")
    return 0


def _cmd_up(args: argparse.Namespace) -> int:
    with JellyfinHarness(args.tag) as jf:
        print(f"Jellyfin {jf.version} is up\n  url:     {jf.base_url}\n"
              f"  admin:   {jf.admin_name}\nCtrl-C removes the container.", flush=True)
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            print("stopping")
    return 0


def _cmd_cleanup(_args: argparse.Namespace) -> int:
    names = cleanup_leftovers()
    print("removed: " + (", ".join(names) if names else "nothing"))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Disposable real-Jellyfin harness for Jellyball.")
    sub = parser.add_subparsers(dest="command", required=True)
    probe = sub.add_parser("probe", help="start Jellyfin, exercise the endpoints Jellyball cares about, report what it answers")
    probe.add_argument("--tag", help=f"image tag or full image (default {DEFAULT_TAG}; env {IMAGE_ENV})")
    probe.add_argument("--out", help="write the sanitised JSON samples to this directory (e.g. fixtures/jellyfin)")
    probe.set_defaults(func=_cmd_probe)
    up = sub.add_parser("up", help="start a ready Jellyfin and keep it until Ctrl-C")
    up.add_argument("--tag", help="image tag or full image")
    up.set_defaults(func=_cmd_up)
    clean = sub.add_parser("cleanup", help="remove leftover jellyball-it-* containers")
    clean.set_defaults(func=_cmd_cleanup)
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except HarnessUnavailable as exc:
        print(f"harness unavailable: {exc}", file=sys.stderr)
        return 2
    except HarnessError as exc:
        print(f"harness error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
