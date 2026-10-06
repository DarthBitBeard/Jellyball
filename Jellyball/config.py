"""Process configuration, imported first by main.py.

Resolves the writable data directory, starts logging (a queue listener so the
event loop never blocks on file I/O), creates first-run files, loads .env, and
holds small helpers every layer shares (_log_failure, escaping, URL checks).

PORT is rebound by the tray app when the configured port is taken, so other
modules read it as `config.PORT`.
"""
import os
import sys
import ipaddress
import logging
import re
import queue
from datetime import datetime, timedelta, timezone
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import socket
from pathlib import Path
from html import escape as html_escape
from typing import Optional, Tuple

# Resolve configuration and writable data independently of the current directory.
# This is important when the application is launched from a Jellyfin service or a shortcut.
APP_DIR = Path(sys.executable if getattr(sys, "frozen", False) else __file__).resolve().parent
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", APP_DIR))


def _get_writable_data_dir() -> Path:
    # Explicit override first: the Windows service uses %ProgramData%\Jellyball,
    # Docker uses the mounted volume, tests use a temp dir.
    override = os.getenv("JELLYBALL_DATA_DIR", "").strip()
    if override:
        candidate = Path(override).expanduser()
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate
    if os.name == "nt":
        roots = [os.getenv("LOCALAPPDATA"), os.getenv("APPDATA"), str(Path.home())]
    else:
        roots = [os.getenv("XDG_DATA_HOME"), str(Path.home() / ".local" / "share")]

    for root in roots:
        if not root:
            continue
        candidate = Path(root) / "Jellyball"
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            continue

    fallback = Path(os.getenv("TEMP", ".")) / "Jellyball"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


DATA_DIR = _get_writable_data_dir()
USER_ENV_FILE = DATA_DIR / ".env"
LOG_FILE = DATA_DIR / "jellyball.log"
LOGGER = logging.getLogger("jellyball")

# Size-capped rotation (5 x 10 MB) so a noisy day can't fill the disk of a box
# that runs for months. Records go through a queue so the event loop never
# blocks on file I/O; a background thread does the writing.
_log_handler = RotatingFileHandler(
    filename=str(LOG_FILE),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
    delay=True,
)
_log_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"))
_sink_handlers: list = [_log_handler]
# The windowed exe and the Windows service have no stdout (sys.stdout is None);
# a StreamHandler there fails on every record.
if sys.stdout is not None:
    _console_handler = logging.StreamHandler(sys.stdout)
    _console_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    _sink_handlers.append(_console_handler)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
_LOG_LISTENER: Optional["QueueListener"] = None
if not root_logger.handlers:
    _log_queue: "queue.SimpleQueue" = queue.SimpleQueue()
    root_logger.addHandler(QueueHandler(_log_queue))
    _LOG_LISTENER = QueueListener(_log_queue, *_sink_handlers, respect_handler_level=True)
    _LOG_LISTENER.start()
LOGGER.setLevel(logging.INFO)

# httpx/httpcore log one INFO line per HTTP request by default — with many active
# channels that's every provider probe and every HLS segment fetch, drowning real
# diagnostics. uvicorn.access would log every segment request (with the full
# tokenized upstream URL in the query string for legacy /chunk requests).
for _noisy_logger in ("httpx", "httpcore", "uvicorn.access", "hpack", "h2"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)


_URL_IN_TEXT_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"<>]+")


def _safe_exception_detail(exc: BaseException, limit: int = 160) -> str:
    """Exception text with URLs (tokens, webhook secrets) cut down to their host."""
    def _host_only(match) -> str:
        text = match.group(0)
        scheme, _, rest = text.partition("://")
        host = rest.split("/", 1)[0].split("?", 1)[0].rsplit("@", 1)[-1]
        return f"{scheme}://{host}/..."

    detail = _URL_IN_TEXT_RE.sub(_host_only, str(exc)).replace("\n", " ").strip()
    return detail[:limit]


def _log_failure(operation: str, exc: BaseException, level: int = logging.WARNING) -> None:
    """Log a failure with its type and a URL-scrubbed detail (URLs carry tokens)."""
    detail = _safe_exception_detail(exc)
    if detail:
        LOGGER.log(level, "%s failed (%s: %s)", operation, type(exc).__name__, detail)
    else:
        LOGGER.log(level, "%s failed (%s)", operation, type(exc).__name__)


def _bootstrap_runtime_files() -> None:
    """Create safe, user-writable first-run files without overwriting user data."""
    if not USER_ENV_FILE.exists():
        template = (
            "# Jellyball settings. Add secrets here; this file is stored per Windows user.\n"
            "PORT=8000\n"
            "# DASHBOARD_PASSWORD=change-this-password\n"
            "# STREAM_PROVIDER_PRIORITY=iSportSurge,MyBuffStreams\n"
            "# ACTIVE_HEALTH_INTERVAL=3\n"
            "# STANDBY_HEALTH_INTERVAL=45\n"
            "# PREFETCH_CHUNK_COUNT=5\n"
            "# STREAM_STARTUP_BUFFER_SECONDS=15\n"
        )
        try:
            USER_ENV_FILE.write_text(template, encoding="utf-8")
            try:
                USER_ENV_FILE.chmod(0o600)
            except OSError:
                pass
        except OSError as exc:
            _log_failure("create first-run environment file", exc, logging.ERROR)


# TheTVAppScraper and DaddyLiveScraper are defined later after HtmlAggregatorScraper
# to ensure the base class is available. See their definitions near the other
# aggregator classes.

_bootstrap_runtime_files()

_PLAYWRIGHT_PATH_SET_BY_APP = not os.getenv("PLAYWRIGHT_BROWSERS_PATH")
if _PLAYWRIGHT_PATH_SET_BY_APP:
    if getattr(sys, "frozen", False):
        bundled_browser_dir = BUNDLE_DIR / "playwright_browsers"
        browser_dir = bundled_browser_dir if bundled_browser_dir.exists() else DATA_DIR / "playwright_browsers"
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(browser_dir)
    else:
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

import asyncio

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import urllib.parse
import secrets
from xml.sax.saxutils import escape as xml_escape
from dotenv import load_dotenv
from fastapi import Request
from typing import Dict

# Precedence: real environment variables > the data directory's .env (per-user
# for the tray app, %ProgramData%\Jellyball\.env for the service, written by the
# installer) > a .env beside the executable (package-wide defaults). Previously
# the file beside the exe overrode everything, including the environment a
# service manager or Docker passed in.
load_dotenv(dotenv_path=USER_ENV_FILE)
load_dotenv(dotenv_path=APP_DIR / ".env")

# Imported after load_dotenv(): both read settings at import time
# (JELLYBALL_USER_AGENT, PLAYWRIGHT_MAX_PAGES, DNS_RESOLVE_TIMEOUT), which
# previously could only come from the real environment, not from .env.
from stream_extractor import DEFAULT_USER_AGENT  # noqa: E402
from network_safety import bounded_float, bounded_int, validate_http_url  # noqa: E402

PORT = bounded_int(os.getenv("PORT", "8000"), 8000, 1, 65535)


# --- Built-in TLS -----------------------------------------------------------
# JELLYBALL_TLS=1 serves the dashboard over HTTPS with a self-signed
# certificate generated into the data dir on first run. Off by default; plain
# HTTP remains the fallback. A self-signed cert still encrypts the Basic-auth
# password on the wire (browsers will show a trust warning, which is expected).
TLS_ENABLED = os.getenv("JELLYBALL_TLS", "").strip().lower() in {"1", "true", "yes", "on"}
TLS_CERT_FILE = DATA_DIR / "tls-cert.pem"
TLS_KEY_FILE = DATA_DIR / "tls-key.pem"


def _server_scheme() -> str:
    return "https" if TLS_ENABLED else "http"


def _ensure_tls_cert() -> Optional[Tuple[Path, Path]]:
    """(cert, key) paths for uvicorn's ssl_certfile/ssl_keyfile, or None when
    TLS is disabled or unavailable. Generates a self-signed certificate on
    first use; an existing pair is reused untouched."""
    if not TLS_ENABLED:
        return None
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError:
        LOGGER.error("JELLYBALL_TLS is set but the 'cryptography' package is not installed; serving plain HTTP")
        return None
    if TLS_CERT_FILE.exists() and TLS_KEY_FILE.exists():
        return (TLS_CERT_FILE, TLS_KEY_FILE)
    try:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Jellyball")])
        now = datetime.now(timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + timedelta(days=3650))
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                ]),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )
        TLS_KEY_FILE.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            )
        )
        try:
            TLS_KEY_FILE.chmod(0o600)
        except OSError as exc:
            _log_failure("restrict TLS key permissions", exc)
        TLS_CERT_FILE.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    except OSError as exc:
        _log_failure("generate self-signed TLS certificate", exc, logging.ERROR)
        return None
    # Point at the files only — never log key material.
    LOGGER.warning(
        "Generated a self-signed TLS certificate (%s); browsers will show a trust "
        "warning, which is expected for a self-signed LAN certificate",
        TLS_CERT_FILE,
    )
    return (TLS_CERT_FILE, TLS_KEY_FILE)


def _find_available_port(preferred_port: int, host: str = "127.0.0.1") -> int:
    # Never probe by binding to all interfaces; keep availability checks on
    # loopback (or the explicit non-wildcard host) so temporary wildcard binds
    # are not introduced during port selection.
    bind_host = "127.0.0.1" if host in ("", "0.0.0.0") else host
    family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
    candidates = list(range(preferred_port, 65536)) + list(range(1, preferred_port))
    for candidate in candidates:
        try:
            with socket.socket(family, socket.SOCK_STREAM) as probe:
                probe.bind((bind_host, candidate))
            return candidate
        except OSError:
            continue
    raise OSError("No available local TCP port found")


def _validate_upstream_url(value: str, *, allow_private: Optional[bool] = None) -> Optional[str]:
    """Allow safe absolute HTTP(S) upstream URLs for proxy endpoints."""
    return validate_http_url(value, allow_private=allow_private)


def _upstream_media_headers(referer: str = "", origin: str = "", cookies: str = "") -> Dict[str, str]:
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "*/*",
    }
    if referer:
        headers["Referer"] = referer
    if origin:
        safe_origin = _validate_upstream_url(origin)
        if safe_origin:
            parsed_origin = urllib.parse.urlsplit(safe_origin)
            headers["Origin"] = f"{parsed_origin.scheme}://{parsed_origin.netloc}"
    if cookies:
        # Session cookies captured from the provider's own browser session at
        # scrape time (see stream_extractor._cookie_header_for_hosts): the CDN
        # sees the same session state the working browser had.
        headers["Cookie"] = cookies
    return headers


def _html(value: object) -> str:
    """Escape dynamic values before inserting them into dashboard HTML."""
    return html_escape(str(value or ""), quote=True)


def _safe_team_id(value: str) -> str:
    """Create a stable, non-empty route/database identifier for a team name."""
    team_id = re.sub(r"[^a-zA-Z0-9_]+", "_", (value or "").lower()).strip("_")
    return team_id[:80] or f"team_{secrets.token_hex(4)}"


def _resource_path(relative_path: str) -> Path:
    """Resolve a packaged resource or a source-tree resource."""
    bundle_dir = Path(getattr(sys, "_MEIPASS", APP_DIR))
    bundled = bundle_dir / relative_path
    if bundled.exists():
        return bundled
    return APP_DIR / relative_path


_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f-\x9f  ]")
# Characters XML 1.0 forbids outright (xml_escape leaves them in and the guide
# then fails to parse).
_XML_INVALID_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f￾￿]")


def _clean_label(value: object, max_length: int = 120) -> str:
    """Single-line, bounded text for names/queries that end up in the M3U, EPG and UI."""
    text = _CONTROL_CHARS_RE.sub(" ", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()[:max_length].strip()


def _m3u_attribute(value: str) -> str:
    # A newline here would start a new playlist line (a planted channel/URL).
    return _CONTROL_CHARS_RE.sub(" ", str(value or "")).replace('"', "&quot;")


def _m3u_title(value: str) -> str:
    return _CONTROL_CHARS_RE.sub(" ", str(value or "")).strip()


def _xml_text(value: object) -> str:
    return xml_escape(_XML_INVALID_RE.sub("", str(value or "")))


def _xml_attr(value: object) -> str:
    return xml_escape(_XML_INVALID_RE.sub("", str(value or "")), {'"': "&quot;"})


_SAFE_HOST_HEADER_RE = re.compile(r"^[A-Za-z0-9.\-]+(:\d{1,5})?$|^\[[0-9A-Fa-f:.]+\](:\d{1,5})?$")


def _public_base_url(request: Optional[Request]) -> str:
    """Base URL for links we hand out (M3U entries, dashboard copy boxes).

    An explicit PUBLIC_BASE_URL env override wins: the M3U is often fetched
    from localhost while Jellyfin lives on another host, and then every stream
    URL is unreachable from Jellyfin's side. Falls back to the request's Host
    header, then loopback when that is missing or malformed."""
    override = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if override:
        return override
    host = (request.headers.get("host") or "").strip() if request is not None else ""
    if not _SAFE_HOST_HEADER_RE.match(host):
        host = f"127.0.0.1:{PORT}"
    scheme = getattr(getattr(request, "url", None), "scheme", "http")
    scheme = scheme if scheme in ("http", "https") else "http"
    return f"{scheme}://{host}"


def _base_url_is_loopback(base_url: str) -> bool:
    """True when a served base URL points at this machine only (Jellyfin on
    another host could never tune it)."""
    try:
        host = urllib.parse.urlsplit(base_url).hostname or ""
    except ValueError:
        return False
    return host.lower() in ("localhost", "127.0.0.1", "::1")


def _positive_env_number(name: str, default: float) -> float:
    return bounded_float(os.getenv(name, str(default)), default, 0.001, 86400.0)
