"""Dashboard security: Basic-auth modes and lockout, the CSRF origin check
middleware, and the HMAC signing of legacy relay URLs.

`_configure_dashboard_auth` rebinds DASHBOARD_PASSWORD / DASHBOARD_AUTH_MODE,
so other modules read them as `security.DASHBOARD_AUTH_MODE`.
"""

import hashlib
import hmac
import ipaddress
import os
import secrets
import socket
import time
import urllib.parse
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from config import _log_failure, DATA_DIR, LOGGER


# bcrypt is used directly: passlib 1.7.4 is unmaintained and its bcrypt backend
# self-test fails against bcrypt>=4.1/5.x, which made hashed DASHBOARD_PASSWORD
# values silently fall back to a plain-text comparison that could never match.
try:
    import bcrypt as _bcrypt
except ImportError:
    _bcrypt = None
    if os.getenv("DASHBOARD_PASSWORD", "").startswith(("$2a$", "$2b$", "$2y$")):
        LOGGER.error("bcrypt is not installed; hashed dashboard passwords cannot be verified")

_BCRYPT_HASH_PREFIXES = ("$2a$", "$2b$", "$2y$")
# bcrypt verification deliberately costs ~100-300ms, and the dashboard polls
# several authenticated API routes; remember recent successful logins briefly.
# Store the successful password bytes (already present on the request) and
# compare with compare_digest — never hash passwords with a fast digest just
# to build a cache key (CodeQL py/weak-sensitive-data-hashing).
# value: (password_bytes, verified_at_monotonic, configured_password_snapshot)
_VERIFIED_CREDENTIALS: "OrderedDict[str, Tuple[bytes, float, str]]" = OrderedDict()
_VERIFIED_CREDENTIALS_TTL = 600.0
_VERIFIED_CREDENTIALS_MAX = 32


def _dashboard_password_matches(candidate: str, configured: str) -> bool:
    if configured.startswith(_BCRYPT_HASH_PREFIXES):
        if _bcrypt is None:
            return False
        try:
            # bcrypt only uses the first 72 bytes; bcrypt>=5 raises instead of truncating.
            return _bcrypt.checkpw(candidate.encode("utf-8")[:72], configured.encode("utf-8"))
        except ValueError:
            return False
    return secrets.compare_digest(candidate.encode("utf-8"), configured.encode("utf-8"))

DASHBOARD_USERNAME = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")
DASHBOARD_PASSWORD_FILE = DATA_DIR / "dashboard-password.txt"
# "configured" (DASHBOARD_PASSWORD), "generated" (network bind without one), or
# "open" (no password; only allowed while listening on loopback).
DASHBOARD_AUTH_MODE = "configured" if DASHBOARD_PASSWORD else "open"
security = HTTPBasic(auto_error=False)

_LOOPBACK_HOSTNAMES = {"localhost", "127.0.0.1", "::1", "[::1]"}


def _is_loopback_host(host: str) -> bool:
    host = (host or "").strip().lower().strip("[]")
    if host in _LOOPBACK_HOSTNAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _configure_dashboard_auth(bind_host: str) -> None:
    """A dashboard with no password may only listen on loopback. Listening on the
    network (Docker's 0.0.0.0, a LAN bind) without DASHBOARD_PASSWORD gets a
    random password, generated once and kept in the data directory."""
    global DASHBOARD_PASSWORD, DASHBOARD_AUTH_MODE
    if DASHBOARD_PASSWORD or _is_loopback_host(bind_host):
        return
    generated = ""
    try:
        generated = DASHBOARD_PASSWORD_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    if not generated:
        generated = secrets.token_urlsafe(12)
        try:
            DASHBOARD_PASSWORD_FILE.write_text(generated + "\n", encoding="utf-8")
            try:
                DASHBOARD_PASSWORD_FILE.chmod(0o600)
            except OSError as exc:
                _log_failure("restrict dashboard password file permissions", exc)
        except OSError as exc:
            _log_failure("save generated dashboard password", exc)
        # Point at the file only — never log the plaintext password.
        LOGGER.warning(
            "Dashboard listens on %s with no DASHBOARD_PASSWORD: generated one for user=%s (saved to %s)",
            bind_host, DASHBOARD_USERNAME, DASHBOARD_PASSWORD_FILE,
        )
    else:
        LOGGER.warning(
            "Dashboard listens on %s with no DASHBOARD_PASSWORD: using the generated password in %s (user=%s)",
            bind_host, DASHBOARD_PASSWORD_FILE, DASHBOARD_USERNAME,
        )
    DASHBOARD_PASSWORD = generated
    DASHBOARD_AUTH_MODE = "generated"


def _request_host_name(request: Request) -> str:
    host = request.headers.get("host", "")
    if host.startswith("["):
        return host[1:].split("]", 1)[0]
    return host.rsplit(":", 1)[0] if host.count(":") == 1 else host


# Failed Basic-auth attempts per client address: (failures, window start, locked until).
_AUTH_FAILURES: "OrderedDict[str, List[float]]" = OrderedDict()
AUTH_FAILURE_LIMIT = 8
AUTH_FAILURE_WINDOW = 300.0
AUTH_LOCKOUT_SECONDS = 300.0


def _auth_client_key(request: Optional[Request], username: str = "") -> str:
    """Bucket key for the login lockout: (TCP peer IP, attempted username).

    Keyed per username so that, behind a reverse proxy where every client
    shares one peer IP, one attacker's failed logins cannot lock all
    legitimate users out. The peer IP (never X-Forwarded-For) is still the
    address component, so the header cannot be spoofed to dodge the lockout.
    """
    client = getattr(request, "client", None) if request is not None else None
    peer = getattr(client, "host", "") or "unknown"
    return f"{peer}\x00{username or ''}"


def _auth_locked_out(client_key: str, now: float) -> bool:
    entry = _AUTH_FAILURES.get(client_key)
    return bool(entry and entry[2] > now)


def _record_auth_failure(client_key: str, now: float) -> None:
    entry = _AUTH_FAILURES.get(client_key)
    if entry is None or now - entry[1] > AUTH_FAILURE_WINDOW:
        entry = [0.0, now, 0.0]
    entry[0] += 1
    if entry[0] >= AUTH_FAILURE_LIMIT:
        entry[2] = now + AUTH_LOCKOUT_SECONDS
        LOGGER.warning("Dashboard login locked for %.0fs after %d failures client=%s",
                       AUTH_LOCKOUT_SECONDS, int(entry[0]), client_key)
    _AUTH_FAILURES[client_key] = entry
    _AUTH_FAILURES.move_to_end(client_key)
    while len(_AUTH_FAILURES) > 256:
        _AUTH_FAILURES.popitem(last=False)


def verify_dashboard_auth(request: Request = None, credentials: Optional[HTTPBasicCredentials] = Depends(security)):
    if not DASHBOARD_PASSWORD:
        # Open access is only ever served on a loopback bind. Also require a
        # loopback Host header, so a DNS-rebinding page (evil.example resolving
        # to 127.0.0.1) can't drive the dashboard from the user's browser.
        if request is not None and not _is_loopback_host(_request_host_name(request)) \
                and _request_host_name(request).lower() != socket.gethostname().lower():
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Dashboard is local-only")
        return True
    now = time.monotonic()
    attempted_username = credentials.username if credentials else ""
    client_key = _auth_client_key(request, attempted_username)
    if _auth_locked_out(client_key, now):
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many failed logins")
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )

    # compare_digest on str raises TypeError for non-ASCII input; compare bytes.
    is_user_ok = secrets.compare_digest(credentials.username.encode("utf-8"), DASHBOARD_USERNAME.encode("utf-8"))
    attempted_password = credentials.password.encode("utf-8")
    cached = _VERIFIED_CREDENTIALS.get(credentials.username)
    if (
        cached is not None
        and cached[2] == DASHBOARD_PASSWORD
        and now - cached[1] < _VERIFIED_CREDENTIALS_TTL
        and secrets.compare_digest(cached[0], attempted_password)
    ):
        is_pass_ok = True
    else:
        is_pass_ok = _dashboard_password_matches(credentials.password, DASHBOARD_PASSWORD)
        if is_pass_ok and is_user_ok:
            _VERIFIED_CREDENTIALS[credentials.username] = (attempted_password, now, DASHBOARD_PASSWORD)
            _VERIFIED_CREDENTIALS.move_to_end(credentials.username)
            while len(_VERIFIED_CREDENTIALS) > _VERIFIED_CREDENTIALS_MAX:
                _VERIFIED_CREDENTIALS.popitem(last=False)

    if not (is_user_ok and is_pass_ok):
        _record_auth_failure(client_key, now)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    _AUTH_FAILURES.pop(client_key, None)
    return True


_UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_DEFAULT_PORTS = {"http": "80", "https": "443"}


def _normalized_netloc(scheme: str, netloc: str) -> str:
    netloc = (netloc or "").strip().lower()
    default_port = _DEFAULT_PORTS.get((scheme or "").lower())
    if default_port and netloc.endswith(f":{default_port}") and not netloc.endswith("]"):
        netloc = netloc[: -(len(default_port) + 1)]
    return netloc


# When True, CSRF also trusts X-Forwarded-Host (only safe behind a reverse
# proxy that overwrites that header). Off by default so a browser cannot
# spoof the allowed Origin target.
TRUST_X_FORWARDED_HOST = os.getenv("TRUST_X_FORWARDED_HOST", "").strip().lower() in {
    "1", "true", "yes", "on",
}


def _is_cross_site_write(method: str, headers: Dict[str, str], scheme: str) -> bool:
    """True for a state-changing request a browser sent from another site.

    Basic-auth credentials are replayed by the browser on cross-site form POSTs,
    so every dashboard write would otherwise be forgeable from any web page.
    Browsers always send Origin (or at least Referer) on such requests;
    non-browser clients (curl, scripts) send neither and aren't a CSRF vector."""
    if method not in _UNSAFE_METHODS:
        return False
    allowed = {_normalized_netloc(scheme, headers.get("host", ""))} - {""}
    if TRUST_X_FORWARDED_HOST:
        allowed.add(_normalized_netloc(scheme, headers.get("x-forwarded-host", "").split(",")[0]))
        allowed.discard("")
    origin = headers.get("origin")
    source = origin if origin is not None else headers.get("referer")
    if source is None:
        return False
    if source.strip().lower() == "null":
        return True
    parsed = urllib.parse.urlsplit(source)
    if not parsed.netloc:
        return True
    return _normalized_netloc(parsed.scheme, parsed.netloc) not in allowed


class CsrfOriginMiddleware:
    """Pure ASGI: reject cross-site state-changing requests (see _is_cross_site_write)."""

    def __init__(self, asgi_app) -> None:
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("method") in _UNSAFE_METHODS:
            headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
            if _is_cross_site_write(scope["method"], headers, scope.get("scheme", "http")):
                LOGGER.warning("Rejected cross-site request method=%s path=%s", scope.get("method"), scope.get("path"))
                response = PlainTextResponse("Cross-site request rejected", status_code=403)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


def _load_relay_signing_key() -> bytes:
    """Key for signing legacy relay URLs, kept in the data dir so URLs handed to
    a player before a restart stay valid after it."""
    key_file = DATA_DIR / "relay-signing.key"
    try:
        key = key_file.read_bytes()
        if len(key) >= 32:
            return key[:32]
    except OSError:
        pass
    key = secrets.token_bytes(32)
    try:
        key_file.write_bytes(key)
        try:
            key_file.chmod(0o600)
        except OSError as exc:
            _log_failure("restrict relay signing key permissions", exc)
    except OSError as exc:
        _log_failure("save relay signing key", exc)
    return key


_RELAY_SIGNING_KEY = _load_relay_signing_key()


def _relay_signature(url: str, ref: str = "", org: str = "") -> str:
    """/substream.m3u8, /chunk* and /resource fetch whatever URL they are given
    and must stay unauthenticated (Jellyfin's ffmpeg calls them), so they only
    serve URLs this server wrote into a playlist itself: without a signature
    they were an open fetch relay for anyone who could reach the port."""
    message = f"{url}\n{ref or ''}\n{org or ''}".encode("utf-8")
    return hmac.new(_RELAY_SIGNING_KEY, message, hashlib.sha256).hexdigest()[:32]


def _relay_signature_ok(url: str, ref: str, org: str, sig: str) -> bool:
    return bool(sig) and hmac.compare_digest(str(sig), _relay_signature(url, ref, org))
