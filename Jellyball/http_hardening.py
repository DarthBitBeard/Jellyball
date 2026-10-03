"""HTTP hardening that applies to the whole app: security response headers
(X2), the API documentation switch (X3), and the playback network allow-list
with the real client address behind trusted reverse proxies (X4).

Everything here is pure ASGI middleware or a pure function, so it adds no
per-chunk overhead to the segment relay path and is testable without a server.
"""

import ipaddress
import os
import re
from typing import Iterable, List, Optional, Sequence, Tuple, Union

from fastapi.responses import PlainTextResponse

from config import LOGGER

IPNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]
IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

_TRUTHY = {"1", "true", "yes", "on"}


# --- X2: security headers -----------------------------------------------------

# Pragmatic policy: the dashboard still has ~50 inline event handlers and ~160
# inline styles, so 'unsafe-inline' stays for scripts and styles (a strict CSP
# is a tracked follow-up). What it does stop: framing (clickjacking), plugins,
# <base> hijacking, forms posting to another site, and loading scripts or
# making requests to other origins. Team logos come from the network (ESPN), so
# images may load from anywhere over http(s).
CONTENT_SECURITY_POLICY = "; ".join([
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline'",
    "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data: https: http:",
    "font-src 'self' data:",
    "connect-src 'self'",
    "media-src 'self' blob:",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self'",
    "frame-ancestors 'none'",
])

# The opt-in API docs pages (X3) load Swagger UI / ReDoc from their CDN.
API_DOCS_CONTENT_SECURITY_POLICY = "; ".join([
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",
    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com",
    "img-src 'self' data: https:",
    "font-src 'self' data: https://fonts.gstatic.com",
    "worker-src 'self' blob:",
    "connect-src 'self'",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self'",
    "frame-ancestors 'none'",
])

API_DOCS_PATHS = ("/docs", "/docs/oauth2-redirect", "/redoc")

# Sent on every response: cheap, and harmless on playlists and segments.
# Referrer-Policy is "same-origin", not "no-referrer": under no-referrer a
# browser sends `Origin: null` on same-origin form POSTs, which the CSRF check
# (security._is_cross_site_write) rightly rejects - every dashboard form would
# break. same-origin still keeps dashboard URLs out of other sites' logs.
_ALWAYS_HEADERS: Tuple[Tuple[bytes, bytes], ...] = (
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"same-origin"),
)


def security_headers_for(path: str, content_type: str) -> List[Tuple[bytes, bytes]]:
    """The headers to add to one response (only those it does not set itself)."""
    headers = list(_ALWAYS_HEADERS)
    if content_type.lower().startswith("text/html"):
        policy = API_DOCS_CONTENT_SECURITY_POLICY if path in API_DOCS_PATHS else CONTENT_SECURITY_POLICY
        headers.append((b"content-security-policy", policy.encode("latin-1")))
        headers.append((b"x-frame-options", b"DENY"))
    return headers


class SecurityHeadersMiddleware:
    """Pure ASGI: add X-Content-Type-Options and Referrer-Policy to every
    response, and a Content-Security-Policy plus X-Frame-Options to HTML pages.
    A header the route already set is left alone."""

    def __init__(self, asgi_app) -> None:
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path") or ""

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                existing = list(message.get("headers") or [])
                present = {name.lower() for name, _ in existing}
                content_type = next(
                    (value.decode("latin-1") for name, value in existing if name.lower() == b"content-type"), "",
                )
                additions = [(n, v) for n, v in security_headers_for(path, content_type) if n not in present]
                if additions:
                    message = {**message, "headers": existing + additions}
            await send(message)

        await self.app(scope, receive, send_wrapper)


# --- X3: API documentation ----------------------------------------------------

def api_docs_enabled() -> bool:
    """/docs, /redoc and /openapi.json are off unless JELLYBALL_API_DOCS=1, and
    then they sit behind the dashboard login like every other admin page."""
    return os.getenv("JELLYBALL_API_DOCS", "").strip().lower() in _TRUTHY


API_DOCS_ENABLED = api_docs_enabled()


# --- X4: client address and the playback allow-list ---------------------------

def parse_networks(raw: str, setting: str) -> Tuple[List[IPNetwork], List[str]]:
    """Comma/space-separated IPs or CIDRs -> networks, plus the invalid entries
    (logged by the caller; an invalid entry never widens access)."""
    networks: List[IPNetwork] = []
    invalid: List[str] = []
    for item in re.split(r"[,\s]+", raw or ""):
        item = item.strip()
        if not item:
            continue
        try:
            networks.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            invalid.append(item)
    if invalid:
        LOGGER.error("%s: ignoring invalid entries %s", setting, ", ".join(invalid))
    return networks, invalid


def parse_ip(value: str) -> Optional[IPAddress]:
    """An address from a socket peer or a forwarding header; None if it isn't
    one. Accepts '[v6]' and 'v4:port' forms; IPv4-mapped IPv6 maps to IPv4."""
    text = (value or "").strip().strip('"')
    if text.startswith("["):
        text = text[1:].split("]", 1)[0]
    elif text.count(":") == 1:  # IPv4 with a port
        text = text.split(":", 1)[0]
    if "%" in text:  # IPv6 zone id
        text = text.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(text)
    except ValueError:
        return None
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        return address.ipv4_mapped
    return address


def _in_networks(address: IPAddress, networks: Iterable[IPNetwork]) -> bool:
    return any(address.version == net.version and address in net for net in networks)


TRUSTED_PROXIES, _ = parse_networks(os.getenv("TRUSTED_PROXIES", ""), "TRUSTED_PROXIES")


def _header(scope, name: bytes) -> str:
    values = [v.decode("latin-1") for k, v in scope.get("headers") or [] if k.lower() == name]
    return ",".join(values)


def client_ip(scope, trusted_proxies: Optional[Sequence[IPNetwork]] = None) -> Optional[IPAddress]:
    """The address of the client that made this request.

    The socket peer, unless the peer is one of TRUSTED_PROXIES: then
    X-Forwarded-For is read right to left, skipping trusted proxies, and the
    first other address is the client. With TRUSTED_PROXIES empty (the
    default) X-Forwarded-For is never read, so a client cannot claim to be
    someone else by sending one. None when the address cannot be determined;
    callers treat that as "not allowed"."""
    proxies = TRUSTED_PROXIES if trusted_proxies is None else trusted_proxies
    client = scope.get("client")
    peer = parse_ip(client[0]) if client else None
    if peer is None or not proxies or not _in_networks(peer, proxies):
        return peer
    hops = [hop.strip() for hop in _header(scope, b"x-forwarded-for").split(",") if hop.strip()]
    if not hops:
        return peer
    for hop in reversed(hops):
        address = parse_ip(hop)
        if address is None:
            return None  # a garbled chain from a trusted proxy: fail closed
        if not _in_networks(address, proxies):
            return address
    return parse_ip(hops[0])


def client_ip_text(scope) -> str:
    address = client_ip(scope)
    return str(address) if address is not None else "unknown"


# The endpoints Jellyfin (its tuner and ffmpeg) and the player use without a
# password: the playlist, the guide, channel and Multi-View audio streams, and
# the signed relay. /healthz stays reachable for monitoring.
_PLAYBACK_PATH_RE = re.compile(
    r"^/(?:playlist\.m3u|epg\.xml|substream\.m3u8|resource|chunk(?:\.(?:ts|mp4|aac|vtt))?)$"
    r"|^/stream/"
    r"|^/multiview/[^/]+/audio[-/]"
)


def is_playback_path(path: str) -> bool:
    return bool(_PLAYBACK_PATH_RE.match(path or ""))


def _load_playback_allow_list() -> Optional[List[IPNetwork]]:
    """None = no restriction (the 2.0 behaviour). A setting that has entries but
    none valid restricts playback to this machine rather than opening it."""
    raw = os.getenv("PLAYBACK_ALLOW_CIDRS", "")
    networks, invalid = parse_networks(raw, "PLAYBACK_ALLOW_CIDRS")
    if not networks and not invalid:
        return None
    if not networks:
        LOGGER.error("PLAYBACK_ALLOW_CIDRS has no valid entry; playback is limited to this machine")
    return networks


PLAYBACK_ALLOW_CIDRS = _load_playback_allow_list()


def playback_allowed(address: Optional[IPAddress], allow_list: Optional[Sequence[IPNetwork]]) -> bool:
    """Loopback is always allowed: Multi-View's ffmpeg and the relay fetch
    Jellyball's own streams over 127.0.0.1."""
    if allow_list is None:
        return True
    if address is None:
        return False
    return address.is_loopback or _in_networks(address, allow_list)


class PlaybackAccessMiddleware:
    """Pure ASGI: refuse playback endpoints to clients outside
    PLAYBACK_ALLOW_CIDRS (403). Does nothing while the setting is empty."""

    def __init__(self, asgi_app) -> None:
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        allow_list = PLAYBACK_ALLOW_CIDRS
        if allow_list is not None and scope["type"] == "http" and is_playback_path(scope.get("path") or ""):
            address = client_ip(scope)
            if not playback_allowed(address, allow_list):
                LOGGER.warning("Refused playback request from %s path=%s (PLAYBACK_ALLOW_CIDRS)",
                               address if address is not None else "unknown", scope.get("path"))
                response = PlainTextResponse("Playback is not allowed from this address", status_code=403)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


def uvicorn_forwarded_allow_ips() -> Optional[List[str]]:
    """What uvicorn should trust for X-Forwarded-For/-Proto. None keeps
    uvicorn's own default (FORWARDED_ALLOW_IPS, else 127.0.0.1), which is what
    2.0 ran with, so an existing loopback reverse proxy keeps working."""
    if not TRUSTED_PROXIES:
        return None
    return [str(net) for net in TRUSTED_PROXIES]
