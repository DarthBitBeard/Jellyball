"""Shared validation helpers for untrusted HTTP(S) upstream URLs."""

from __future__ import annotations

import collections
import ipaddress
import os
import asyncio
import socket
import threading
import time
import urllib.parse
from typing import Dict, Optional, Tuple


_LOCAL_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
}


def private_upstreams_allowed() -> bool:
    value = os.getenv("ALLOW_PRIVATE_UPSTREAMS", "0").strip().casefold()
    return value in {"1", "true", "yes", "on"}


def is_private_host(hostname: str) -> bool:
    normalized = (hostname or "").strip().rstrip(".").casefold()
    if not normalized:
        return True
    if normalized in _LOCAL_HOSTNAMES or normalized.endswith((".local", ".internal", ".home.arpa")):
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    return any(
        (
            address.is_private,
            address.is_loopback,
            address.is_link_local,
            address.is_multicast,
            address.is_unspecified,
            address.is_reserved,
        )
    )


def _is_literal_ip(hostname: str) -> bool:
    """True when hostname is already a literal IPv4/IPv6 address (no DNS needed)."""
    try:
        ipaddress.ip_address((hostname or "").strip().rstrip("."))
    except ValueError:
        return False
    return True


def bounded_int(value: object, default: int, minimum: int, maximum: int) -> int:
    """Parse an integer environment value without allowing import-time failure."""
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def bounded_float(value: object, default: float, minimum: float, maximum: float) -> float:
    """Parse a finite floating-point environment value with explicit bounds."""
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return default
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return default
    return max(minimum, min(maximum, parsed))


def validate_http_url(value: str, *, allow_private: Optional[bool] = None) -> Optional[str]:
    """Return a normalized HTTP(S) URL or None when it is unsafe/invalid."""
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        parsed = urllib.parse.urlsplit(candidate)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme.casefold() not in {"http", "https"}:
        return None
    if not hostname or parsed.username or parsed.password:
        return None
    if port is not None and not 1 <= port <= 65535:
        return None
    if allow_private is None:
        allow_private = private_upstreams_allowed()
    if not allow_private and is_private_host(hostname):
        return None
    return candidate


# --- DNS resolution verdict cache -------------------------------------------------
#
# validate_http_url_async runs on every upstream request in the streaming proxy
# (every playlist poll every 1-4s, every segment fetch, every redirect hop, twice
# per health probe). Resolving the hostname via getaddrinfo costs a thread hop plus
# a DNS query each time, and a hung resolver can stall streaming indefinitely. The
# cache below remembers the safety verdict per (hostname, port, allow_private) for
# a short time, collapses concurrent lookups of the same host into one resolution,
# and bounds how long a single lookup is allowed to block.

DNS_RESOLVE_TIMEOUT = bounded_float(os.getenv("DNS_RESOLVE_TIMEOUT", "2.0"), 2.0, 0.1, 10.0)

_DNS_CACHE_MAX_ENTRIES = 1024
_DNS_CACHE_ALLOWED_TTL = 300.0
_DNS_CACHE_BLOCKED_TTL = 30.0

_DnsCacheKey = Tuple[str, int, bool]

_dns_lock = threading.Lock()
_dns_cache: "collections.OrderedDict[_DnsCacheKey, Tuple[bool, float]]" = collections.OrderedDict()
# Keyed by (id(loop), cache key) so a stale future from a previous/foreign event
# loop is never awaited: each test (or thread) gets its own loop, and entries are
# removed as soon as their lookup finishes, so ids are never reused while live.
_dns_inflight: Dict[Tuple[int, _DnsCacheKey], "asyncio.Future[bool]"] = {}


def clear_dns_cache() -> None:
    """Drop all cached DNS verdicts and in-flight lookups. Intended for tests."""
    with _dns_lock:
        _dns_cache.clear()
        _dns_inflight.clear()


def _dns_cache_get(key: _DnsCacheKey) -> Optional[bool]:
    now = time.monotonic()
    with _dns_lock:
        entry = _dns_cache.get(key)
        if entry is None:
            return None
        decision, expiry = entry
        if expiry <= now:
            del _dns_cache[key]
            return None
        _dns_cache.move_to_end(key)
        return decision


def _dns_cache_put(key: _DnsCacheKey, decision: bool, ttl: float) -> None:
    with _dns_lock:
        _dns_cache[key] = (decision, time.monotonic() + ttl)
        _dns_cache.move_to_end(key)
        while len(_dns_cache) > _DNS_CACHE_MAX_ENTRIES:
            _dns_cache.popitem(last=False)


async def _lookup_dns_verdict(hostname: str, port: int, timeout: float) -> Tuple[bool, float]:
    """Resolve hostname:port and decide whether it is safe to contact.

    Returns (decision, ttl). decision is False when any resolved address is
    private/loopback/link-local/reserved, the address list is empty, or the
    lookup timed out. A genuine resolution failure (unrelated to safety) is
    treated permissively, matching the previous behavior of leaving unresolved
    hosts to the HTTP client, but is cached only briefly since it may be
    transient.
    """
    try:
        addresses = await asyncio.wait_for(
            asyncio.to_thread(socket.getaddrinfo, hostname, port, type=socket.SOCK_STREAM),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        return False, _DNS_CACHE_BLOCKED_TTL
    except (OSError, socket.gaierror):
        return True, _DNS_CACHE_BLOCKED_TTL
    if not addresses:
        return False, _DNS_CACHE_BLOCKED_TTL
    for address in addresses:
        if is_private_host(address[4][0]):
            return False, _DNS_CACHE_BLOCKED_TTL
    return True, _DNS_CACHE_ALLOWED_TTL


async def _resolve_dns_verdict(hostname: str, port: int, allow_private: bool, timeout: float) -> bool:
    """Cache- and single-flight-aware wrapper around _lookup_dns_verdict."""
    key: _DnsCacheKey = (hostname, port, allow_private)
    cached = _dns_cache_get(key)
    if cached is not None:
        return cached

    loop = asyncio.get_running_loop()
    inflight_key = (id(loop), key)
    with _dns_lock:
        future = _dns_inflight.get(inflight_key)
        leader = future is None or future.done()
        if leader:
            future = loop.create_future()
            _dns_inflight[inflight_key] = future

    if not leader:
        # Ride along on the in-flight lookup. shield() keeps our own
        # cancellation from cancelling the shared future out from under the
        # leader and the other followers.
        return await asyncio.shield(future)

    try:
        decision, ttl = await _lookup_dns_verdict(hostname, port, timeout)
    except BaseException as exc:
        with _dns_lock:
            if _dns_inflight.get(inflight_key) is future:
                del _dns_inflight[inflight_key]
        if not future.done():
            if isinstance(exc, asyncio.CancelledError):
                future.cancel()
            else:
                future.set_exception(exc)
        raise

    _dns_cache_put(key, decision, ttl)
    with _dns_lock:
        if _dns_inflight.get(inflight_key) is future:
            del _dns_inflight[inflight_key]
    if not future.done():
        future.set_result(decision)
    return decision


async def validate_http_url_async(value: str, *, allow_private: Optional[bool] = None) -> Optional[str]:
    """Validate a URL and resolve its hostname before an async request.

    URL parsing alone cannot detect a public hostname that resolves to a private
    address. Resolution is repeated by callers after each redirect. This does
    not replace address pinning in the HTTP transport, but closes the common
    redirect-to-private-network path without blocking the event loop.

    The safety verdict for a hostname is cached briefly (see the DNS cache
    section above), a literal IP host skips DNS entirely since is_private_host()
    already covers it, and concurrent lookups of the same host share a single
    resolution instead of spawning a thread each.
    """
    safe_url = validate_http_url(value, allow_private=allow_private)
    if not safe_url:
        return None
    parsed = urllib.parse.urlsplit(safe_url)
    if allow_private is None:
        allow_private = private_upstreams_allowed()
    if allow_private:
        return safe_url
    hostname = parsed.hostname
    if _is_literal_ip(hostname):
        return safe_url
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    hostname_key = hostname.rstrip(".")
    decision = await _resolve_dns_verdict(hostname_key, port, allow_private, DNS_RESOLVE_TIMEOUT)
    return safe_url if decision else None
