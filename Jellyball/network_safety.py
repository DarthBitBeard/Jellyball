"""Shared validation helpers for untrusted HTTP(S) upstream URLs."""

from __future__ import annotations

import ipaddress
import os
import urllib.parse
from typing import Optional


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
