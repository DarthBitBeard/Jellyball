"""Upstream fetch helpers shared by the channel sessions and the legacy relay:
the bounded, SSRF-checked GET (`_fetch_upstream_body`) and the HLS playlist
response (`_hls_response`).

They used to live in legacy_proxy.py, which made the session engine import the
legacy relay for them. legacy_proxy re-exports every name defined here, so the
old `legacy_proxy.<name>` imports keep working.
"""

import os
import time
import urllib.parse
from collections import OrderedDict
from typing import Dict, Optional, Tuple

import httpx
from fastapi.responses import Response

from config import LOGGER
from network_safety import bounded_int, validate_http_url_async


MAX_UPSTREAM_REDIRECTS = bounded_int(os.getenv("MAX_UPSTREAM_REDIRECTS", "3"), 3, 0, 5)
STREAM_REQUEST_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)


_UPSTREAM_REJECTION_LOGGED: "OrderedDict[Tuple[str, int], float]" = OrderedDict()


def _log_upstream_rejection(url: str, status_code: int) -> None:
    """Rate-limited: a dead source is polled every few seconds by its channel
    session and would otherwise write a warning line per poll."""
    host = urllib.parse.urlsplit(url).netloc
    key = (host, status_code)
    now = time.monotonic()
    if now - _UPSTREAM_REJECTION_LOGGED.get(key, 0.0) < 60.0:
        return
    _UPSTREAM_REJECTION_LOGGED[key] = now
    _UPSTREAM_REJECTION_LOGGED.move_to_end(key)
    while len(_UPSTREAM_REJECTION_LOGGED) > 256:
        _UPSTREAM_REJECTION_LOGGED.popitem(last=False)
    LOGGER.warning(
        "Upstream proxy resource rejected status=%s host=%s path=%s",
        status_code, host, urllib.parse.urlsplit(url).path,
    )


async def _fetch_upstream_body(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
    max_bytes: int,
    timeout: Optional[httpx.Timeout] = None,
) -> Optional[tuple[int, str, str, bytes, Dict[str, str]]]:
    """GET a bounded upstream body, validating every redirect hop (SSRF guard)."""
    current_url = url
    for _ in range(MAX_UPSTREAM_REDIRECTS + 1):
        safe_url = await validate_http_url_async(current_url)
        if not safe_url:
            return None
        try:
            async with client.stream(
                "GET",
                safe_url,
                headers=headers,
                timeout=timeout or STREAM_REQUEST_TIMEOUT,
                follow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    location = response.headers.get("location")
                    if not location:
                        return None
                    current_url = urllib.parse.urljoin(str(response.url), location)
                    continue
                if response.status_code >= 400:
                    _log_upstream_rejection(str(response.url), response.status_code)
                content_length = response.headers.get("content-length")
                try:
                    if content_length and int(content_length) > max_bytes:
                        return None
                except ValueError:
                    pass
                body = bytearray()
                async for block in response.aiter_bytes():
                    if len(body) + len(block) > max_bytes:
                        return None
                    body.extend(block)
                return (
                    response.status_code,
                    str(response.url),
                    response.headers.get("content-type", ""),
                    bytes(body),
                    dict(response.headers),
                )
        except Exception as exc:
            _log_upstream_exception(safe_url, exc)
            return None
    return None


_UPSTREAM_EXCEPTION_LOGGED: "OrderedDict[Tuple[str, str], float]" = OrderedDict()


def _log_upstream_exception(url: str, exc: BaseException) -> None:
    """Once per (host, error type) per minute: a dead CDN is polled every few
    seconds by every session using it."""
    host = urllib.parse.urlsplit(url).netloc.lower()
    key = (host, type(exc).__name__)
    now = time.monotonic()
    if now - _UPSTREAM_EXCEPTION_LOGGED.get(key, -1e9) < 60.0:
        return
    _UPSTREAM_EXCEPTION_LOGGED[key] = now
    _UPSTREAM_EXCEPTION_LOGGED.move_to_end(key)
    while len(_UPSTREAM_EXCEPTION_LOGGED) > 256:
        _UPSTREAM_EXCEPTION_LOGGED.popitem(last=False)
    LOGGER.warning("Upstream fetch failed host=%s error=%s", host, type(exc).__name__)


HLS_MEDIA_TYPE = "application/vnd.apple.mpegurl"


def _hls_response(text: str) -> Response:
    return Response(
        content=text,
        media_type=HLS_MEDIA_TYPE,
        headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"},
    )
