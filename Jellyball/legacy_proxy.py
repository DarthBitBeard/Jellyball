"""Minimal SAMPLE-AES shim (3.0).

The old pre-session passthrough proxy (fMP4, demuxed audio, chunk cache,
prefetch, startup buffer) is gone: those sources now go through the ffmpeg
remux ingest into the normalizing session engine. Only SAMPLE-AES encrypted
sources remain here, because ffmpeg cannot decrypt them. This is a thin
manifest rewrite plus an HMAC-signed chunk relay, nothing more.
"""

import logging
import re
import urllib.parse

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import Response

from config import (
    _log_failure,
    _public_base_url,
    _upstream_media_headers,
    _validate_upstream_url,
    LOGGER,
)
from state import _media_client, stream_state
from security import _relay_signature, _relay_signature_ok
from upstream import _fetch_upstream_body, _hls_response, HLS_MEDIA_TYPE

router = APIRouter()

_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_MAX_CHUNK_BYTES = 48 * 1024 * 1024


def _signed_relay_uri(uri: str, target_url: str, referer: str, origin: str, host: str, route: str) -> str:
    resolved = urllib.parse.urljoin(target_url, uri)
    if not _validate_upstream_url(resolved):
        return uri
    query = urllib.parse.urlencode({
        "url": resolved,
        "ref": referer or "",
        "org": origin or "",
        "sig": _relay_signature(resolved, referer or "", origin or ""),
    })
    return f"{host}{route}?{query}"


def _rewrite_sample_aes_manifest(manifest_text: str, target_url: str, referer: str, origin: str, host: str) -> str:
    """Rewrite segment/key URIs to the local signed relay routes."""
    out = []
    for line in manifest_text.splitlines():
        stripped = line.strip()
        if not stripped:
            out.append(line)
        elif stripped.startswith("#"):
            if 'URI="' in line:
                route = "/sample_aes/resource" if stripped.startswith(("#EXT-X-KEY", "#EXT-X-MAP", "#EXT-X-SESSION-KEY")) else "/sample_aes/chunk"
                line = re.sub(
                    r'URI="([^"]+)"',
                    lambda m: f'URI="{_signed_relay_uri(m.group(1), target_url, referer, origin, host, route)}"',
                    line,
                )
            out.append(line)
        else:
            out.append(_signed_relay_uri(stripped, target_url, referer, origin, host, "/sample_aes/chunk"))
    return "\n".join(out)


async def _sample_aes_proxy_stream(team_id: str, request: Request) -> Response:
    """Serve a SAMPLE-AES source through the minimal shim."""
    data = stream_state.get(team_id)
    candidates = (data or {}).get("candidates") or []
    if not candidates:
        return Response(status_code=404, content="Stream unavailable")
    active = candidates[min(data.get("active_index", 0), len(candidates) - 1)]
    target_url = active.get("url", "")
    if not _validate_upstream_url(target_url):
        return Response(status_code=502, content="Invalid upstream stream URL")
    referer = active.get("referer", "") or ""
    origin = active.get("origin", "") or ""
    headers = _upstream_media_headers(referer, origin, active.get("cookies", ""))
    host = _public_base_url(request)

    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(follow_redirects=False, timeout=12.0)
    try:
        result = await _fetch_upstream_body(client, target_url, headers, _MAX_MANIFEST_BYTES)
        if result is None:
            return Response(status_code=502, content="Upstream manifest unavailable")
        status_code, effective_url, _, body, _ = result
        if status_code != 200:
            return Response(status_code=status_code)
        rewritten = _rewrite_sample_aes_manifest(
            body.decode("utf-8", errors="replace"), effective_url, referer, origin, host
        )
        return _hls_response(rewritten)
    except Exception as exc:
        _log_failure(f"sample_aes manifest team={team_id}", exc, logging.ERROR)
        return Response(status_code=502, content="Upstream manifest unavailable")
    finally:
        if owns_client:
            await client.aclose()


@router.get("/sample_aes/chunk")
async def sample_aes_chunk(request: Request, url: str, ref: str = "", org: str = "", sig: str = ""):
    """Relay one SAMPLE-AES segment or key. Signed URLs only (no open relay)."""
    if not _relay_signature_ok(url, ref or "", org or "", sig):
        return Response(status_code=403, content="Unsigned relay URL")
    if not _validate_upstream_url(url):
        return Response(status_code=400, content="Invalid upstream URL")
    headers = _upstream_media_headers(ref or "", org or "")

    owns_client = _media_client() is None
    client = _media_client() or httpx.AsyncClient(follow_redirects=False, timeout=30.0)
    try:
        result = await _fetch_upstream_body(client, url, headers, _MAX_CHUNK_BYTES)
        if result is None:
            return Response(status_code=502, content="Upstream chunk unavailable")
        status_code, _, content_type, body, _ = result
        media_type = content_type.split(";")[0].strip() or HLS_MEDIA_TYPE
        return Response(content=body, status_code=status_code, media_type=media_type,
                        headers={"Cache-Control": "public, max-age=15"})
    except Exception as exc:
        _log_failure("sample_aes chunk relay", exc)
        return Response(status_code=502, content="Upstream chunk unavailable")
    finally:
        if owns_client:
            await client.aclose()


@router.get("/sample_aes/resource")
async def sample_aes_resource(request: Request, url: str, ref: str = "", org: str = "", sig: str = ""):
    """Relay a SAMPLE-AES key or init segment (same signed relay as chunks)."""
    return await sample_aes_chunk(request, url=url, ref=ref, org=org, sig=sig)
