"""Operator tools for providers: a dry-run search (R4) and a real stream probe.

Neither touches the circuit breaker or the persisted provider_performance
history: a test button must not trip a breaker or skew the success rates.
"""

import asyncio
import time
from typing import Dict, List, Optional

import httpx

import provider_telemetry as telemetry
import state
from stream_extractor import verify_stream_live

DRY_RUN_TIMEOUT = 90.0
DEFAULT_TEST_TERMS = ["ESPN"]
MAX_TERMS = 5
MAX_TERM_LENGTH = 60
PROBE_LIMIT = 3

_RUNNING: set = set()


class ProviderBusy(Exception):
    """A dry-run of this provider is already in flight."""


def clean_terms(raw) -> List[str]:
    if not isinstance(raw, list):
        return list(DEFAULT_TEST_TERMS)
    terms = [str(term).strip()[:MAX_TERM_LENGTH] for term in raw if str(term).strip()]
    return terms[:MAX_TERMS] or list(DEFAULT_TEST_TERMS)


async def dry_run(provider, terms: List[str], browser=None, timeout: float = DRY_RUN_TIMEOUT) -> Dict:
    """Run one search and report events listed, matches, streams and error class."""
    if provider.name in _RUNNING:
        raise ProviderBusy(provider.name)
    _RUNNING.add(provider.name)
    try:
        run = telemetry.begin_run(provider.name)
        owns_client = state.SHARED_HTTP_CLIENT is None
        client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=12.0, follow_redirects=True)
        started = time.monotonic()
        streams: list = []
        exc_seen: Optional[BaseException] = None
        timed_out = False
        try:
            streams = await asyncio.wait_for(
                provider.search(terms, browser=browser, http_client=client), timeout=timeout
            )
        except asyncio.TimeoutError:
            timed_out = True
        except Exception as exc:  # noqa: BLE001 - reported to the operator as a class
            exc_seen = exc
        finally:
            if owns_client:
                await client.aclose()
        outcome, error_class = telemetry.derive_outcome(streams, run, exc=exc_seen, timed_out=timed_out)
        return {
            "provider": provider.name,
            "outcome": outcome,
            "error_class": error_class,
            "page_errors": sorted(set(run.page_errors)),
            "index_events": run.index_events if run.index_known else None,
            "matches": run.matches,
            "streams": len(streams or []),
            "response_time_ms": int((time.monotonic() - started) * 1000),
            "terms": terms,
        }
    finally:
        _RUNNING.discard(provider.name)


async def probe_candidates(candidates: List[dict], limit: int = PROBE_LIMIT) -> Dict:
    """Actually fetch the first `limit` candidates (playlist and one segment)."""
    checked = candidates[:limit]
    if not checked:
        return {"probed": 0, "live": 0}
    owns_client = state.SHARED_HTTP_CLIENT is None
    client = state.SHARED_HTTP_CLIENT or httpx.AsyncClient(timeout=12.0, follow_redirects=True)
    try:
        results = await asyncio.gather(
            *(
                verify_stream_live(
                    client,
                    str(candidate.get("url") or ""),
                    referer=str(candidate.get("referer") or ""),
                    origin=str(candidate.get("origin") or ""),
                )
                for candidate in checked
            ),
            return_exceptions=True,
        )
    finally:
        if owns_client:
            await client.aclose()
    live = sum(1 for result in results if result is True)
    return {"probed": len(checked), "live": live}
