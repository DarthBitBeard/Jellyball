"""Operator tools for providers: a dry-run search (R4) and a real stream probe.

Neither touches the circuit breaker or the persisted provider_performance
history: a test button must not trip a breaker or skew the success rates.
"""

import asyncio
import os
import time
from typing import Dict, List, Optional

import httpx

import provider_telemetry as telemetry
import state
from config import _log_failure
from network_safety import bounded_int
from stream_extractor import verify_stream_live

DRY_RUN_TIMEOUT = 90.0
DEFAULT_TEST_TERMS = ["ESPN"]
MAX_TERMS = 5
MAX_TERM_LENGTH = 60
PROBE_LIMIT = 3

# Nightly self-test: dry-run every enabled provider once a day and send one
# digest of the degraded ones through the normal alert channels. Opt out with
# PROVIDER_SELFTEST_ENABLED=0; PROVIDER_SELFTEST_HOUR sets the local hour.
PROVIDER_SELFTEST_ENABLED = os.getenv("PROVIDER_SELFTEST_ENABLED", "1") == "1"
PROVIDER_SELFTEST_HOUR = bounded_int(os.getenv("PROVIDER_SELFTEST_HOUR", "3"), 3, 0, 23)
SELFTEST_DRY_RUN_TIMEOUT = 60.0
SELFTEST_CONCURRENCY = 3

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


async def run_provider_selftest() -> Dict:
    """Dry-run every enabled provider and return the degraded ones.

    {"checked": N, "degraded": [dry_run result dicts with outcome != "ok"]}.
    Never touches the circuit breaker or persisted telemetry (dry_run doesn't),
    so the nightly pass can't trip breakers or skew success rates.
    """
    # Late imports: scrapers must not be imported at module load (it is also
    # imported by the routes that import this module).
    import provider_settings
    import scrapers

    providers = provider_settings.filter_enabled(scrapers.ACTIVE_PROVIDERS)
    browser = await scrapers.get_healthy_browser()
    semaphore = asyncio.Semaphore(SELFTEST_CONCURRENCY)

    async def check(provider) -> Optional[Dict]:
        async with semaphore:
            try:
                result = await dry_run(
                    provider,
                    list(DEFAULT_TEST_TERMS),
                    browser=browser,
                    timeout=SELFTEST_DRY_RUN_TIMEOUT,
                )
            except ProviderBusy:
                return None
            except Exception as exc:  # noqa: BLE001 - reported as the error class
                return {"provider": provider.name, "outcome": "error", "error_class": type(exc).__name__}
            return result if result.get("outcome") != "ok" else None

    results = await asyncio.gather(*(check(provider) for provider in providers))
    return {"checked": len(providers), "degraded": [result for result in results if result]}


async def send_selftest_digest(summary: Dict) -> bool:
    """Send one digest of degraded providers; returns True when an alert went out."""
    from alerts import send_alert  # late: alerts pulls in the catalog

    degraded = summary.get("degraded") or []
    if not degraded:
        return False
    lines = [
        f"- **{item.get('provider')}**: {item.get('outcome')}"
        + (f" ({item.get('error_class')})" if item.get("error_class") else "")
        for item in degraded
    ]
    await send_alert(
        "Nightly provider self-test",
        f"{len(degraded)} of {summary.get('checked', 0)} providers degraded:\n" + "\n".join(lines),
        "warning",
    )
    return True


async def provider_selftest_loop() -> None:
    """Sleep until the next scheduled hour, then run the self-test every 24h."""
    from datetime import datetime, timedelta

    if not PROVIDER_SELFTEST_ENABLED:
        return
    while True:
        now = datetime.now().astimezone()
        target = now.replace(hour=PROVIDER_SELFTEST_HOUR, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            summary = await run_provider_selftest()
        except Exception as exc:
            _log_failure("nightly provider self-test", exc)
            continue
        try:
            await send_selftest_digest(summary)
        except Exception as exc:
            _log_failure("send provider self-test digest", exc)


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
