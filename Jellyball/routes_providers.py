"""HTTP routes (and, through dashboard_cards, dashboard cards) for provider reliability.

Pre-wired in Phase A so this lane can add endpoints without editing main.py or
other lanes' files (R1-R7 in the 2.1.0 plan). Rules:

- Protect every route with `auth: bool = Depends(verify_dashboard_auth)`.
  test_auth_guardrail fails on a route that answers an anonymous request unless
  its author lists it as public by design, which a reviewer will see.
- Keep the logic in its own modules; this file is the thin HTTP layer.
- Register dashboard cards or tabs here with dashboard_cards.register_card.
"""

import asyncio
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import provider_settings
import provider_telemetry
import provider_tools
import scrapers
from plugins import get_plugin_errors, get_plugin_records
from config import _log_failure
from dashboard_cards import Card, register_card
from security import verify_dashboard_auth

router = APIRouter()


class ProviderTestRequest(BaseModel):
    terms: Optional[List[str]] = Field(default=None, max_length=provider_tools.MAX_TERMS)


class ProviderSettingsRequest(BaseModel):
    enabled: bool = True
    # Lower ranks first, like STREAM_PROVIDER_PRIORITY's order; null clears the override.
    priority: Optional[int] = Field(default=None, ge=0, le=999)


def _find_provider(name: str):
    for provider in scrapers.ACTIVE_PROVIDERS:
        if provider.name == name:
            return provider
    raise HTTPException(status_code=404, detail="Unknown provider")


def _plugin_info_map() -> dict:
    return {
        record.name: {
            "version": record.version,
            "api_version": record.api_version,
            "origin": record.origin,
            "builtin": record.builtin,
            "linear": record.linear,
            "permissions": list(record.permissions),
        }
        for record in get_plugin_records()
    }


def provider_rows(db_stats: dict) -> list:
    """One row per provider: settings, breaker state, last run, persisted stats,
    and plugin metadata (3.0.0)."""
    breakers = scrapers.provider_breaker_snapshot()
    base_priority = scrapers._provider_priority
    plugin_info = _plugin_info_map()
    rows = []
    for index, provider in enumerate(scrapers.ACTIVE_PROVIDERS):
        name = provider.name
        stats = db_stats.get(name, {})
        last = provider_telemetry.last_run(name) or {}
        breaker = breakers.get(name, {"failures": 0, "open": False})
        rows.append({
            "name": name,
            "enabled": provider_settings.is_enabled(name),
            "priority": provider_settings.priority_override(name),
            "default_priority": base_priority.get(name),
            "position": index,
            "breaker_open": bool(breaker["open"]),
            "breaker_failures": int(breaker["failures"]),
            "suggested_domain": scrapers.get_suggested_domain(name),
            "last_success": stats.get("last_success"),
            "index_events": last.get("index_events") if last.get("index_events") is not None else stats.get("index_events"),
            "last_outcome": last.get("outcome"),
            "last_error_class": last.get("error_class"),
            "outcomes_24h": stats.get("outcomes_24h", {}),
            "plugin": plugin_info.get(name, {}),
        })
    return rows


@router.get("/api/providers", response_class=JSONResponse)
async def list_providers(auth: bool = Depends(verify_dashboard_auth)):
    stats = await asyncio.to_thread(provider_telemetry.provider_stats_sync)
    return {"providers": provider_rows(stats)}


@router.post("/api/providers/{name}/test", response_class=JSONResponse)
async def test_provider(name: str, body: Optional[ProviderTestRequest] = None, auth: bool = Depends(verify_dashboard_auth)):
    """Dry-run one provider: events listed, matches, streams found and the error class."""
    provider = _find_provider(name)
    terms = provider_tools.clean_terms(body.terms if body else None)
    try:
        browser = await scrapers.get_healthy_browser()
        return await provider_tools.dry_run(provider, terms, browser=browser)
    except provider_tools.ProviderBusy:
        raise HTTPException(status_code=429, detail="A test of this provider is already running")
    except Exception as exc:  # noqa: BLE001
        _log_failure(f"test provider {name}", exc)
        raise HTTPException(status_code=500, detail="Provider test failed")


@router.post("/api/providers/{name}/settings", response_class=JSONResponse)
async def update_provider_settings(name: str, body: ProviderSettingsRequest, auth: bool = Depends(verify_dashboard_auth)):
    provider = _find_provider(name)
    await asyncio.to_thread(provider_settings.set_provider_sync, provider.name, body.enabled, body.priority)
    return {"name": provider.name, "enabled": body.enabled, "priority": body.priority}


@router.post("/api/providers/{name}/retry", response_class=JSONResponse)
async def retry_provider(name: str, body: Optional[ProviderTestRequest] = None, auth: bool = Depends(verify_dashboard_auth)):
    """Clear the circuit breaker and run a test search right now, so a fixed
    provider (e.g. after a domain edit) doesn't wait out the hidden cooldown."""
    provider = _find_provider(name)
    scrapers.reset_provider_breaker(name)
    terms = provider_tools.clean_terms(body.terms if body else None)
    try:
        browser = await scrapers.get_healthy_browser()
        return await provider_tools.dry_run(provider, terms, browser=browser)
    except provider_tools.ProviderBusy:
        raise HTTPException(status_code=429, detail="A test of this provider is already running")
    except Exception as exc:  # noqa: BLE001
        _log_failure(f"retry provider {name}", exc)
        raise HTTPException(status_code=500, detail="Provider retry failed")


class ProviderDomainRequest(BaseModel):
    url: Optional[str] = None
    dismiss: bool = False


@router.post("/api/providers/{name}/apply-domain", response_class=JSONResponse)
async def apply_provider_domain(name: str, body: ProviderDomainRequest, auth: bool = Depends(verify_dashboard_auth)):
    """Apply (or dismiss) an auto-probed suggested domain for a provider. Uses
    the same validation and live-apply path as the dashboard's Provider Domains
    card, and clears the breaker so the new domain is used immediately."""
    from config import _validate_upstream_url
    from db import set_setting_async

    provider = _find_provider(name)
    suggested = (body.url or "").strip()
    if body.dismiss or not suggested:
        scrapers.dismiss_suggested_domain(name)
        return {"name": provider.name, "dismissed": True}
    validated = _validate_upstream_url(suggested)
    if not validated:
        raise HTTPException(status_code=400, detail="Not a valid public http(s) URL")
    url = validated.rstrip("/")
    await set_setting_async(scrapers._provider_url_setting_key(provider.name), url)
    scrapers._set_provider_url_override(provider, url)
    scrapers.dismiss_suggested_domain(name)
    scrapers.reset_provider_breaker(name)
    return {"name": provider.name, "url": url}


@router.post("/api/providers/selftest", response_class=JSONResponse)
async def run_selftest_now(auth: bool = Depends(verify_dashboard_auth)):
    """Run the nightly provider self-test on demand and return the digest."""
    summary = await provider_tools.run_provider_selftest()
    await provider_tools.send_selftest_digest(summary)
    return summary


@router.post("/api/providers/reload", response_class=JSONResponse)
async def reload_providers(auth: bool = Depends(verify_dashboard_auth)):
    """Reload provider plugins (picks up third_party/ changes) and rebuild the
    engine's provider lists. Per-plugin failures are isolated and reported."""
    from plugins import reload_plugins

    records = reload_plugins()
    scrapers._rebuild_provider_lists(records)
    errors = get_plugin_errors()
    return {
        "reloaded": len(records),
        "providers": [r.name for r in records],
        "errors": errors,
    }


async def _provider_card_context(request: Request) -> dict:
    stats = await asyncio.to_thread(provider_telemetry.provider_stats_sync)
    return {"providers": provider_rows(stats), "plugin_errors": get_plugin_errors()}


register_card(Card(
    tab="performance",
    name="provider_status",
    template="partials/providers.html",
    context=_provider_card_context,
    scripts=("/static/js/providers.js",),
    order=50,
))
