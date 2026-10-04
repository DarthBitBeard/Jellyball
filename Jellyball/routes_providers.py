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


def provider_rows(db_stats: dict) -> list:
    """One row per provider: settings, breaker state, last run and persisted stats."""
    breakers = scrapers.provider_breaker_snapshot()
    base_priority = scrapers._provider_priority
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
            "last_success": stats.get("last_success"),
            "index_events": last.get("index_events") if last.get("index_events") is not None else stats.get("index_events"),
            "last_outcome": last.get("outcome"),
            "last_error_class": last.get("error_class"),
            "outcomes_24h": stats.get("outcomes_24h", {}),
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


async def _provider_card_context(request: Request) -> dict:
    stats = await asyncio.to_thread(provider_telemetry.provider_stats_sync)
    return {"providers": provider_rows(stats)}


register_card(Card(
    tab="performance",
    name="provider_status",
    template="partials/providers.html",
    context=_provider_card_context,
    scripts=("/static/js/providers.js",),
    order=50,
))
