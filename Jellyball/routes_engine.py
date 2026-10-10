"""HTTP routes (and, through dashboard_cards, dashboard cards) for the streaming engine and Multi-View.

Pre-wired in Phase A so this lane can add endpoints without editing main.py or
other lanes' files (E1-E5 in the 2.1.0 plan). Rules:

- Protect every route with `auth: bool = Depends(verify_dashboard_auth)`.
  test_auth_guardrail fails on a route that answers an anonymous request unless
  its author lists it as public by design, which a reviewer will see.
- Keep the logic in its own modules; this file is the thin HTTP layer.
- Register dashboard cards or tabs here with dashboard_cards.register_card.
"""

import asyncio

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse

import engine_settings
import engine_stats
from dashboard_cards import Card, register_card
from scrapers import provider_breaker_snapshot
from security import verify_dashboard_auth

router = APIRouter()


@router.get("/api/engine/status")
async def api_engine_status(auth: bool = Depends(verify_dashboard_auth)):
    """Provider breaker state and remux counts for the Live Sessions and
    Remux Ingest dashboard cards (per-session numbers: /api/sessions)."""
    return {
        "breakers": provider_breaker_snapshot(),
        "remux": engine_stats.remux_counts(),
        "remux_events": engine_stats.REMUX_EVENTS,
    }


@router.post("/settings/audio-language")
async def update_audio_language(
    preferred_audio_language: str = Form(""),
    auth: bool = Depends(verify_dashboard_auth),
):
    await asyncio.to_thread(engine_settings.save_audio_language, preferred_audio_language)
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


async def _audio_language_context(request: Request) -> dict:
    current = await asyncio.to_thread(engine_settings.preferred_audio_language)
    return {"current": current or "", "languages": engine_settings.AUDIO_LANGUAGES}


async def _remux_context(request: Request) -> dict:
    return {"rows": engine_stats.remux_counts(), "events": engine_stats.REMUX_EVENTS}


register_card(Card(
    tab="performance", name="engine_live_sessions", template="partials/engine_live_sessions.html",
    scripts=("/static/js/engine.js",), order=20,
))
register_card(Card(
    tab="performance", name="engine_remux_stats", template="partials/engine_remux_stats.html",
    context=_remux_context, scripts=("/static/js/engine.js",), order=21,
))
register_card(Card(
    tab="playback", name="engine_audio_language", template="partials/engine_audio_language.html",
    context=_audio_language_context, order=20,
))
