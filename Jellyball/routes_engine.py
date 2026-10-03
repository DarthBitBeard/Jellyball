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
    """Provider breaker state and legacy-fallback counts for the Live Sessions
    and Legacy Fallbacks dashboard cards (per-session numbers: /api/sessions)."""
    return {
        "breakers": provider_breaker_snapshot(),
        "legacy_fallbacks": engine_stats.legacy_fallback_counts(),
        "legacy_reasons": engine_stats.LEGACY_REASONS,
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


async def _legacy_context(request: Request) -> dict:
    return {"rows": engine_stats.legacy_fallback_counts(), "reasons": engine_stats.LEGACY_REASONS}


register_card(Card(
    tab="performance", name="engine_live_sessions", template="partials/engine_live_sessions.html",
    scripts=("/static/js/engine.js",), order=20,
))
register_card(Card(
    tab="performance", name="engine_legacy_fallbacks", template="partials/engine_legacy_fallbacks.html",
    context=_legacy_context, scripts=("/static/js/engine.js",), order=21,
))
register_card(Card(
    tab="playback", name="engine_audio_language", template="partials/engine_audio_language.html",
    context=_audio_language_context, order=20,
))
