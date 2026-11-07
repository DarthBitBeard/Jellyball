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
import stream_language
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


@router.post("/settings/stream-language")
async def update_stream_language(
    stream_language_1: str = Form("eng"),
    stream_language_2: str = Form(""),
    stream_language_mode: str = Form("prefer"),
    auth: bool = Depends(verify_dashboard_auth),
):
    await asyncio.to_thread(
        stream_language.save_preference, [stream_language_1, stream_language_2], stream_language_mode
    )
    _rescrape_always_live_channels()
    return RedirectResponse(url="/?tab=playback&status=saved", status_code=303)


def _rescrape_always_live_channels() -> None:
    """Apply a new language choice now: re-sort (and in "only" mode prune) every
    channel's existing candidates, and re-search the 24/7 channels rather than
    waiting for their next scheduled refresh."""
    from config import LOGGER
    from failover import trigger_scrape
    from state import _spawn_background_task, stream_state

    dropped = 0
    for team_id, data in list(stream_state.items()):
        if data.get("type") == "multiview":
            continue
        dropped += stream_language.apply_to_channel(data)
        if data.get("always_live"):
            _spawn_background_task(trigger_scrape(team_id, force=True), f"language rescrape {team_id}")
    LOGGER.info(
        "Stream language set to %s (%s); removed %d candidate(s) in other languages",
        ",".join(stream_language.preferred_languages()), stream_language.language_mode(), dropped,
    )


async def _stream_language_context(request: Request) -> dict:
    languages = await asyncio.to_thread(stream_language.preferred_languages)
    mode = await asyncio.to_thread(stream_language.language_mode)
    return {
        "first": languages[0] if languages else "eng",
        "second": languages[1] if len(languages) > 1 else "",
        "mode": mode,
        "languages": stream_language.LANGUAGES,
    }


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
register_card(Card(
    tab="playback", name="engine_stream_language", template="partials/engine_stream_language.html",
    context=_stream_language_context, order=19,
))
