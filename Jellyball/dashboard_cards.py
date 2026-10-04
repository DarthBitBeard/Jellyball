"""Dashboard cards: self-contained panels that feature modules add to a tab
without editing the shared dashboard template or the dashboard route.

A feature module registers its cards at import time. main.py imports every
routes_<lane> module statically (PyInstaller cannot see dynamic discovery), so
registration is deterministic and ordered:

    from dashboard_cards import Card, register_card

    async def _context(request):
        return {"state": await load_state()}

    register_card(Card(
        tab="alerts",                          # a built-in tab, or one from register_tab
        name="jellyfin_connect",               # unique; keys the card's context
        template="partials/jellyfin_connect.html",
        context=_context,                      # optional: async (request) -> dict
        scripts=("/static/js/jellyfin.js",),   # optional: loaded after dashboard.js
    ))

dashboard.html includes each card's template at the end of its tab's pane with
`card` (the Card) and `ctx` (what its context function returned, else {}) in
scope next to the page's normal context. A context function that raises is
logged and the card gets {"error": "<message>"}: one feature's bug must not
take the whole dashboard down.

register_tab adds a tab after the built-in ones, for a feature that deserves
its own page rather than a card.
"""

from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from fastapi import Request

from config import _log_failure, _safe_exception_detail

BUILTIN_TABS = ("channels", "metrics", "performance", "playback", "alerts", "logs")

ContextFn = Callable[[Request], Awaitable[dict]]


@dataclass(frozen=True)
class Tab:
    key: str  # URL value of ?tab=, and the id suffix (tab-<key>, btn-<key>)
    label: str  # button text, may include an emoji


@dataclass(frozen=True)
class Card:
    tab: str
    name: str
    template: str
    context: Optional[ContextFn] = None
    scripts: Tuple[str, ...] = ()
    styles: Tuple[str, ...] = ()
    order: int = 100  # lower first; ties keep registration order


_TABS: List[Tab] = []
_CARDS: List[Card] = []


def register_tab(tab: Tab) -> None:
    if not tab.key.isidentifier() or tab.key in BUILTIN_TABS or any(t.key == tab.key for t in _TABS):
        raise ValueError(f"invalid or duplicate dashboard tab {tab.key!r}")
    _TABS.append(tab)


def register_card(card: Card) -> None:
    valid_tabs = {*BUILTIN_TABS, *(t.key for t in _TABS)}
    if card.tab not in valid_tabs:
        raise ValueError(f"card {card.name!r} targets unknown tab {card.tab!r}")
    if not card.name.isidentifier() or any(c.name == card.name for c in _CARDS):
        raise ValueError(f"invalid or duplicate dashboard card name {card.name!r}")
    if not card.template.endswith(".html"):
        raise ValueError(f"card {card.name!r} template must be an .html file, got {card.template!r}")
    for path in (*card.scripts, *card.styles):
        if not path.startswith("/static/"):
            raise ValueError(f"card {card.name!r} asset {path!r} must be served from /static/")
    _CARDS.append(card)


def cards_by_tab() -> Dict[str, List[Card]]:
    """Registered cards grouped by tab, each group ordered by Card.order."""
    grouped: Dict[str, List[Card]] = {}
    for card in sorted(_CARDS, key=lambda c: c.order):  # sorted() is stable
        grouped.setdefault(card.tab, []).append(card)
    return grouped


def _unique_assets(attribute: str) -> List[str]:
    seen: List[str] = []
    for card in sorted(_CARDS, key=lambda c: c.order):
        for path in getattr(card, attribute):
            if path not in seen:
                seen.append(path)
    return seen


async def template_context(request: Request) -> dict:
    """What dashboard.html needs to render every registered tab and card."""
    contexts: Dict[str, dict] = {}
    for card in _CARDS:
        if card.context is None:
            contexts[card.name] = {}
            continue
        try:
            contexts[card.name] = await card.context(request)
        except Exception as exc:  # noqa: BLE001 - isolate one card's failure from the page
            _log_failure(f"build dashboard card {card.name}", exc)
            detail = _safe_exception_detail(exc)  # URL-scrubbed: URLs carry tokens
            contexts[card.name] = {"error": f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__}
    return {
        "extra_tabs": list(_TABS),
        "cards_by_tab": cards_by_tab(),
        "card_contexts": contexts,
        "card_scripts": _unique_assets("scripts"),
        "card_styles": _unique_assets("styles"),
    }
