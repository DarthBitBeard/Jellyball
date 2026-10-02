"""HTTP routes (and, through dashboard_cards, dashboard cards) for Jellyfin integration and the programme guide.

Pre-wired in Phase A so this lane can add endpoints without editing main.py or
other lanes' files (J3-J7 and G1-G3 in the 2.1.0 plan). Rules:

- Protect every route with `auth: bool = Depends(verify_dashboard_auth)`.
  test_auth_guardrail fails on a route that answers an anonymous request unless
  its author lists it as public by design, which a reviewer will see.
- Keep the logic in its own modules; this file is the thin HTTP layer.
- Register dashboard cards or tabs here with dashboard_cards.register_card.
"""

from fastapi import APIRouter

router = APIRouter()
