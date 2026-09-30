# Copilot Instructions

## Project Guidelines
- For Jellyball, preserve synchronous SQLite helpers but expose async wrappers using asyncio.to_thread; use a self-healing shared Playwright browser, an asyncio.Lock-protected bounded chunk cache, and await tracked background-task cancellation during shutdown.
- For Jellyball, keep event-directory providers separate from 24/7 linear-channel providers: event scrapers should scan only valid base/category paths and be tested with active event terms, while TheTVApp/DaddyLive must not be added or guessed without an authorized endpoint or playlist/API contract.