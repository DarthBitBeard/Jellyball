# Providers

A "provider" is a website Jellyball reads to find live streams for a game or a
24/7 channel. Jellyball does not host or produce any streams; it asks each
provider what it currently lists, picks playable HLS candidates, and keeps the
best one on air, switching to a backup when it fails. This page explains how
that works and what you can do when a provider breaks. Everything here
describes the code as it is today; **planned** items are not implemented yet.

Providers are third-party sites that change or disappear without notice.
Jellyball ships no guarantee that any given one works on any given day, and it
does not circumvent anti-bot, CAPTCHA or DRM protections: a source that
requires those is simply skipped.

## Kinds of provider

| Kind | Providers | Used for |
| :--- | :--- | :--- |
| Event directories | iSportSurge, MyBuffStreams, MethStreams, StreamEast, Footybite, 1Stream, Streamed, TopStreams | Per-game streams: Jellyball reads the site's index pages, matches event links against the team (see below) and extracts a playlist from the event page. |
| Linear-channel adapters | TheTVApp, DaddyLive | 24/7 channels (ESPN, NFL RedZone, ...). Always-live channels are searched only on the linear providers. |
| Static playlist | IPTV-Org | A cached public sports playlist, used only as an independent backup for 24/7 channels. It does no scraping, so it still works when every HTML provider is down. |

The list is fixed in `Jellyball/scrapers.py` (`ACTIVE_PROVIDERS`); per-provider
enable/disable and priority controls in the dashboard are **planned**.

## How a search works

1. For a channel, `master_scrape` searches the providers concurrently
   (`PROVIDER_SEARCH_CONCURRENCY`, per-search timeout `PROVIDER_SEARCH_TIMEOUT`).
2. Each event-directory provider fetches its category/index pages (cached for
   `SCRAPE_INDEX_CACHE_SECONDS`), finds links that look like events, and keeps
   those whose text matches the team. Matching is identity-aware
   (`sports_matcher.py`): "Eastern Michigan" does not match "Michigan State".
3. For each matching event page the stream extractor looks for an HLS
   playlist. It uses plain HTTP where it can and a shared headless Chromium
   (Playwright) where the page needs a browser. If Chromium cannot start,
   Jellyball logs it and continues with HTTP-only extraction.
4. Candidates are probed for liveness (`VERIFY_STREAM_CONCURRENCY`) and ranked
   by match quality, provider priority and HLS quality. At most
   `MAX_STREAM_CANDIDATES` are kept per channel.
5. The best candidate goes on air; the others stay as standbys and are
   health-checked at `STANDBY_HEALTH_INTERVAL` / `UNWATCHED_STANDBY_INTERVAL`.

All outbound fetches go through the SSRF guard: private, loopback and
link-local targets are rejected, including through redirects, unless
`ALLOW_PRIVATE_UPSTREAMS=1` (testing only).

## What Jellyball does when a provider misbehaves

* **Circuit breaker.** After `PROVIDER_BREAKER_FAILURES` (default 3)
  consecutive failures a provider is skipped for `PROVIDER_BREAKER_COOLDOWN`
  seconds (default 120), then tried again. Breaker state is exported as
  Prometheus metrics at `/metrics`.
* **Adaptive timeout.** Each provider's search timeout is learned from recent
  responses and clamped between `PROVIDER_TIMEOUT_MIN` and
  `PROVIDER_TIMEOUT_MAX`.
* **Failover.** If a live source stops delivering, the channel switches to the
  next healthy candidate without interrupting playback and an emergency
  rescrape runs (`EMERGENCY_SCRAPE_COOLDOWN`). If everything is dead the
  channel shows "No Signal" and keeps probing (`EXHAUSTED_PROBE_INTERVAL`).
* **Alerts.** With a Discord or Telegram webhook configured you are told about
  failovers, exhausted channels and recoveries (see the README).

## Configuring providers

| What | How |
| :--- | :--- |
| A provider moved to a new domain | Dashboard, Alerts & Integrations tab, **Provider Domains** card: change the base URL, no restart. Or set the matching `AGGREGATOR_N_URL` variable. |
| Prefer some providers | `STREAM_PROVIDER_PRIORITY` (comma-separated names, e.g. `iSportSurge,MyBuffStreams`) breaks ties between equal candidates. |
| Spread load | Dashboard, Settings tab, **Provider Rotation**: rotates the preferred provider hourly. |
| Tune timeouts, breaker, concurrency | Advanced Settings (Settings tab) or the variables above; see the README configuration tables. |
| The fallback playlist | `IPTV_ORG_PLAYLIST_URL`, `IPTV_ORG_REFRESH_SECONDS`. |

`AGGREGATOR_N_URL` numbering (the gaps are providers retired earlier):
1 iSportSurge, 2 MyBuffStreams, 3 MethStreams, 4 StreamEast, 5 TheTVApp,
6 DaddyLive, 9 Footybite, 10 1Stream, 11 Streamed, 12 TopStreams. Current
defaults are in `Jellyball/.env.example`.

## When a provider "stops working"

1. Open the **Performance** tab for per-provider response times and success
   rates, and `/metrics` (`jellyball_` series) for breaker state.
2. Check the log (Logs tab or `jellyball.log`) for lines naming the provider.
3. If the site's address changed, update it in **Provider Domains**.
4. If the site now needs a login, CAPTCHA or a changed page structure, Jellyball
   cannot use it until the adapter is updated. Open an issue with the provider
   name and the log lines, and never include URLs that carry tokens.
5. In the meantime the other providers keep working; set
   `STREAM_PROVIDER_PRIORITY` so a healthy one wins ties.

**Planned for 2.1.0 (not in the code yet):** a "Test provider" dry-run, a
"provider silent" alert, last-success and parsed-event counts on the provider
cards, per-provider enable/disable and priority, and reliability-aware ranking
from observed playback outcomes.

## For contributors: adding or changing a provider

* Event-directory providers subclass `HtmlAggregatorScraper` in
  `scrapers.py`: give it a name, a base URL read from an `AGGREGATOR_N_URL`
  variable, the index/category paths to scan and the URL fragments that mark
  an event page. Add an instance to `ACTIVE_PROVIDERS` (and to
  `LINEAR_PROVIDERS` only if it carries 24/7 channels).
* Do not put real provider hostnames, tokens or captured pages into tests or
  fixtures; tests use synthetic HTML.
* Do not add anti-bot, CAPTCHA or DRM circumvention.
* Add a changelog fragment under `changelog.d/`.
