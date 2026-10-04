# Architecture

A map of how Jellyball is put together, for people who want to fix a bug or add
a feature. It describes the code as it is today (every claim below was checked
against `Jellyball/*.py`); things marked **planned** are on the 2.1.0 roadmap
and are not in the code yet. Settings are listed in
[`Jellyball/README.md`](../Jellyball/README.md#configuration).

## What it is

One Python process (FastAPI on uvicorn, a single worker: `WEB_CONCURRENCY` must
stay `1` or startup is refused) that

1. finds live streams for the teams and channels you picked (**scrapers**),
2. keeps one continuous HLS feed per channel and switches sources underneath it
   when one dies (**failover** + **sessions**),
3. serves Jellyfin an M3U tuner list and an XMLTV guide (**epg**),
4. optionally composites 2 or 4 channels into one feed with ffmpeg
   (**Multi-View**),
5. offers a dashboard and a small JSON API.

State lives in one SQLite file (`sports_proxy.db`) in the data directory plus
in-memory dictionaries; the process is not horizontally scalable.

## Data flow

```
scrapers (event-directory + linear-channel providers)
        |  candidates, ranked (match quality, provider priority, HLS quality)
        v
   failover engine ---> channel session (owns the HLS timeline)
        |                       |
        |                       v
        |                TS normalizer (PIDs, PAT/PMT, PTS/DTS/PCR per epoch)
        |                       |
        |                       v
        |                /stream/{id}.m3u8 + /stream/{id}/seg/{seq}.ts   <- Jellyfin
        v
  /playlist.m3u  and  /epg.xml                                          <- Jellyfin
```

Sources the session engine cannot normalise (fMP4, demuxed audio, SAMPLE-AES)
go through the **legacy relay** (`/chunk`, `/resource`, `/substream.m3u8`,
HMAC-signed URLs only). Failover marks such a source "session-incompatible" and
prefers a normal one when available.

## Modules

| Module | Role |
| :--- | :--- |
| `main.py` | Composition root: builds the FastAPI app, lifespan (DB init, Playwright start, background tasks, shutdown), middleware, routers; runs headless or under the tray (`TrayApplication`). |
| `config.py` | Resolves the data directory, loads `.env`, starts logging; URL/Host helpers (`_public_base_url`, `_validate_upstream_url`). |
| `jellyball_launcher.py` | Packaged Windows entry point: tray app, `--console`, or `--service` (Windows service, data in `%ProgramData%\Jellyball`). |
| `db.py` | SQLite access, settings table, metrics, schema migrations, pre-migration backups. |
| `migrations_<lane>.py` | Per-feature migration lists with reserved version ranges (jellyfin 100-199, sports 200-299, providers 300-399, setup 400-499, engine 500-599). `db.py` folds them together. |
| `scrapers.py` | Provider classes (`HtmlAggregatorScraper` subclasses, `IptvOrgScraper`), circuit breaker, index-page cache, `master_scrape`. |
| `stream_extractor.py` | Finds an HLS playlist in a provider page (Playwright or HTTP), validates it. |
| `sports_matcher.py`, `sports_catalog.py`, `catalog.py` | Team identity and alias matching, the ESPN team directory, special channels, season windows. |
| `channels.py` | Add/remove channels, catalog enable/disable, scheduled auto-disable. |
| `failover.py` | Per-channel scrape loops, candidate merging, health probes, the failover monitor, emergency rescrapes. |
| `hls_session.py`, `sessions.py` | The channel session: a proxy-built continuous playlist, segment fetching, the registry and responses. |
| `ts_normalize.py` | Rewrites MPEG-TS so segments from different sources splice cleanly (a switch is signalled as an HLS discontinuity). |
| `legacy_proxy.py` | The relay for sources the session engine cannot handle; chunk cache, prefetch, startup buffer. |
| `placeholder.py` | The shared "No Signal" stream (one ffmpeg run started on demand). |
| `multiview.py`, `ffmpeg_proc.py` | Grid compositing (NVENC/QSV/libx264), restart backoff, watchdog; ffmpeg discovery and child-process control. |
| `epg.py` | `/playlist.m3u` and `/epg.xml` generation, including Multi-View programme composition. |
| `alerts.py` | Discord/Telegram alerts and the debounced Jellyfin guide refresh. |
| `jellyfin_client.py` | The Jellyfin REST client (guide refresh, version check). See [JELLYFIN.md](JELLYFIN.md). |
| `security.py`, `network_safety.py` | Dashboard auth modes and lockout, CSRF origin middleware, HMAC signing of relay URLs; SSRF validation of upstream URLs. |
| `tunables.py` | The registry behind Advanced Settings (live-editable values). |
| `updates.py` | Opt-in update check against GitHub Releases. |
| `routes_*.py`, `dashboard_cards.py` | HTTP layer. `routes_api.py` (health, status, metrics), `routes_dashboard.py` (pages), `routes_stream.py`; empty lane routers (`routes_jellyfin`, `routes_sports`, `routes_providers`, `routes_setup`, `routes_engine`) are the extension points for new features. |

## Startup and shutdown

`main.lifespan` runs in this order: initialise the database (running pending
migrations after a backup), start the metric writer, kill ffmpeg processes left
by a crashed previous run, clear Multi-View/placeholder scratch directories,
load Advanced Settings overrides, start the update-check loop, start Playwright
and a shared headless Chromium (if that fails, HTTP-only extraction continues),
create the shared HTTP clients, check that ffmpeg is available, restore
channels and Multi-View channels from the database and start their scrape loops
(staggered over `STARTUP_SCRAPE_SPREAD_SECONDS`). On stop, uvicorn gets 10 seconds to drain streams, then
sessions and ffmpeg children are stopped.

## Where the data lives

* **Data directory**: `$JELLYBALL_DATA_DIR`, else `%LOCALAPPDATA%\Jellyball`
  (tray app), `%ProgramData%\Jellyball` (Windows service), `~/.local/share/Jellyball`
  (Linux, from source) or `/app/data` (Docker image). Holds `.env`,
  `sports_proxy.db`, `jellyball.log`, `dashboard-password.txt` (only when
  generated), `relay-signing.key`, backups (`sports_proxy.db.bak-<version>`) and
  scratch directories.
* **Settings precedence** (highest first): process environment, the data
  directory's `.env`, a `.env` beside the program. A value saved from the
  dashboard's Advanced Settings or Provider Domains card is stored in the
  database and applies live.

## Compatibility contract

These must keep working across upgrades (they are frozen by
`test_compat_2_0_0.py`): channel ids, `tvg-id`s, `/playlist.m3u`, `/epg.xml`,
`/stream/*`, the public routes, the `.env` file and database upgrades. Changing
any of them breaks Jellyfin installations that already map channels.

## Extension points

* **Database**: add migrations in your lane's `migrations_<lane>.py`, inside its
  reserved range, as idempotent functions. Never edit or renumber a shipped
  migration.
* **HTTP**: add routes to the lane's `routes_<lane>.py`. Every route needs
  `Depends(verify_dashboard_auth)` unless it is public on purpose and listed in
  `test_auth_guardrail.py` with a reason.
* **Dashboard**: `dashboard_cards.register_card` adds a card to a tab without
  editing the shared template.
* **Settings**: `tunables.register_tunables` exposes a value in Advanced
  Settings.
* **Changelog**: add a fragment `changelog.d/<slug>.<type>.md`; do not edit
  `CHANGELOG.md` (see `changelog.d/README.md`).
* New modules must be listed in `hiddenimports` in `Jellyball/jellyball.spec`
  (`test_packaging.py` enforces it), or the Windows build will not include them.

## Planned for 2.1.0 (not in the code yet)

Stored per-game schedules and richer guide entries, a connect-Jellyfin wizard,
a diagnostics bundle and a login page. See the 2.1.0 changelog when it ships
rather than relying on this list.

## Shipped in 2.1.0

Per-provider health and enable/disable controls: the Provider Status card on
the Performance tab (`routes_providers.py`, `provider_settings.py`).
