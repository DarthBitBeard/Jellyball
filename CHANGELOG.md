# Changelog

All notable changes to Jellyball are documented in this file.
The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [2.1.0] - 2026-10-04

### Added

- **Automatic database backup before a schema migration.** When an upgrade
  has to change the database layout, Jellyball first copies the database to
  `sports_proxy.db.bak-<version>` next to it (the newest three copies are
  kept; a brand-new install makes none). The README's troubleshooting section
  explains how to roll back with a copy.
- Docker image on GHCR: Tagged releases publish `ghcr.io/darthbitbeard/jellyball` (linux/amd64, with build provenance and an SBOM; pre-releases get only their exact tag). `docker-compose.yml` now pulls that image by default, declares a healthcheck, and still builds locally with `docker compose build`. Set `JELLYBALL_IMAGE` to pin a version.
- Documentation: New `docs/` guides: ARCHITECTURE, PROVIDERS, JELLYFIN (including Jellyfin 12 notes and a client compatibility table, all clients still unverified), TROUBLESHOOTING, DOCKER (GPU, reverse proxy and TLS) and UPGRADING (backups and rollback).
- Checksums for every release: The release now includes a `SHA256SUMS` file for the installer (written by `build-installer.ps1` after signing, in `sha256sum` format).
- Installer switches for unattended installs: `JellyballSetup-<version>.exe /VERYSILENT` now accepts `/PORT=`, `/USER=`, `/LAN=1|0` and `/DATADIR="D:\Folder"` (the data folder, handed to the service as its own `JELLYBALL_DATA_DIR`). Invalid values stop the install with a message. There is no password switch on purpose: a silent install without one lets Jellyball generate a password on first start in `dashboard-password.txt` in the data folder, and the installer log (`/LOG=`) names that file, never the password.
- Count fallbacks to the legacy proxy by reason (fMP4, SAMPLE-AES, separate audio) and provider, in /metrics and on the Performance tab.
- Linux systemd service: `deploy/linux/` has a `jellyball.service` unit and an install guide for running Jellyball without Docker.
- Live Sessions panel on the Performance tab: per-channel bitrate, p95 latency, edge age, memory and failovers with sparklines, plus provider breaker state.
- Preferred audio language: choose which audio track to use when a source carries several (Playback tab); the default is unchanged.
- Provider silent alerts: a notification when a provider's index page lists no events for hours, its circuit breaker opens, or it has not succeeded in five days (at most one per provider and reason every six hours).
- Provider Status card on the Performance tab: last success, index events, circuit breaker state, a Test button that runs one dry-run search, and per-provider enable/disable and priority.

### Changed

- Provider searches now record what actually happened (ok, empty, timeout or error), an error class, and how many events the provider's index page listed, so a site that silently stopped working is visible. History is kept for 14 days instead of 7.

### Fixed

- The Performance tab now refreshes when you switch to it, not only when the page was loaded on it.
- The Provider Health Leaderboard no longer shows 0% for every provider: failovers are charged to the provider that failed (the event also names the successor) and the rate is the 24-hour search success rate.
- The per-channel Test button now probes the channel's streams (playlist and a segment) instead of reporting the in-memory health flag.

### Security

- **Dependency refresh closes every published advisory.** `pip-audit` reported
  24 advisories in three pinned packages: Starlette (5, fixed in 1.3.1),
  Pillow (18, fixed in 12.3.0) and python-dotenv (1). Starlette moves from
  0.52.1 to 1.7.0, which brings FastAPI 0.128.8 to 0.142.2 and uvicorn
  0.52.4 to 0.54.0; Pillow is now 12.3.0 and python-dotenv 1.2.4. Also
  refreshed: cryptography 50.0.2, beautifulsoup4 4.15.0, tzdata 2026.4.
  Playwright moves to 1.63.0 and PyInstaller to 6.22.3, keeping the bundled
  Chromium build. No configuration or URL changes.

### Internal

- A 2.0.0 compatibility test suite freezes channel ids, `tvg-id`s, the exact
  M3U playlist, XMLTV channel ids, the public routes and database upgrades, so
  refactors cannot silently break an existing Jellyfin setup. CI also gained
  non-required coverage, mypy, Python 3.13 and `pip-audit` jobs.
- A tag push now runs the unit tests, linters and type check before building; publishes the installer, `SHA256SUMS` and the Docker image; takes the release notes from the matching CHANGELOG.md section; and marks tags containing a hyphen (for example `v2.1.0-beta.1`) as pre-releases.

## [2.0.1] - 2026-10-02

A small Jellyfin 12 compatibility fix for the automatic guide refresh, plus
documentation corrections. Nothing in the streaming path changed, and no
configuration, URL or database change is needed to upgrade.

### Fixed

- **Automatic guide refresh on Jellyfin 12.** Jellyfin 12 deprecated the
  legacy `X-Emby-Token` header that Jellyball used to authenticate its
  "Refresh Guide" call, and a rejected call failed silently. Jellyball now
  sends `Authorization: MediaBrowser Token="…"` first, falls back to the
  legacy header, and remembers which one works, so it runs on Jellyfin 10.11
  and 12.x.
- A stale saved "Refresh Guide" task ID (for example after Jellyfin was
  reinstalled) is rediscovered once automatically instead of failing forever.
- Documentation that did not match the product: the dashboard-auth note now
  lists the FastAPI `/docs`, `/redoc` and `/openapi.json` pages, which are
  unauthenticated; the bandwidth cap is per session, not per client; removed
  mentions of Jellyfin "event-time lookups", a manual-add control and a
  test-alert button that the dashboard does not have; documented
  `TRUST_X_FORWARDED_HOST`; and corrected the Docker note that the generated
  dashboard password is printed to the logs (it is only written to
  `dashboard-password.txt`).

### Added

- **Visible Jellyfin status.** The Alerts & Integrations tab shows the Jellyfin
  version and the outcome of the last guide refresh (OK, or why it failed:
  server unreachable, API key rejected, or no "Refresh Guide" task). A failing
  refresh also flags the Jellyfin badge in the dashboard header, and "Test
  Jellyfin Connection" now names the step that failed. `/api/version` reports
  the same data under `jellyfin`. A failure is logged once with its HTTP
  status (repeats at most hourly) and never includes the API key.

### Changed

- Outbound Jellyfin and webhook requests identify as `Jellyball/<version>`.
- The OpenAPI title is "Jellyball" (it was an internal codename).
- The 2.0.0 entry below now also lists the post-release-candidate hardening
  that the `v2.0.0` tag already contained, and carries the actual release
  date.

## [2.0.0] - 2026-09-30

Jellyball 2.0.0 is a stability and packaging release. The streaming core was
rebuilt around per-channel sessions with a real TS normalizer so failovers no
longer freeze Jellyfin playback, the dashboard gained a proper Advanced
Settings registry and observability endpoints, and Jellyball now ships as a
signed-optional Windows service installer with a supported Docker image.

### Added

- **Windows service installer.** `JellyballSetup-<version>.exe` (Inno Setup)
  installs Jellyball as a Windows service (`NT SERVICE\Jellyball`, delayed
  auto-start, restarts itself on crash), asks for a port and dashboard
  sign-in on first install, locks down the data directory's permissions, and
  optionally opens a local-subnet firewall rule. A "Jellyball Dashboard"
  Start Menu shortcut is added. See "Upgrading from 1.x" below for what an
  upgrade preserves.
- **Seamless failover.** Each channel is now a persistent `ChannelSession`
  that owns its own monotonic HLS sequence numbers and stitches source
  switches together with `#EXT-X-DISCONTINUITY` markers instead of handing
  Jellyfin's ffmpeg a broken stream. A new TS normalizer rewrites PIDs,
  PAT/PMT, and PTS/DTS/PCR per segment so playback continues through a
  failover instead of stalling.
- **Multi-View.** Composite 2 (side-by-side) or 4 (2x2 grid) existing
  channels into one server-side ffmpeg-encoded feed, created from the
  dashboard's Channels tab. Each member's audio is exposed as its own
  Jellyfin channel (`/multiview/{id}/audio-N.m3u8`) so viewers can switch
  commentary from Jellyfin's audio track picker without restarting the
  encode. Hardware encoding (NVENC or Quick Sync) is used when available and
  falls back to software (libx264) automatically, including a temporary
  fallback after an NVENC failure that retries hardware again later. Members
  that are slow to start, or removed, are backfilled from a placeholder feed
  instead of stalling the whole grid.
- **Observability.** `/api/sessions` reports each channel's recent bitrate,
  segment latency (avg/p95), edge age, uptime, provider, codec, and
  placeholder/exhausted state and failover history. `/metrics` serves the
  same numbers in Prometheus text format (behind dashboard auth).
  `/api/status` gains `watching`, `on_placeholder`, and `failover_count`.
- **Advanced Settings.** The old "Playback Settings" sliders were replaced by
  a registry of about 30 health/failover, scraping, session, Multi-View and
  legacy-proxy tunables, each editable live from the dashboard's Settings
  tab (env var sets the default; a saved override applies immediately,
  including to already-running channel sessions where relevant).
- **Provider Domains.** The dashboard's Alerts & Integrations tab gained a
  "Provider Domains" card and `POST /settings/providers` so aggregator base
  URLs can be changed live, without restarting Jellyball, when a provider's
  domain moves.
- **Off-season toggle.** A Settings-tab toggle keeps a team's channel listed
  in the M3U/guide during its off-season with an "Off-season (resumes
  &lt;date&gt;)" guide entry, instead of removing it (default: removed, as
  before).
- **Opt-in update check.** A Settings toggle (default off) checks GitHub
  Releases twice a day and shows a dashboard banner when a newer release
  exists; `/api/version` reports it too.
- **Signed relay URLs.** The legacy `/chunk`, `/resource`, and
  `/substream.m3u8` routes now only serve HMAC-signed URLs that Jellyball
  itself wrote into a rewritten playlist — they no longer act as an open
  fetch relay for arbitrary URLs.
- **Dashboard password enforcement.** Binding to anything other than
  loopback now requires a dashboard password. If `DASHBOARD_PASSWORD` isn't
  set, Jellyball generates one on first start and stores it in
  `dashboard-password.txt` in the data directory.
- **CSRF protection.** A pure-ASGI middleware rejects cross-site
  POST/PUT/PATCH/DELETE requests to the dashboard by checking the browser's
  `Origin`/`Referer` against the request's `Host`.
- **SQLite schema migrations.** A `schema_migrations` table replaces the old
  ad-hoc `ALTER TABLE` loop; new indexes were added on `teams(catalog_key)`
  and `teams(is_favorite)`.
- **Alert retries.** Discord/Telegram webhook delivery now retries up to
  twice (honoring `Retry-After`) on network errors, 5xx, or 429 responses.
- **Optional code signing** for the installer build, via a traditional
  code-signing certificate (`SIGN_CERT_THUMBPRINT`) or Azure Trusted Signing
  (`AZURE_SIGNING_DLIB` / `AZURE_SIGNING_METADATA`, `SIGN_TIMESTAMP_URL`).
  Unconfigured by default; unsigned builds work exactly as before.
- **CI.** Unit tests and lint run on Windows and Linux for every push/PR to
  `master`/`release-*`; a separate release workflow builds and (optionally)
  signs the installer and attaches it to tagged GitHub releases.
- Extendable non-English channel filter (`EXTRA_NON_ENGLISH_MARKERS`).
- Per-provider aggregator base-URL overrides stored in the database, with
  the environment variable as the fallback default.

### Changed

- **Per-channel stream URLs are now `/stream/{id}.m3u8`** (an HLS media
  playlist served by the new session engine), not the old `.ts` chunk proxy
  route. The main M3U/XMLTV endpoint URLs (`/playlist.m3u`, `/epg.xml`)
  are unchanged.
- **Multi-View prefers Jellyfin's own ffmpeg** when Jellyfin is installed on
  the same machine, since it's built against the driver Jellyfin already
  transcodes with; resolution order is `MULTIVIEW_FFMPEG_PATH` →
  `FFMPEG_PATH` → Jellyfin's ffmpeg → the bundled binary.
  Multi-View audio-channel URLs changed from
  `/multiview/{id}/audio/{n}.m3u8` to `/multiview/{id}/audio-{n}.m3u8` so
  Jellyfin's M3U parser can't mistake the trailing number for a channel
  number. The old form is kept as a deprecated alias.
- **Multi-View guide data** now reflects each pane's real per-member
  schedule (with a merged, capped set of intervals) instead of one flat
  block for the whole grid.
- Failover no longer adds playback latency: only the actual outage gap is
  appended on a mid-playback source switch, instead of a fixed few seconds
  every time.
- Health probes now detect a frozen (non-advancing) playlist, not just an
  unreachable one, and carry per-candidate state across probes.
- Exhausted channels (all sources failed) now recover automatically: every
  20 seconds all candidates are re-probed and the channel snaps back to the
  first one that answers, instead of staying on "No Signal" until the next
  scheduled rescrape.
- Rescrapes merge new candidates into the existing list instead of
  replacing it, so a healthy active source and standby health history
  survive a rescrape.
- Team matching: same-market identity conflicts (e.g. NY Yankees vs. Mets,
  Chicago Bears vs. Cubs) are now derived from the team catalog instead of
  three hand-listed special cases, closing several near-miss collisions.
- The Athletics' canonical display name changed from "Oakland Athletics" to
  "Athletics" (their current ESPN display name); old aliases still resolve
  and the catalog's persisted team ID is unchanged.
- Docker image is pinned to `python:3.12-slim-bookworm`, runs as a
  non-root user, and adds an `HEALTHCHECK` against `/healthz`.
- `DEFAULT_USER_AGENT`'s bundled fallback Chrome version was bumped to
  match the pinned Playwright browser build (override with
  `JELLYBALL_USER_AGENT`).
- `/metrics` exports failover-reason counters, deferred-failover counts,
  emergency-rescrape totals/duration, provider circuit-breaker state, Playwright
  page usage, and aggregate session memory.
- `stream_state` entries are typed via `ChannelState` / `new_channel_state()`.
- Release workflow signing env vars match `build-installer.ps1`
  (`AZURE_SIGNING_DLIB` / `AZURE_SIGNING_METADATA`).
- CI runs e2e tool wrappers (Windows) and a Docker compose health smoke (Ubuntu).

### Fixed

- A failover mid-playback used to freeze the Jellyfin player; sessions now
  signal discontinuities correctly and ffmpeg's HLS demuxer keeps decoding.
- `/chunk` no longer leaks its single-flight cache key or upstream
  connection when the client disconnects mid-body or any exception occurs.
- Startup warm-up now fetches the live edge of a stream instead of the
  oldest (often already-expired) segments.
- Segment-name regexes accept 6+ digit sequence numbers (the placeholder
  channel used to 404 after roughly 4.6 days of uptime).
- Passlib's bcrypt shim (broken under bcrypt 5.x) was replaced with direct
  bcrypt verification and a byte-safe, constant-time compare.
- Dashboard-rendered dynamic values are now escaped before being written via
  `innerHTML`, closing a stored-XSS-shaped path through hostile channel
  names.
- Multiple duplicate-canonical-key bugs in the team catalog (Texas A&M
  Aggies, UCF Knights) that made those teams permanently unmatchable by
  some of their own search terms.
- Installer: ACL reset on upgrade no longer strips existing files'
  inherited permissions without granting new ones (this had crashed the
  service after an upgrade, locked out of its own database and log).
- Installer: ACLs are now applied only after the Windows service account
  exists, so they can actually resolve `NT SERVICE\Jellyball`.
- Cached DB connections are now closed on shutdown (thread/DB-path scoped
  reuse, reconnecting on `sqlite3.ProgrammingError`/`OperationalError`).
- A DNS-only-private redirect target is now rejected by the live-stream
  health probe (SSRF hardening), not just the initial URL.
- HLS sessions no longer advance `last_useq` on failed segment downloads, so a
  transient CDN blip is retried on the next poll instead of permanently skipped.
- Failover and emergency-rescrape candidate mutations are serialized on the
  per-team state lock, closing races between `request_failover` and partial
  scrape installs.
- On `playlist forbidden`, same-host / same-provider standbys are no longer
  burned before a token refresh rescrape lands.

### Security

- Dashboard auth is now enforced based on bind address: a network bind
  (Docker's `0.0.0.0`, a LAN address) without `DASHBOARD_PASSWORD` gets a
  generated password instead of silently running open.
- Open (loopback, no password) mode still requires a loopback `Host` header,
  closing a DNS-rebinding path to the otherwise-open local dashboard.
- Failed dashboard logins are rate-limited per client.
- Secrets (API keys, webhook URLs) use masked inputs in the dashboard: a
  blank field keeps the existing value, a checkbox is required to clear it.
  Changing the Jellyfin host requires re-entering the API key.
- `/api/import-config` now runs imported teams through the same add-team
  sanitizer and logo-URL validation as the UI path.
- M3U/XMLTV output strips control characters and quote-escapes XML
  attributes; the `Host` header used to build handed-out URLs is validated.
- Legacy relay routes (`/chunk`, `/resource`, `/substream.m3u8`) only serve
  HMAC-signed URLs now (see "Added" above).
- Redirect hops and nested playlist/segment URL resolution in the live
  probe now use the DNS-checking URL validator, not the sync-only one.
- CSRF Origin checks ignore `X-Forwarded-Host` unless `TRUST_X_FORWARDED_HOST=1`.
- Generated `dashboard-password.txt` and `relay-signing.key` are written with
  mode `0600`; the generated dashboard password is no longer logged in plaintext.

### Removed

- The dead `/placeholder/*` routes (the legacy path now redirects to the
  placeholder session instead).
- `is_fuzzy_match`, `_record_playback_event_sync`, `get_stability_metrics`,
  and the `/api/playback-settings` route (superseded by Advanced Settings).
- `_diagnose_always_live.py` (a stale diagnostic script; the README no
  longer references it).
- The dashboard's old "Playback Settings" sliders (see Advanced Settings
  above — saved slider values are carried over as the equivalent Advanced
  Settings override).
- Dead `FuzzFallback` / unused `fuzz` import and `safe_get_content` from
  `scrapers.py`; unused `CACHE_TTL_SECONDS` from `legacy_proxy.py`.

### Deprecated

- `/multiview/{id}/audio/{n}.m3u8` (old Multi-View per-audio URL form) —
  use `/multiview/{id}/audio-{n}.m3u8`. The old form is kept working until
  you refresh Jellyfin's guide data, and may be removed in a future release.

---

## Upgrading from 1.x

- **M3U stream URLs changed.** Per-channel stream URLs are now
  `/stream/{id}.m3u8` instead of the old chunk-proxy form. Refresh
  Jellyfin's M3U tuner (**Dashboard → Live TV → Tuner Devices → ⋮ → Refresh
  Guide Data**, or remove and re-add the tuner) after upgrading so it picks
  up the new URLs. The playlist and guide endpoints themselves
  (`/playlist.m3u`, `/epg.xml`) did not move.
- **Multi-View per-audio channel URLs changed** from
  `/multiview/{id}/audio/{n}.m3u8` to `/multiview/{id}/audio-{n}.m3u8` (so
  Jellyfin can't mistake the audio index for a channel number). The old
  alias still works but is deprecated — refresh the guide to pick up the
  new URLs.
- **A dashboard password is now required when listening beyond
  localhost.** If you bind to a network address (`JELLYBALL_HOST=0.0.0.0`,
  a Docker deployment, or a LAN IP) without `DASHBOARD_PASSWORD` set,
  Jellyball generates a random password on first start and writes it to
  `dashboard-password.txt` in the data directory. Set `DASHBOARD_PASSWORD`
  explicitly to choose your own.
- **Playback Settings → Advanced Settings.** The old dashboard sliders were
  replaced by the Advanced Settings registry on the Settings tab. Any
  slider values you had saved carry over automatically as the equivalent
  Advanced Settings overrides — no action needed.
- **`/placeholder/*` routes were removed.** Nothing else references them;
  if you had a bookmark or external tool pointed at one directly, point it
  at the current placeholder/`No Signal` behavior instead (it's served
  automatically wherever a channel has no live source).
- **Legacy relay URLs are now signed.** `/chunk`, `/resource`, and
  `/substream.m3u8` only accept HMAC-signed URLs that Jellyball itself
  generates in the M3U/HLS rewriting path. You don't need to do anything —
  existing Jellyfin tuners regenerate these URLs on their own — but an
  external tool that constructed one of these URLs by hand will need to
  fetch it from Jellyball's own output instead.
- **The installer keeps your `.env` and database on upgrade.** Running a new
  `JellyballSetup-<version>.exe` over an existing install does not touch
  `%ProgramData%\Jellyball\.env` or the SQLite database; the configuration
  wizard page is skipped entirely when a `.env` already exists. Uninstalling
  still asks (default: No) before deleting `%ProgramData%\Jellyball`.
- **Data directory locations are unchanged:**
  - Windows service: `%ProgramData%\Jellyball`
  - Windows tray app (no service installed): `%LOCALAPPDATA%\Jellyball`
  - Docker: whatever host path you mount at `/app/data` (`JELLYBALL_DATA_DIR`)
  - Linux/source: `$XDG_DATA_HOME/Jellyball` or `~/.local/share/Jellyball`

  `JELLYBALL_DATA_DIR` overrides all of the above if set.
