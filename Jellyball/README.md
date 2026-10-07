# Jellyball

A Jellyfin Live TV sports proxy. It scrapes/aggregates live sports streams
from multiple sources, serves them to Jellyfin as an M3U tuner + XMLTV guide,
fails over between sources without freezing playback, and can composite
several channels into a Multi-View grid. Runs as a Windows service, in
Docker, or from source.

---

## Features

- **Seamless failover.** Each channel is a persistent session with its own
  monotonic HLS timeline; a source switch is signaled as an HLS
  discontinuity instead of breaking the stream, so Jellyfin's ffmpeg keeps
  decoding through it.
- **Multi-aggregator scraping.** iSportSurge, MyBuffStreams, MethStreams,
  StreamEast, Footybite, 1Stream, Streamed, TopStreams (event-directory
  providers), plus TheTVApp and DaddyLive (24/7 linear-channel adapters) for
  always-live channels like ESPN and NFL RedZone.
- **Identity-aware team matching.** Rejects near-name collisions (Eastern
  Michigan vs. Michigan State, Rays vs. Buccaneers, same-market team
  conflicts) while keeping known aliases and nicknames.
- **Catalog-based channel selection.** Pick exact NFL/MLB/NHL/NBA/college
  identities from dashboard toggles; college entries refresh from ESPN when
  reachable.
- **Multi-View.** Composite 2 or 4 channels into one grid feed with
  hardware-accelerated ffmpeg (NVENC/QSV, falling back to libx264), with
  each member's audio also exposed as its own switchable Jellyfin channel.
- **Dynamic XMLTV guide.** `/epg.xml` starts ~1 hour in the past and covers
  the next 10 days, with realistic per-sport game durations (football 4h,
  basketball/hockey 3h, baseball 4h, soccer 2.5h) instead of a fixed
  rolling window — channels never go blank or show expired programme data.
- **Observability.** `/api/sessions`, `/api/status`, and a Prometheus
  `/metrics` endpoint; stability metrics logged to SQLite.
- **Alerts.** Discord/Telegram webhook notifications on failover, exhausted
  candidates, and recovery.
- **Advanced Settings.** ~30 health/failover/scraping/session/Multi-View
  tunables editable live from the dashboard, no restart needed.
- **Security.** Dashboard auth required on any non-loopback bind, CSRF
  origin checks, HMAC-signed legacy relay URLs, masked secret inputs.
- **Windows service installer** and a **Docker image**, both first-class.
- **Full DVR support** via Jellyfin's native Live TV recording.

---

## Quick start

### Windows (recommended): installer

1. Download `JellyballSetup-<version>.exe` from the
   [Releases](https://github.com/DarthBitBeard/Jellyball/releases) page and
   run it (requires administrator privileges).
2. On first install you'll be asked for a port (default `8000`), a dashboard
   username/password, and whether to allow other computers on your network
   to connect. These are written once to
   `%ProgramData%\Jellyball\.env` and are **never overwritten by a later
   upgrade** — the wizard skips this page entirely on an upgrade.
3. The installer registers a Windows service named **Jellyball**
   (`NT SERVICE\Jellyball`, delayed auto-start, restarts on crash), locks
   down `%ProgramData%\Jellyball` to Administrators/SYSTEM/the service
   account, and (if you allowed network access) opens a local-subnet
   firewall rule on the configured port.
4. A "Jellyball Dashboard" shortcut is added to the Start Menu. The final
   wizard page shows the M3U and XMLTV URLs to paste into Jellyfin (see
   [Jellyfin setup](#jellyfin-setup) below).
5. On uninstall, the service and firewall rule are removed; you're asked
   (default: **No**) whether to also delete `%ProgramData%\Jellyball`
   (settings and database).

To run in the foreground with visible logs (e.g. to diagnose an issue with
the installed service — stop the `Jellyball` service first so the two don't
fight over the same port and data directory):

```powershell
JellyballConsole.exe --console
```

Building the installer yourself: see [Building from source](#building-from-source-and-running-tests).

### Docker

1. Copy `Jellyball/.env.example` to `.env` next to `docker-compose.yml` and
   edit what you need (at minimum, review `DASHBOARD_PASSWORD`).
2. `docker compose up -d`
3. Open `http://<server-ip>:8000`. Docker binds `0.0.0.0` by default, so if
   you didn't set `DASHBOARD_PASSWORD`, read the generated password from
   `/app/data/dashboard-password.txt` (i.e. `./data/dashboard-password.txt`
   on the host with the default compose volume). Logs point at that file;
   they do not print the password itself.

Multi-View under Docker falls back to software encoding
(`MULTIVIEW_HWACCEL=none`) unless you build a CUDA-enabled image and wire up
the NVIDIA container runtime yourself — the stock image's ffmpeg has no
NVENC support.

### From source

```bash
cd Jellyball
python -m venv .venv && .venv\Scripts\activate   # or source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
cp .env.example .env   # edit as needed
python main.py
```

On Linux, install the browser with `python -m playwright install --with-deps
chromium` instead (needs root): without the system libraries Chromium cannot
launch and Jellyball silently falls back to HTTP-only stream extraction.

Requires Python 3.12+. On first launch, Jellyball creates its data directory
(`%LOCALAPPDATA%\Jellyball` on Windows, `~/.local/share/Jellyball` on Linux,
or `$JELLYBALL_DATA_DIR` if set) with a starter `.env`, `sports_proxy.db`,
and `jellyball.log`.

On the **Channels & Streams** tab, expand a Sports Catalog category, select
the teams/special channels you want, and click **Apply Selected Teams**
once. Catalog entries are off by default so Jellyball doesn't spin up
hundreds of provider searches unless you asked for them. Applying the
catalog treats the checked entries as the desired set — anything unchecked
that was previously enabled is stopped and removed.

---

## Jellyfin setup

1. **Dashboard → Live TV → Tuner Devices → +** → **M3U Tuner** → File or
   URL: `http://<jellyball-host>:8000/playlist.m3u`
2. **Dashboard → Live TV → TV Guide Data Providers → +** → **XMLTV** → File
   or URL: `http://<jellyball-host>:8000/epg.xml`
3. **Dashboard → Playback → Streaming**: make sure direct-stream is allowed
   for Live TV sources so Jellyfin's ffmpeg doesn't force an unnecessary
   transcode wrapper.
4. After changing catalog selections, refresh both the tuner and the guide
   (⋮ next to each → **Refresh Guide Data**). If Jellyfin keeps stale
   duplicate entries, remove and re-add the tuner.
5. To record: **Live TV → Guide** → select an active game → **Record**.
   Jellyfin's scheduler records the proxied stream into your normal TV
   library folder.

**Automatic guide refresh (optional).** In Jellyball's **Alerts &
Integrations** tab enter the Jellyfin URL and an API key (Jellyfin
**Dashboard → API Keys**) and Jellyball asks Jellyfin to refresh its guide
whenever the channel list or schedule changes. It authenticates with
`Authorization: MediaBrowser Token="…"` and falls back to the legacy
`X-Emby-Token` header, so it works on Jellyfin 10.11 and 12.x. The tab shows
the Jellyfin version and the result of the last refresh; **Test Jellyfin
Connection** triggers a refresh and says which step failed (server
unreachable, API key rejected, or no *Refresh Guide* task found). A failing
refresh also flags the Jellyfin badge in the dashboard header and is logged
once with its HTTP status (never the key).

Per-channel stream URLs are `/stream/{id}.m3u8` (HLS media playlists served
by the session engine); you don't need to reference these directly, they
come from the M3U playlist.

Always-live channels use stable `tvg-id`s so they can be mapped to an
external XMLTV EPG provider without changing the local proxy URL:

| Channel | `tvg-id` |
| :--- | :--- |
| NFL RedZone | `NFLRedZone.us` |
| ESPN | `ESPN.us` |
| ESPN2 | `ESPN2.us` |
| ESPNU | `ESPNU.us` |
| FOX Sports 1 | `FoxSports1.us` |
| FOX Sports 2 | `FoxSports2.us` |
| CBS Sports Network | `CBSSportsNetwork.us` |
| TNT Sports | `TNT.us` |
| NBC Sports | `NBCSports.us` |

College entries have separate IDs per sport for the same school — e.g.
Michigan Wolverines football is `ncaaf_130`, men's basketball is
`ncaam_130`.

---

## Multi-View

A Multi-View channel composites 2 (side-by-side) or 4 (2x2 grid) existing
channels into one server-side ffmpeg-encoded feed, created from the
**Channels** tab.

**ffmpeg resolution order** (first one found wins): `MULTIVIEW_FFMPEG_PATH`
→ `FFMPEG_PATH` → **Jellyfin's own ffmpeg**, if Jellyfin is installed on the
same machine (preferred — it's built against the NVENC API version the
NVIDIA driver on that box already supports, which the bundled ffmpeg may
not be) → the bundled binary (Windows exe builds that included one).

**Hardware encoding.** `MULTIVIEW_HWACCEL` is `nvenc` (NVIDIA), `qsv` (Intel
Quick Sync), or `none` (software `libx264`). An NVENC spawn failure falls
back to `libx264` for that run and for `NVENC_FALLBACK_SECONDS` (default
600s) afterward, then hardware is tried again automatically.

**Per-audio channels.** Each member's audio is also published as its own
Jellyfin channel at `/multiview/{id}/audio-N.m3u8` (titled "🔊 &lt;title&gt;"),
so switching commentary in Jellyfin is just picking a different audio
track/channel — no ffmpeg restart. The dashboard's **Set Audio** control
does the same thing without leaving the grid. (The older
`/multiview/{id}/audio/N.m3u8` form is a deprecated alias kept for
compatibility — see the [changelog](../CHANGELOG.md).)

Members that are slow to warm up, or later removed from the grid, are
backfilled from a placeholder ("No Signal") feed instead of stalling the
whole composite.

### Getting ffmpeg with hardware encoding

1. Download a build with NVENC/QSV support from
   [gyan.dev](https://www.gyan.dev/ffmpeg/builds/) ("essentials", a static
   single-file `ffmpeg.exe`) or [BtbN builds](https://github.com/BtbN/FFmpeg-Builds/releases).
2. Either add its `bin` folder to `PATH`, or set `FFMPEG_PATH` to the full
   path of `ffmpeg.exe`. Restart Jellyball.
3. Verify: `ffmpeg -encoders | findstr "nvenc qsv"`.
4. If Jellyfin is on the same machine with its own ffmpeg, you likely don't
   need to do this at all — Jellyball will find and prefer it.

Without hardware acceleration, set `MULTIVIEW_HWACCEL=none`; expect
significantly higher CPU usage per composited grid.

---

## Dashboard tour

- **Channels & Streams** — catalog selection,
  Multi-View creation, per-channel status/override/rescrape.
- **Stability Metrics** — failover/uptime history per channel.
- **Performance** — cache hit rates and provider timing.
- **Settings** — **Advanced Settings** (live-editable tunables, grouped by
  area, each showing its default and a "changed" badge when overridden),
  **Off-season Channels** toggle (keep a team's channel listed between
  seasons with an "Off-season (resumes &lt;date&gt;)" guide entry, instead
  of removing it), and the opt-in **update check** (polls GitHub Releases
  twice a day; shows a banner when a newer release exists; also reported at
  `/api/version`).
- **Alerts & Integrations** — Discord/Telegram webhook config and the
  **Provider Domains** card (`POST /settings/providers`) to change an aggregator's
  base URL live without restarting Jellyball or editing `.env`.
- **Logs** — tails `jellyball.log`.

Machine-readable endpoints (behind dashboard auth, except `/healthz` and FastAPI's own `/docs`, `/redoc` and `/openapi.json`, which are currently unauthenticated and only describe the API surface):

| Endpoint | Purpose |
| :--- | :--- |
| `/healthz` | Unauthenticated liveness check (used by Docker's `HEALTHCHECK`) |
| `/api/status` | Per-channel watching/on_placeholder/failover_count summary |
| `/api/sessions` | Per-channel bitrate, segment latency (avg/p95), edge age, uptime, provider, codec, failover history |
| `/metrics` | The same numbers in Prometheus text format |
| `/api/ffmpeg-status` | Multi-View process status, recent log lines, last error |
| `/api/version` | Current version and whether an update is available |
| `/api/logs` | Recent log lines |

---

## Security model

- **Auth by bind address.** A loopback bind (`127.0.0.1`) can run without a
  dashboard password. Anything else — Docker's `0.0.0.0`, a LAN address —
  requires one: if `DASHBOARD_PASSWORD` isn't set, Jellyball generates a
  random one on first start and saves it to `dashboard-password.txt` in the
  data directory (not printed to logs — read the file, or set
  `DASHBOARD_PASSWORD` yourself).
  Even in open (loopback, no password) mode, requests must present a
  loopback `Host` header, closing a DNS-rebinding path to the dashboard.
- **CSRF check.** A pure-ASGI middleware rejects cross-site
  POST/PUT/PATCH/DELETE requests by checking the browser's
  `Origin`/`Referer` against the request's `Host`. `X-Forwarded-Host` is
  ignored unless `TRUST_X_FORWARDED_HOST=1` (only enable behind a reverse
  proxy that overwrites that header). Non-browser clients (curl, scripts)
  send neither Origin nor Referer and aren't affected.
- **Open playback surface.** Dashboard auth does **not** cover
  `/playlist.m3u`, `/epg.xml`, `/stream/*`, `/multiview/*`, or the HMAC-signed
  legacy relay routes. Anyone who can reach the listen port can enumerate
  channels and watch streams. Put Jellyball behind a firewall, VPN, or TLS
  reverse proxy on network binds; do not expose the port to the public
  internet.
- **Signed relay URLs.** The legacy `/chunk`, `/resource`, and
  `/substream.m3u8` routes (used for fMP4/demuxed-audio/SAMPLE-AES sources
  that can't go through the main session engine) only serve HMAC-signed
  URLs that Jellyball itself wrote when rewriting a playlist — they are not
  an open fetch relay for arbitrary URLs.
- **Generated secrets** (`dashboard-password.txt`, `relay-signing.key`) are
  written with mode `0600` on platforms that support it. The generated
  dashboard password is not printed to the log; read it from the file (or
  set `DASHBOARD_PASSWORD` yourself).
- **Failed-login lockout** per client, masked secret inputs (a blank field
  keeps the existing value; a checkbox is required to clear one), and
  re-entering the Jellyfin API key whenever you change the Jellyfin host.
- **SSRF guards** on outbound scraping/probing: private/loopback/link-local
  targets are rejected (including through redirects), unless
  `ALLOW_PRIVATE_UPSTREAMS=1` (local testing only).

---

## Configuration

Precedence (highest wins): real environment variables > the data
directory's `.env` (per-user tray app, or `%ProgramData%\Jellyball\.env` for
the service) > a `.env` beside the executable/source. See
`Jellyball/.env.example` for a copy-pasteable template with every variable
below, commented out at its default.

"Adv" = also editable live from the dashboard's **Advanced Settings**
(Settings tab); a saved override there takes effect immediately.
"Domains" = editable live from the **Provider Domains** card
(Alerts & Integrations tab).

### Server & auth

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8000` | Web server port |
| `JELLYBALL_HOST` | `127.0.0.1` (Windows) / `0.0.0.0` (else) | Bind address |
| `JELLYBALL_DATA_DIR` | *(platform default)* | Override the writable data directory |
| `DB_FILE` | `sports_proxy.db` | SQLite database path (relative paths live in the data dir) |
| `DASHBOARD_USERNAME` | `admin` | Dashboard sign-in username |
| `DASHBOARD_PASSWORD` | *(empty → generated on a network bind)* | Dashboard sign-in password |
| `WEB_CONCURRENCY` | `1` | Must stay `1`; refuses to start otherwise |
| `PORT_BIND_WAIT_SECONDS` | `30` | How long to wait for the port to free up at startup |
| `JELLYBALL_HEADLESS` | off (Windows) / always on (else) | Run without the tray/GUI |
| `STREAM_PROVIDER_PRIORITY` | *(empty)* | Comma-separated provider names to break candidate ties, e.g. `iSportSurge,MyBuffStreams` |
| `JELLYBALL_USER_AGENT` | *(Chrome build matching the pinned Playwright)* | Override the scraping/probe User-Agent |
| `ALLOW_PRIVATE_UPSTREAMS` | `0` | Allow scraping/probing private or loopback addresses (testing only) |
| `TRUST_X_FORWARDED_HOST` | `0` | Also trust `X-Forwarded-Host` for the dashboard's CSRF Origin check (enable only behind a reverse proxy that overwrites that header) |
| `DNS_RESOLVE_TIMEOUT` | `2.0` | Timeout for a single DNS resolution during URL validation |

### Jellyfin

| Variable | Default | Description |
| :--- | :--- | :--- |
| `JELLYFIN_URL` | `http://localhost:8096` | Jellyfin base URL |
| `JELLYFIN_API_KEY` | *(empty)* | API key used to trigger Jellyfin's Refresh Guide task |
| `JELLYFIN_TASK_ID` | *(empty)* | "Refresh Guide Data" task ID (auto-detected if blank) |
| `JELLYFIN_AUTO_REFRESH_MIN_INTERVAL` | `600` | Minimum seconds between automatic guide-refresh triggers |

### Alerts

| Variable | Default | Description |
| :--- | :--- | :--- |
| `DISCORD_WEBHOOK_URL` | *(empty)* | Discord webhook for failover/alert notifications |
| `TELEGRAM_BOT_TOKEN` | *(empty)* | Telegram bot token |
| `TELEGRAM_CHAT_ID` | *(empty)* | Telegram chat ID |

Webhook delivery retries up to twice (honoring `Retry-After`, capped at
10s) on network errors, 5xx, or 429.

### Providers / scraping

| Variable | Default | Description | |
| :--- | :--- | :--- | :--- |
| `AGGREGATOR_1_URL` | `https://isportsurge.ws` | iSportSurge base URL | Domains |
| `AGGREGATOR_2_URL` | `https://mybuffstreams.plus` | MyBuffStreams base URL | Domains |
| `AGGREGATOR_3_URL` | `https://methstreams.click` | MethStreams base URL | Domains |
| `AGGREGATOR_4_URL` | `https://thestreameast.top` | StreamEast base URL | Domains |
| `AGGREGATOR_5_URL` | `https://thetvapp.st` | TheTVApp base URL (24/7 linear channels) | Domains |
| `AGGREGATOR_6_URL` | `https://dlhd.pk` | DaddyLive base URL (24/7 linear channels) | Domains |
| `AGGREGATOR_9_URL` | `https://footybite.im` | Footybite base URL | Domains |
| `AGGREGATOR_10_URL` | `https://1stream.ws` | 1Stream base URL | Domains |
| `AGGREGATOR_11_URL` | `https://streamed.su` | Streamed base URL | Domains |
| `AGGREGATOR_12_URL` | `https://topstreams.info` | TopStreams base URL | Domains |
| `EXTRA_NON_ENGLISH_MARKERS` | *(empty)* | Extra comma-separated channel-name substrings treated as non-English | |
| `IPTV_ORG_PLAYLIST_URL` | `https://iptv-org.github.io/iptv/categories/sports.m3u` | Fallback iptv-org sports playlist | |
| `IPTV_ORG_REFRESH_SECONDS` | `21600` | Refresh interval for the iptv-org playlist | |
| `CATALOG_REFRESH_SECONDS` | `3600` | ESPN college-directory refresh interval | |
| `CATALOG_FAILURE_RETRY_SECONDS` | `120` | Retry delay after a failed catalog refresh | |
| `MAX_PROVIDER_EVENTS` | `60` | Max schedule entries returned per provider search | |
| `PROVIDER_EVENT_CONCURRENCY` | `6` | Concurrent per-event fetches within a search | |
| `PROVIDER_EVENT_TIMEOUT` | `20` | Per-event fetch timeout (s) | |
| `PROVIDER_SEARCH_CONCURRENCY` | `6` | Concurrent provider searches | |
| `PROVIDER_SEARCH_TIMEOUT` | `45` | Provider search timeout (s) | |
| `PER_TEAM_PROVIDER_CONCURRENCY` | `3` | Concurrent providers searched per team | |
| `PROVIDER_TIMEOUT_MIN` | `20` | Floor for the dynamically learned provider timeout (s) | Adv |
| `PROVIDER_TIMEOUT_MAX` | `90` | Ceiling for the dynamically learned provider timeout (s) | Adv |
| `PROVIDER_TIMEOUT_DEFAULT` | `45` | Starting provider timeout before enough data is learned (s) | |
| `PROVIDER_BREAKER_FAILURES` | `3` | Consecutive failures before a provider's breaker opens | Adv |
| `PROVIDER_BREAKER_COOLDOWN` | `120` | Breaker cooldown (s) | Adv |
| `MAX_STREAM_CANDIDATES` | `24` | Max stream candidates kept per channel | Adv |
| `VERIFY_STREAM_CONCURRENCY` | `6` | Concurrent stream-liveness verifications | |
| `SCRAPE_INDEX_CACHE_SECONDS` | `60` | TTL for cached/single-flighted aggregator index pages | |
| `PLAYWRIGHT_RECYCLE_HOURS` | `6` | Recycle the shared headless Chromium after this long (0 disables) | |
| `PLAYWRIGHT_MAX_PAGES` | `3` | Max concurrent Playwright pages across all scrapers | |

### Health & failover

| Variable | Default | Description | |
| :--- | :--- | :--- | :--- |
| `ACTIVE_HEALTH_INTERVAL` | `3` | Health check interval for a watched channel's active source (s) | |
| `IDLE_HEALTH_INTERVAL` | `30` | Health check interval for an unwatched channel's active source (s) | Adv |
| `STANDBY_HEALTH_INTERVAL` | `45` | Standby check interval, watched channels (s) | Adv |
| `UNWATCHED_STANDBY_INTERVAL` | `300` | Standby check interval, unwatched channels (s) | Adv |
| `STANDBY_HEALTH_CONCURRENCY` | `4` | Concurrent standby health checks | |
| `ACTIVE_HEALTH_CONCURRENCY` | `8` | Concurrent active-source health checks | |
| `STANDBY_PROBES_PER_CHANNEL` | `5` | Max standbys probed per channel per cycle | |
| `HEALTH_FAILURE_THRESHOLD` | `2` | Failed probes before failover | Adv |
| `HEALTH_PROBE_TIMEOUT` | `12` | Probe timeout (s) | Adv |
| `EXHAUSTED_PROBE_INTERVAL` | `20` | Recovery probe interval once every source has failed | Adv |
| `EMERGENCY_SCRAPE_COOLDOWN` | `60` | Minimum seconds between emergency rescans | Adv |
| `HEALTHY_RESCRAPE_SECONDS` | `1800` | Refresh standbys of an already-healthy channel this often | Adv |
| `STARTUP_SCRAPE_SPREAD_SECONDS` | `45` | Spread startup rescrapes across this many seconds | |
| `WINDOW_CLOSE_GRACE_SECONDS` | `180` | Keep a finished game on-air without playback for this long (s) | Adv |
| `SELF_RETRY_LIMIT` | `2` | Retries of a channel's only source before "No Signal" | Adv |
| `SELF_RETRY_WINDOW` | `120` | Rolling window those retries are counted over (s) | |
| `INCOMPATIBLE_RETRY_SECONDS` | `3600` | How long a session-incompatible source is skipped before retry | |
| `FAILOVER_ALERT_COOLDOWN` | `300` | Minimum seconds between failover alerts per channel | Adv |
| `TOKEN_REFRESH_COOLDOWN` | `120` | Minimum seconds between auth-token refreshes on 401/403 | |
| `CANDIDATE_GOOD_FOR_SECONDS` | `180` | How long a candidate is considered known-good | |
| `CANDIDATE_FAILED_RETRY_AFTER` | `60` | How long a failed candidate is skipped before retry | |

### Channel sessions

| Variable | Default | Description | |
| :--- | :--- | :--- | :--- |
| `SESSION_IDLE_SECONDS` | `60` | Stop a channel's session after this many idle seconds | Adv |
| `STREAM_STARTUP_TIMEOUT` | `20` | Wait this long for a channel to start before "No Signal" (s) | Adv |
| `STARTUP_PLACEHOLDER_SECONDS` | `30` | How long "No Signal" is shown for a slow start (s) | Adv |
| `STREAM_MAX_BANDWIDTH` | `0` (unlimited) | Per-session bandwidth cap, bits/second (picks the highest HLS variant at or below it, else the lowest) | |
| `SESSION_LIVE_EDGE_SEGMENTS` | `3` | Segments of buffer handed to a player at tune-in | Adv |
| `SESSION_WINDOW_SECONDS` | `30` | Length of the proxy-built playlist window (s) | Adv |
| `SESSION_STALE_SECONDS` | `15` | Fail over when no new segment arrives for at least this long (s) | Adv |
| `SESSION_FAIL_THRESHOLD` | `3` | Consecutive fetch failures before failover | Adv |
| `SESSION_SEGMENT_TIMEOUT` | `15` | Per-segment download timeout (s) | Adv |

### Multi-View

| Variable | Default | Description |
| :--- | :--- | :--- |
| `FFMPEG_PATH` | *(auto-detected)* | ffmpeg binary path |
| `MULTIVIEW_FFMPEG_PATH` | *(empty)* | ffmpeg override specifically for Multi-View (checked before `FFMPEG_PATH`) |
| `MULTIVIEW_HWACCEL` | `nvenc` | `nvenc`, `qsv`, or `none` |
| `MULTIVIEW_BITRATE` | `6M` | Target video bitrate |
| `MULTIVIEW_SEGMENT_SECONDS` | `4` | HLS segment duration |
| `MULTIVIEW_HLS_LIST_SIZE` | `8` | HLS playlist length (segment count) |
| `MULTIVIEW_FPS` | `30` | Output frame rate |
| `MULTIVIEW_NVENC_PRESET` | `p4` | NVENC preset (unknown values are ignored and logged) |
| `MULTIVIEW_NVENC_TUNE` | `ll` | NVENC tune |
| `NVENC_FALLBACK_SECONDS` | `600` | Stay on software encoding this long after an NVENC failure |
| `MULTIVIEW_IDLE_TIMEOUT_SECONDS` | `180` | Stop an unwatched Multi-View after this long (s) | Adv |
| `MULTIVIEW_IDLE_CHECK_INTERVAL` | `30` | How often the idle monitor scans running processes |
| `MULTIVIEW_STARTUP_TIMEOUT_SECONDS` | `30` | Max wait for the first segment before failing the request |
| `MULTIVIEW_MEMBER_WARM_TIMEOUT` | `20` | Per-member warm-up timeout |
| `MAX_CONCURRENT_MULTIVIEW` | `3` | Max Multi-View grids running at once |
| `MULTIVIEW_AUDIO_CHANNELS` | `1` (on) | Expose per-member audio as separate Jellyfin channels |
| `MULTIVIEW_BACKOFF_BASE_SECONDS` | `15` | Spawn-failure retry backoff base |
| `MULTIVIEW_BACKOFF_MAX_SECONDS` | `300` | Spawn-failure retry backoff cap |
| `MULTIVIEW_RESTART_WINDOW_SECONDS` | `600` | Watchdog restart-burst rolling window |
| `MULTIVIEW_RESTART_BURST` | `2` | Immediate restarts allowed per window before backing off |
| `MULTIVIEW_REFUSAL_BACKOFF_SECONDS` | `5` | Backoff after a refused start (cap reached / no ffmpeg) |
| `MULTIVIEW_WATCHDOG_INTERVAL` | `3` | Stall-watchdog scan interval |
| `MULTIVIEW_SWEEP_INTERVAL` | `600` | How often leftover run directories are swept from disk |
| `MULTIVIEW_FFMPEG_LOGLEVEL` | `warning` | ffmpeg log verbosity (`info` enables `-stats`) |
| `PLACEHOLDER_IDLE_SECONDS` | `900` | Idle timeout for the placeholder ffmpeg process |

`Adv` above marks Multi-View settings also present in Advanced Settings;
the rest are env-only (would be unusual to change without a restart).

### Legacy proxy (fMP4 / demuxed-audio / SAMPLE-AES sources)

Used only for sources whose container/codec can't go through the main
session engine (`/chunk`, `/resource`, `/substream.m3u8`, all
HMAC-signed — see [Security model](#security-model)).

| Variable | Default | Description |
| :--- | :--- | :--- |
| `STREAM_STARTUP_BUFFER_SECONDS` | `15` | Warm-up cache time before serving the first chunk |
| `PREFETCH_CHUNK_COUNT` | `5` | Read-ahead chunk count |
| `PREFETCH_CONCURRENCY` | `2` | Concurrent read-ahead downloads |
| `STREAM_CHUNK_CACHE_CAPACITY` | `60` | Max chunks held in the short-lived proxy cache |
| `STREAM_CHUNK_CACHE_TTL` | `15` | Seconds a completed chunk stays cached |
| `STREAM_CHUNK_CACHE_MAX_BYTES` | `134217728` (128 MiB) | Total cache byte cap |
| `MAX_MANIFEST_BYTES` | `2097152` (2 MiB) | Max fetched manifest size |
| `MAX_RESOURCE_BYTES` | `8388608` (8 MiB) | Max proxied generic resource size |
| `MAX_CACHEABLE_CHUNK_BYTES` | `4194304` (4 MiB) | Largest chunk still eligible for caching |
| `MAX_UPSTREAM_REDIRECTS` | `3` | Max redirects the legacy proxy follows upstream |

All five are also editable in Advanced Settings ("Legacy proxy" group).

### Update check

| Variable | Default | Description |
| :--- | :--- | :--- |
| `UPDATE_CHECK_URL` | `https://api.github.com/repos/DarthBitBeard/Jellyball/releases/latest` | Where the opt-in update checker looks (toggle is in the dashboard's Settings tab, off by default) |

### Paths & runtime

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PLAYWRIGHT_BROWSERS_PATH` | *(set automatically)* | Playwright's Chromium cache location; override only if you know you need to |

### Backups

Everything Jellyball cannot rebuild lives in one place: the data directory
(`%LOCALAPPDATA%\Jellyball` on Windows, `~/.local/share/Jellyball` on Linux,
`/app/data` in Docker, or `$JELLYBALL_DATA_DIR` if set). That folder holds
`sports_proxy.db` (channels, schedules, settings, provider history),
`dashboard-password.txt`, `relay-signing.key`, `.env`, and `jellyball.log`.
Back up that single directory and you can rebuild from scratch; lose it and
you start over. Copy it while Jellyball is stopped (or copy the live
`sports_proxy.db` with SQLite's `.backup`, which is safe on a running
database), and keep a few dated copies somewhere other than the same disk.
Restore is the reverse: stop Jellyball, put the directory back, start it.

---

## Troubleshooting

- **Logs**: `jellyball.log` in the data directory (rotates at 10 MB × 5
  files). The dashboard's **Logs** tab tails it; `/api/logs` returns it as
  JSON.
- **Diagnosing the Windows service**: stop the `Jellyball` service, then run
  `JellyballConsole.exe --console` from an administrator console for
  foreground logs. Don't run both at once — they'll fight over the port and
  data directory.
- **NVENC driver/API mismatch**: the bundled ffmpeg may need a newer NVENC
  API than your installed NVIDIA driver supports, causing Multi-View to
  silently fall back to software encoding. If Jellyfin is installed on the
  same machine, Jellyball prefers its ffmpeg automatically (see
  [Multi-View](#multi-view)); otherwise update your NVIDIA driver, or set
  `MULTIVIEW_HWACCEL=none` to accept CPU encoding.
- **Port already in use**: the installer's service and a manually-run
  `JellyballConsole.exe --console` both bind the configured `PORT` — stop
  one before starting the other. Jellyball refuses to silently move to a
  different port (this used to break Jellyfin tuner URLs); it logs an error
  and exits instead.
- **"No ffmpeg found"**: Multi-View shows a dashboard warning. See
  [Getting ffmpeg with hardware encoding](#getting-ffmpeg-with-hardware-encoding).
- **A provider's site moved / stopped resolving**: update its base URL from
  the dashboard's **Provider Domains** card (Alerts & Integrations tab) —
  no restart needed — or set the corresponding `AGGREGATOR_N_URL`.
- **Locked out of the dashboard**: check `dashboard-password.txt` in the
  data directory for a generated password, or set `DASHBOARD_PASSWORD`
  yourself and restart.
- **Rolling back an upgrade**: before the first start after an update that
  changes the database layout, Jellyball copies the database next to itself
  as `sports_proxy.db.bak-<version>` (the newest three copies are kept). To
  go back, stop Jellyball, install the older version, delete
  `sports_proxy.db-wal` and `sports_proxy.db-shm` if they exist, and replace
  `sports_proxy.db` with the copy. Stale `-wal`/`-shm` files left next to a
  restored database can corrupt it.

---

## Architecture

```
scrapers (event-directory + linear-channel providers)
        │  candidates (ranked by match quality, provider priority, HLS quality)
        ▼
   failover engine  ──▶  channel session (owns the HLS timeline)
        │                        │
        │                        ▼
        │                 TS normalizer (PIDs, PAT/PMT, PTS/DTS/PCR per epoch)
        │                        │
        │                        ▼
        │                 /stream/{id}.m3u8 + /stream/{id}/seg/{seq}.ts
        ▼
  M3U (/playlist.m3u) / XMLTV (/epg.xml)
```

Sources that can't go through the session engine (fMP4, demuxed audio,
SAMPLE-AES) fall back to a legacy passthrough proxy
(`/chunk`, `/resource`, `/substream.m3u8`, HMAC-signed URLs only).

Multi-View sits alongside this: it warms up the member channels' own
sessions as ffmpeg inputs, encodes one shared video track plus one audio
track per member via ffmpeg's `tee` muxer, and serves each output the same
way as an ordinary channel session.

---

## Building from source and running tests

### Building the Windows installer

```powershell
cd Jellyball
.\build-installer.ps1
```

This creates a build-only virtual environment (`.venv-build`), installs
`requirements-build.txt` (runtime deps + PyInstaller + pywin32), installs
Playwright's Chromium headless shell, runs PyInstaller against
`jellyball.spec` (producing a **ONEDIR** build at `dist\Jellyball\` with
`Jellyball.exe` and `JellyballConsole.exe` sharing one `_internal`
payload), then compiles `installer\jellyball.iss` with Inno Setup 6's
`ISCC.exe` into `installer\Output\JellyballSetup-<version>.exe`.

To bundle a portable `ffmpeg.exe` into the build (so the installed app
works without a separate ffmpeg install): download a build with
NVENC/QSV support and either place it at
`%LOCALAPPDATA%\ffmpeg\bin\ffmpeg.exe`, or set `FFMPEG_BUNDLE_PATH` before
running the script.

**Code signing** (optional, avoids the SmartScreen "unrecognized app"
warning): set one of

- `SIGN_CERT_THUMBPRINT` — SHA1 thumbprint of a code-signing certificate in
  the current user's or machine's certificate store, or
- `AZURE_SIGNING_DLIB` + `AZURE_SIGNING_METADATA` — Azure Trusted Signing,
  pointing at `Azure.CodeSigning.Dlib.dll` and its `metadata.json`,

plus optionally `SIGN_TIMESTAMP_URL` (defaults to
`http://timestamp.acs.microsoft.com`). Unset, the build is unsigned and
otherwise unaffected. Requires `signtool.exe` (Windows SDK) on `PATH` or
under `Windows Kits\10\bin`.

### Tests

From `Jellyball/`:

```powershell
# Unit suite (no ffmpeg or installed Playwright browsers required)
python -m unittest discover -p "test_*.py"

# End-to-end (fake HLS origins + real ffmpeg subprocesses; requires ffmpeg)
$env:JELLYBALL_E2E = "1"
python -m unittest test_e2e_tools -v
Remove-Item Env:\JELLYBALL_E2E

# Soak test (30 minutes, 3 channels; watches for consumer restarts, lag
# growth, RSS growth, session-memory growth)
python tools/soak.py --minutes 30 --channels 3
```

CI (`.github/workflows/ci.yml`) runs the unit suite and `ruff` lint on both
`windows-latest` and `ubuntu-latest` for every push/PR to `master` or
`release-*`. `.github/workflows/release.yml` builds (and, if secrets are
configured, signs) the installer on a `v*` tag push and attaches it to the
GitHub release. See [`docs/RELEASE_CHECKLIST.md`](../docs/RELEASE_CHECKLIST.md)
for the full release process.

---

## Proactive monitoring & webhooks

When `DISCORD_WEBHOOK_URL` and/or `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`
are set, Jellyball sends:

- **Stream Failover** — an active candidate failed its health check and
  switched to a backup.
- **All Candidates Exhausted** — every scraped source for a channel went
  offline; an emergency rescrape starts immediately.
- **Stream Available** — fresh live streams were found and brought online.
- **Multi-View Unstable** — a Multi-View grid is restarting repeatedly.

Alerts are rate-limited per channel (`FAILOVER_ALERT_COOLDOWN`).

---

## More documentation

| Guide | What it covers |
| :--- | :--- |
| [`docs/DOCKER.md`](../docs/DOCKER.md) | Image and tags, GPUs, reverse proxy and TLS |
| [`deploy/linux/README.md`](../deploy/linux/README.md) | systemd service without Docker |
| [`docs/JELLYFIN.md`](../docs/JELLYFIN.md) | Jellyfin setup, Jellyfin 12 notes, client compatibility |
| [`docs/PROVIDERS.md`](../docs/PROVIDERS.md) | How providers work and what to do when one breaks |
| [`docs/TROUBLESHOOTING.md`](../docs/TROUBLESHOOTING.md) | Symptoms and fixes |
| [`docs/UPGRADING.md`](../docs/UPGRADING.md) | Upgrading, backups and rollback |
| [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) | How the code fits together |

Unattended Windows installs accept `/PORT=`, `/USER=`, `/LAN=1|0` and
`/DATADIR=` (see the header of `Jellyball/installer/jellyball.iss`).

---

For what changed in this release, see [`CHANGELOG.md`](../CHANGELOG.md).
