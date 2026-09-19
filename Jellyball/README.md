# Jellyball - Jellyfin Sports Proxy Engine

A high-performance live sports stream aggregator and Jellyfin Live TV gateway. It tracks games across multiple live scrapers, proxies HLS streams with clean MPEG-TS chunk extensions to prevent FFmpeg playback failures, dynamically generates XMLTV EPG schedule data, logs stream stability metrics into SQLite, alerts you via Discord/Telegram webhooks, and enables out-of-the-box Jellyfin DVR recording.

---

## Key Features

- **Multi-Aggregator Failover**: Continuously monitors stream health and automatically fails over to candidate backups without interrupting your channel list.
- **FFmpeg-Compliant Chunk Proxy (`/chunk.ts`)**: Proxies HLS chunks using `.ts` route extensions with `video/mp2t` MIME types, eliminating FFmpeg "fatal player error" crashes caused by extensionless query routes.
- **Buffered Chunk Recovery**: Keeps a bounded short-lived chunk cache and retries transient upstream failures without caching live manifests.
- **Identity-Aware Team Search**: Rejects near-name collisions such as Eastern Michigan for Michigan State, Rays for Buccaneers, and Mets for Jets while retaining known aliases.
- **Catalog-Based Channel Selection**: Select exact NFL, MLB, NHL, NBA, college football, and college basketball identities from dashboard toggles instead of entering team names and search terms.
- **Always-Live Sports Channels**: Includes toggleable NFL RedZone, ESPN, ESPN2, ESPNU, FOX Sports 1/2, CBS Sports Network, TNT Sports, and NBC Sports entries.
- **Dynamic 4-Hour Rolling EPG (`/epg.xml`)**: Generates dynamic XMLTV program schedules starting in real-time (`now` to `+4 hours`) so channels never go blank or show expired dates.
- **Proactive Monitoring & Alerts**: Instant notifications to mobile devices via Discord or Telegram webhooks when streams fail over or exhaust candidates.
- **Stability Metrics in SQLite**: Automatically records failover events, scraper health, and uptime history to analyze which aggregator source delivers the most consistent feeds.
- **Dashboard Access Control**: Secure the management interface with HTTP Basic Authentication while keeping streaming endpoints open for local Jellyfin clients.
- **Windows System Tray**: The packaged application runs without a console window and provides View, Restart, and Quit actions from the tray icon.
- **Docker Containerization**: Ready-to-deploy `Dockerfile` and `docker-compose.yml` for seamless integration into Linux-based media servers alongside Jellyfin.
- **Full DVR Support**: Native integration with Jellyfin's Live TV DVR scheduler—click "Record" on any active guide event to record the live feed to your library.

---

## Quick Start

### Option A: Running with Docker Compose (Recommended for Media Servers)

1. Copy `.env.example` to `.env` and set your configuration:
   ```bash
   cp .env.example .env
   ```
2. Start the container in background:
   ```bash
   docker compose up -d
   ```
3. Open the web dashboard at: `http://<SERVER_IP>:8000`

On the **Channels & Streams** tab, expand the Sports Catalog categories you need,
select as many teams or special channels as desired, and click **Apply Selected
Teams** once. Categories are collapsed by default, and catalog entries are off
by default so the application does not create hundreds of provider searches
unless selected. Applying the catalog treats the checked entries as the desired
set: unchecked catalog entries that were previously enabled are stopped and
removed.
College football and men's college basketball entries are refreshed from ESPN
when available; the bundled catalog remains available when ESPN cannot be
reached. Existing manually configured trackers continue to work.

### Option B: Running Locally with Python

1. Ensure Python 3.10+ is installed, then install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the application:
   ```bash
   cd Jellyball
   python main.py
   ```

---

## Configuration (`.env`)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8000` | Web server port |
| `DB_FILE` | `sports_proxy.db` | Path to SQLite database file |
| `DISCORD_WEBHOOK_URL` | *(empty)* | Discord webhook URL for failover & alert notifications |
| `TELEGRAM_BOT_TOKEN` | *(empty)* | Telegram Bot Token from `@BotFather` |
| `TELEGRAM_CHAT_ID` | *(empty)* | Telegram Chat ID to receive alerts |
| `AGGREGATOR_1_URL` | `https://isportsurge.ws` | Scraper endpoint 1 |
| `AGGREGATOR_2_URL` | `https://mybuffstreams.plus` | Scraper endpoint 2 |
| `AGGREGATOR_3_URL` | `https://methstreams.click` | Scraper endpoint 3 |
| `AGGREGATOR_4_URL` | `https://thestreameast.top` | Scraper endpoint 4 |
| `AGGREGATOR_9_URL` | `https://footybite.im` | Scraper endpoint 9 |
| `AGGREGATOR_10_URL` | `https://1stream.ws` | Scraper endpoint 10 |
| `AGGREGATOR_11_URL` | `https://streamed.su` | Scraper endpoint 11 |
| `AGGREGATOR_12_URL` | `https://topstreams.info` | Scraper endpoint 12 |
| `STREAM_PROVIDER_PRIORITY` | *(empty)* | Optional comma-separated provider order used to break equal-quality stream ties, for example `iSportSurge,MyBuffStreams` |
| `STREAM_CHUNK_CACHE_CAPACITY` | `300` | Maximum number of completed chunks held in the short-lived proxy cache |
| `STREAM_CHUNK_CACHE_TTL` | `15` | Seconds completed chunks remain cached |
| `ACTIVE_HEALTH_INTERVAL` | `3` | Seconds between checks of each active stream |
| `STANDBY_HEALTH_INTERVAL` | `45` | Seconds between checks of standby candidates |
| `STANDBY_HEALTH_CONCURRENCY` | `4` | Maximum concurrent standby health checks |
| `EMERGENCY_SCRAPE_COOLDOWN` | `60` | Minimum seconds between emergency rescans for one team |
| `PREFETCH_CHUNK_COUNT` | `5` | Maximum upcoming numeric chunks to read ahead |
| `PREFETCH_CONCURRENCY` | `2` | Maximum concurrent read-ahead downloads |
| `CATALOG_REFRESH_SECONDS` | `3600` | Minimum time between ESPN college team-directory refreshes |

### Provider roles and diagnostics

The iSportSurge, MyBuffStreams, MethStreams, StreamEast, Footybite, 1Stream,
Streamed, and TopStreams integrations are event-directory providers. They expose
event links only while games are active or shortly before start time, so a search
for a team outside its game window can legitimately return no candidates. Footybite
and 1Stream are scanned at their base URLs because their former category paths
returned 404 responses.

24/7 channels such as ESPN, FS1, and NFL RedZone require linear-channel providers,
not event-directory searches. The project does not currently include TheTVApp or
DaddyLive adapters. To add those providers safely, supply an authorized endpoint
or playlist/API contract for each provider (including channel identifiers and
stream URL rules); do not assume event-provider paths are interchangeable with
linear-channel paths. `_diagnose_always_live.py` reports this missing registration
and tests ESPN only through providers named `TheTVApp` or `DaddyLive`; event
providers are tested with an active-event term instead.

The first candidate is selected using match quality, then the optional provider
priority, then HLS URL quality. On first launch, the Windows executable creates
these files under `%LOCALAPPDATA%\Jellyball` (or the next available writable
user-data location):

- `.env` - starter configuration template; existing settings are never overwritten
- `sports_proxy.db` - SQLite application database
- `jellyball.log` - persistent startup and runtime log

If `DB_FILE` is relative, it is stored in that same user-data directory, so the
application remains writable regardless of where the `.exe` is launched. An
absolute `DB_FILE` path is honored.

### Windows executable build

Build from the project directory with a clean PyInstaller run:

```powershell
.\build-self-contained.ps1
```

The script runs the following equivalent commands:

```powershell
python -m pip install -r requirements.txt pyinstaller playwright
python -m playwright install chromium
python -m PyInstaller --clean --noconfirm jellyfin-sports-proxy.spec
```

The resulting one-file executable is written to `dist\jellyfin-sports-proxy.exe`
and includes the application dependencies, runtime assets, and the installed
Playwright Chromium browser. The build requires Chromium to be installed locally
before PyInstaller runs. The build script stops on dependency, browser, or
PyInstaller failures and verifies that the executable was produced. At runtime,
the one-file bootloader extracts its bundled payload to a temporary directory;
this is normal and does not require files beside the executable. If the bundled
browser cannot launch, the application still starts and uses HTTP extraction.
You may place `.env` beside the executable for machine/package-level defaults;
writable settings and the SQLite database remain under the per-user Jellyball
data directory.

### Jellyfin channel metadata (Method 1)

The M3U playlist includes Jellyfin-compatible `tvg-id`, `tvg-name`, `tvg-logo`,
and `group-title` attributes. Always-live channels use stable IDs so they can
be mapped to an XMLTV provider without changing the local proxy URL:

| Channel | `tvg-id` | Group |
| :--- | :--- | :--- |
| NFL RedZone | `NFLRedZone.us` | 24/7 Sports |
| ESPN | `ESPN.us` | 24/7 Sports |
| ESPN2 | `ESPN2.us` | 24/7 Sports |
| ESPNU | `ESPNU.us` | 24/7 Sports |
| FOX Sports 1 | `FoxSports1.us` | 24/7 Sports |
| FOX Sports 2 | `FoxSports2.us` | 24/7 Sports |
| CBS Sports Network | `CBSSportsNetwork.us` | 24/7 Sports |
| TNT Sports | `TNT.us` | 24/7 Sports |
| NBC Sports | `NBCSports.us` | 24/7 Sports |

To use the metadata in Jellyfin:

1. Refresh the M3U tuner at `http://127.0.0.1:8000/playlist.m3u` (replace the
   host with the Jellyball machine's address when Jellyfin runs elsewhere).
2. Use Jellyball's `http://127.0.0.1:8000/epg.xml` as the XMLTV guide provider
   for the built-in live/standby guide, or add a current external XMLTV provider
   and map entries by the IDs above.
3. Refresh the tuner and guide after changing catalog selections. If Jellyfin
   retained old duplicate college entries, remove and re-add the tuner so it
   picks up the new metadata.

The IPTV-ORG `tvguide.com.epg.xml` URL from the original proposal currently
returns 404, so it should not be configured until a current guide URL is
available. Logos use public channel-logo assets when Jellyfin displays the
playlist; the channel still works if a logo host is temporarily unavailable.

College entries have separate IDs and labels for the same school in different
sports. For example, Michigan appears as `Michigan Wolverines (Football)` with
ID `ncaaf_130` and `Michigan Wolverines (Men's Basketball)` with ID `ncaam_130`.

### System tray controls

The Windows executable runs quietly in the system tray. Right-click the
Jellyball tray icon to:

- **View** - open the local web dashboard
- **Restart** - gracefully restart the proxy
- **Quit** - stop the proxy

The tray mode is included in the packaged executable. When running from Python
for development, install the complete `requirements.txt` first.

---

## Proactive Monitoring & Webhooks

When configured, Jellyball sends real-time alert notifications:
- **Stream Failover (Warning)**: Triggered when an active stream candidate fails health check and switches to a backup candidate from another aggregator.
- **All Candidates Exhausted (Danger)**: Triggered when all scraped feeds for a team go offline, initiating an immediate emergency scrape.
- **Stream Available (Success)**: Triggered when fresh live streams are successfully detected and brought online.

---

## Jellyfin Configuration Guide

### 1. Add M3U Tuner Device
1. Open Jellyfin Web Interface and navigate to **Dashboard** &rarr; **Live TV**.
2. Under **Tuner Devices**, click **+ (Add)**.
3. Select **M3U Tuner** as the Tuner Type.
4. Enter the File or URL:
   ```text
   http://<YOUR_SERVER_IP>:8000/playlist.m3u
   ```
5. Click **Save**.

### 2. Add XMLTV Guide Provider
1. Under the same **Live TV** settings page, scroll to **TV Guide Data Providers**.
2. Click **+ (Add)** and select **XMLTV**.
3. In the **File or URL** field, enter:
   ```text
   http://<YOUR_SERVER_IP>:8000/epg.xml
   ```
4. Click **Save**.
5. Click the three dots next to XMLTV and choose **Refresh Guide Data**. Tracked teams will now appear in Jellyfin's Live TV Guide.

### 3. Enforce Direct Stream Profiles (FFmpeg Hand-off)
To prevent FFmpeg from forcing unnecessary video transcoding wrappers on Live TV sources:
1. Go to **Dashboard** &rarr; **Playback**.
2. Under **Streaming**, ensure your server is allowed to stream media directly without transcoding live sources when clients support Direct Stream.

### 4. Record Games via DVR
Because Jellyfin treats the proxied channels as digital antenna tuners with dynamic XMLTV schedule windows:
1. Open **Live TV** &rarr; **Guide** in Jellyfin.
2. Select any active game or channel card.
3. Click **Record**. Jellyfin's internal scheduler will record the proxied stream directly into your designated TV recording library folder.

