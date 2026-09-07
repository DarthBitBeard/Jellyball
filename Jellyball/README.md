# Jellyball - Jellyfin Sports Proxy Engine

A high-performance live sports stream aggregator and Jellyfin Live TV gateway. It tracks games across multiple live scrapers, proxies HLS streams with clean MPEG-TS chunk extensions to prevent FFmpeg playback failures, dynamically generates XMLTV EPG schedule data, logs stream stability metrics into SQLite, alerts you via Discord/Telegram webhooks, and enables out-of-the-box Jellyfin DVR recording.

---

## Key Features

- **Multi-Aggregator Failover**: Continuously monitors stream health and automatically fails over to candidate backups without interrupting your channel list.
- **FFmpeg-Compliant Chunk Proxy (`/chunk.ts`)**: Proxies HLS chunks using `.ts` route extensions with `video/mp2t` MIME types, eliminating FFmpeg "fatal player error" crashes caused by extensionless query routes.
- **Buffered Chunk Recovery**: Keeps a bounded short-lived chunk cache and retries transient upstream failures without caching live manifests.
- **Identity-Aware Team Search**: Rejects near-name collisions such as Eastern Michigan for Michigan State, Rays for Buccaneers, and Mets for Jets while retaining known aliases.
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
| `DASHBOARD_USERNAME` | `admin` | Username for dashboard management access |
| `DASHBOARD_PASSWORD` | *(empty)* | Password for dashboard. If left blank, auth is disabled. |
| `DISCORD_WEBHOOK_URL` | *(empty)* | Discord webhook URL for failover & alert notifications |
| `TELEGRAM_BOT_TOKEN` | *(empty)* | Telegram Bot Token from `@BotFather` |
| `TELEGRAM_CHAT_ID` | *(empty)* | Telegram Chat ID to receive alerts |
| `AGGREGATOR_1_URL` | `https://isportsurge.ws` | Scraper endpoint 1 |
| `AGGREGATOR_2_URL` | `https://mybuffstreams.plus` | Scraper endpoint 2 |
| `STREAM_PROVIDER_PRIORITY` | *(empty)* | Optional comma-separated provider order used to break equal-quality stream ties, for example `iSportSurge,MyBuffStreams` |
| `STREAM_CHUNK_CACHE_CAPACITY` | `300` | Maximum number of completed chunks held in the short-lived proxy cache |
| `STREAM_CHUNK_CACHE_TTL` | `15` | Seconds completed chunks remain cached |
| `ACTIVE_HEALTH_INTERVAL` | `3` | Seconds between checks of each active stream |
| `STANDBY_HEALTH_INTERVAL` | `45` | Seconds between checks of standby candidates |
| `STANDBY_HEALTH_CONCURRENCY` | `4` | Maximum concurrent standby health checks |
| `EMERGENCY_SCRAPE_COOLDOWN` | `60` | Minimum seconds between emergency rescans for one team |
| `PREFETCH_CHUNK_COUNT` | `5` | Maximum upcoming numeric chunks to read ahead |
| `PREFETCH_CONCURRENCY` | `2` | Maximum concurrent read-ahead downloads |

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
python -m pip install -r requirements.txt pyinstaller playwright
python -m playwright install chromium
pyinstaller --clean --noconfirm jellyfin-sports-proxy.spec
```

You may place `.env` beside `jellyfin-sports-proxy.exe` for machine/package-level
defaults. Chromium is optional for basic HTTP extraction; if it is unavailable,
the executable still starts and uses HTTP extraction, while Chromium enables the
browser interception fallback. The executable does not depend on its current
working directory or write application state beside itself.

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

