# Jellyball - Jellyfin Sports Proxy Engine

A high-performance live sports stream aggregator and Jellyfin Live TV gateway. It tracks games across multiple live scrapers, proxies HLS streams with clean MPEG-TS chunk extensions to prevent FFmpeg playback failures, dynamically generates XMLTV EPG schedule data, logs stream stability metrics into SQLite, alerts you via Discord/Telegram webhooks, and enables out-of-the-box Jellyfin DVR recording.

---

## Key Features

- **Multi-Aggregator Failover**: Continuously monitors stream health and automatically fails over to candidate backups without interrupting your channel list.
- **FFmpeg-Compliant Chunk Proxy (`/chunk.ts`)**: Proxies HLS chunks using `.ts` route extensions with `video/mp2t` MIME types, eliminating FFmpeg "fatal player error" crashes caused by extensionless query routes.
- **Dynamic 4-Hour Rolling EPG (`/epg.xml`)**: Generates dynamic XMLTV program schedules starting in real-time (`now` to `+4 hours`) so channels never go blank or show expired dates.
- **Proactive Monitoring & Alerts**: Instant notifications to mobile devices via Discord or Telegram webhooks when streams fail over or exhaust candidates.
- **Stability Metrics in SQLite**: Automatically records failover events, scraper health, and uptime history to analyze which aggregator source delivers the most consistent feeds.
- **Dashboard Access Control**: Secure the management interface with HTTP Basic Authentication while keeping streaming endpoints open for local Jellyfin clients.
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

