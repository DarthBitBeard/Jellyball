# Real Jellyfin responses

Sanitised samples recorded from a real **Jellyfin 12.1.0** (`jellyfin/jellyfin:12.1`,
official Docker image) by `python tools/jellyfin_harness.py probe --out fixtures/jellyfin`.
API keys, tokens, the password, server/user ids and the container's address are replaced
by `<PLACEHOLDERS>`; object ids Jellyfin generates (tuners, channels, programmes) are left
as observed and change on every run. Re-run the command against another version
(`--tag 10.11`) to see what changed.

| File | What it shows |
| --- | --- |
| `Startup.wizard.json` | The setup wizard exchange: bodies and status codes |
| `Auth.matrix.json` | Which credential styles a real server accepts, `GET /ScheduledTasks` |
| `Auth.Keys.json` | `GET /Auth/Keys` after `POST /Auth/Keys?app=...` |
| `System.Info.Public.json`, `System.Info.json` | Version and server details |
| `System.Configuration.encoding.json` | ffmpeg path: `EncoderAppPathDisplay` |
| `ScheduledTasks.json`, `ScheduledTasks.RefreshGuide*.json` | Task list; the RefreshGuide entry before and after a run |
| `LiveTv.TunerHosts.*.json` | Tuner types, M3U tuner request and stored response |
| `LiveTv.ListingProviders.add.*.json` | XMLTV provider request and stored response |
| `System.Configuration.livetv*.json` | Where configured tuners and providers are listed |
| `LiveTv.Info.json`, `LiveTv.Channels*.json`, `LiveTv.Programs.first3.json` | Live TV state before and after a guide refresh |
| `LiveTv.errors.json` | Status and body for malformed or unreachable registrations |

## What a real Jellyfin 12.1.0 does

- **Auth.** Only `Authorization: MediaBrowser Token="<key>"` works for an API key (also with
  Client/Device/DeviceId/Version fields, or an unquoted token) and `?ApiKey=`. `X-Emby-Token`,
  `X-MediaBrowser-Token`, `X-Emby-Authorization`, `?api_key=` and `Bearer` are ignored and
  answered 401. A wrong key is 401 in every style.
- **Startup.** `/System/Info/Public` first answers 200 with camelCase keys from a temporary
  host, drops the connection, answers 503 "loading", and only then gives the real
  PascalCase 200. Wait for `Version` to appear.
- **RefreshGuide** exists on a fresh server with no Live TV (hidden, Idle, daily). Its Id is
  the same on every install. `POST /ScheduledTasks/Running/<id>` is 204, also when already
  running; an unknown id is 404. A run reports `Completed` even if the playlist fetch failed.
- **Encoding.** `EncoderAppPathDisplay` is the ffmpeg in use (`/usr/lib/jellyfin-ffmpeg/ffmpeg`
  in the image); `EncoderAppPath` is absent unless a user set one.
- **Tuner** (`POST /LiveTv/TunerHosts`): `{Type:"m3u", Url, FriendlyName}`. Jellyfin fetches
  the playlist at once (`User-Agent: Jellyfin-Server/<version>`); an unreachable or 404 URL is
  a bare-text 500, a missing or unknown `Type` is a 404. There is no list endpoint: read
  `TunerHosts` from `GET /System/Configuration/livetv`. Without `Id` a POST always adds;
  with the stored `Id` it updates. `DELETE ...?id=` is 204 even for unknown ids.
- **Listing** (`POST /LiveTv/ListingProviders`): `{Type:"xmltv", Path}`; `EnableAllTuners`
  defaults to true. The URL is not checked until a refresh. Same list, update and delete rules.
- **Channels** stay empty until RefreshGuide runs, and stay listed after deletion until the
  next refresh.
