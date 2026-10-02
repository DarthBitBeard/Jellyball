# Security policy

## Supported versions

Security fixes go into the latest release (the 2.x line). Older minor versions
are not patched.

## Reporting a vulnerability

Use GitHub private vulnerability reporting: open the **Security** tab of this
repository and choose **Report a vulnerability**. Please do not open a public
issue with details.

If that option is not available, open a public issue titled
"Security contact request" that contains **no details**, and the maintainer
(@DarthBitBeard) will arrange a private channel.

Jellyball is a hobby project, so reports are handled on a best-effort basis.
The aim is to acknowledge a report within about a week.

## What is in scope

- Dashboard authentication or CSRF bypass.
- Bypass of the SSRF guards on outbound requests.
- Forging the HMAC-signed relay URLs (`/chunk`, `/resource`,
  `/substream.m3u8`).
- Secrets (API keys, webhook URLs, passwords) exposed in logs, pages or API
  responses.
- Path traversal or injection in the M3U/XMLTV output or the dashboard.

## What is not in scope

- The playback endpoints (`/playlist.m3u`, `/epg.xml`, `/stream/*`,
  `/multiview/*`) being reachable without a password. This is by design,
  because Jellyfin's ffmpeg fetches them, and the README documents it (see the
  [security model](Jellyball/README.md#security-model)).
- Content served by third-party stream sites.
- Attacks that need administrator access to the machine running Jellyball.
- Denial of service by someone who already has network access.
- Vulnerabilities in Jellyfin itself.

## Running Jellyball safely

- Bind to loopback or a trusted LAN.
- Keep it behind a firewall, a VPN or a TLS reverse proxy. Do not expose the
  port to the internet.
- A dashboard password is required on any non-loopback bind. Set
  `DASHBOARD_PASSWORD`, or use the generated one in `dashboard-password.txt`.
- Redact passwords, webhook URLs and API keys before sharing logs.
