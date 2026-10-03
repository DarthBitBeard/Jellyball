# Running Jellyball in Docker

## Quick start

```bash
git clone https://github.com/DarthBitBeard/Jellyball.git && cd Jellyball
cp Jellyball/.env.example .env      # optional; every line is commented out at its default
mkdir -p data && sudo chown 1000:1000 data
docker compose up -d
docker compose ps                    # STATUS shows (healthy) after ~20-30 s
```

Open `http://<server-ip>:8000/`. The container binds `0.0.0.0`, which requires a
dashboard password: if `DASHBOARD_PASSWORD` is empty, Jellyball generates one on
first start and stores it in `./data/dashboard-password.txt` (user `admin`
unless `DASHBOARD_USERNAME` is set). It is never printed to the log.

```bash
cat data/dashboard-password.txt
```

Then follow [JELLYFIN.md](JELLYFIN.md) to add the tuner and guide.

## Image, tags and the build fallback

`docker-compose.yml` uses the published image:

```yaml
image: ${JELLYBALL_IMAGE:-ghcr.io/darthbitbeard/jellyball:latest}
```

* Release tags build `ghcr.io/darthbitbeard/jellyball:X.Y.Z` (plus `X.Y` and
  `latest` for stable releases). A pre-release such as `2.1.0-beta.1` gets only
  its exact tag and never moves `latest`. For production pin an exact version:
  put `JELLYBALL_IMAGE=ghcr.io/darthbitbeard/jellyball:2.1.0` in `.env`.
* The image is **linux/amd64 only**. Release builds attach build provenance and
  an SBOM. Images are published by the release workflow starting with 2.1.0; for
  older versions use the build fallback below.
  arm64 and a CUDA variant are not published; see "Build it yourself".
* **Build it yourself** (a checkout with local changes, an arm64 host, or when
  no image has been published for the version you want):

  ```bash
  docker compose build        # builds ./Jellyball and tags it with the image name above
  docker compose up -d        # uses that local build
  ```

  Playwright publishes Chromium for amd64 and arm64 Linux, so an arm64 build
  is expected to work but is not tested by the project.

## Data, permissions, upgrades

* `./data` on the host is mounted at `/app/data` and holds the database,
  settings, logs, the generated password and the signing key. Back it up.
* The image runs as uid 1000 (`jellyball`, `no-new-privileges`). The host
  folder must be writable by that uid (`sudo chown -R 1000:1000 data`), or the
  container will fail to start.
* Every setting from the README configuration tables can go in `.env` next to
  `docker-compose.yml`; the compose file loads it with `env_file`. Container
  plumbing (`PORT=8000` inside the container, `JELLYBALL_DATA_DIR`,
  `JELLYBALL_HOST=0.0.0.0`) is set in the compose `environment:` block. To
  change the published port set `PORT` in `.env` (it maps host `PORT` to
  container 8000).
* Upgrade: see [UPGRADING.md](UPGRADING.md) (`docker compose pull && docker compose up -d`).

## Health

The image has a `HEALTHCHECK` and compose declares the same one: `curl` against
`http://localhost:8000/healthz`, which is unauthenticated and does not touch
the database. `docker compose ps` shows `(healthy)`.
`/healthz` is the only route that is useful without a password; `/metrics` and
`/api/*` need the dashboard credentials.

## Talking to Jellyfin

* Jellyfin in the same compose project or Docker network: use the service
  name, e.g. `http://jellyfin:8096` for `JELLYFIN_URL`, and
  `http://jellyball:8000/playlist.m3u` as the tuner address inside Jellyfin.
* Jellyfin on the host: use the host's LAN address, not `localhost` (inside the
  container that is the container itself).
* The playlist's stream links use the `Host` header of whoever fetched it, so
  fetch it through the address Jellyfin will keep using.

## Multi-View and GPUs

Multi-View composites channels with ffmpeg. The stock image installs Debian's
ffmpeg, which has **no NVENC support**, so the compose file defaults to
`MULTIVIEW_HWACCEL=none` (software `libx264`, higher CPU use). If you set
`nvenc` with the stock image, the first run fails over to software and retries
hardware every `NVENC_FALLBACK_SECONDS` (600), which only wastes time.

### NVIDIA (NVENC)

Not shipped or tested by the project (a CUDA image variant is a stretch goal);
this is the recipe that follows from how Jellyball finds and uses ffmpeg.

1. Install the NVIDIA driver and the
   [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/)
   on the host and confirm `docker run --rm --gpus all ubuntu nvidia-smi` works.
2. Provide an ffmpeg built with NVENC (for example jellyfin-ffmpeg or a static
   build) inside the container, by bind-mounting it or building a derived image,
   and point Jellyball at it with `FFMPEG_PATH`.
3. Give the container the GPU. In a `docker-compose.override.yml`:

   ```yaml
   services:
     jellyball:
       volumes:
         - /opt/ffmpeg-nvenc/ffmpeg:/usr/local/bin/ffmpeg-nvenc:ro
       environment:
         - NVIDIA_VISIBLE_DEVICES=all
         - NVIDIA_DRIVER_CAPABILITIES=compute,video,utility
       deploy:
         resources:
           reservations:
             devices:
               - driver: nvidia
                 count: 1
                 capabilities: [gpu]
   ```

4. In `.env`: `MULTIVIEW_HWACCEL=nvenc` and `FFMPEG_PATH=/usr/local/bin/ffmpeg-nvenc`.
5. Start a Multi-View and look at `/api/ffmpeg-status`; if it reports a
   fallback to software, check driver and ffmpeg versions (an ffmpeg built for a
   newer NVENC API than your driver supports fails this way).

Jellyball cannot use Jellyfin's ffmpeg across containers; it only looks for it
on the same machine.

### Intel Quick Sync and others

`MULTIVIEW_HWACCEL=qsv` needs an ffmpeg with QSV, the Intel media driver in
the image, and `devices: ["/dev/dri:/dev/dri"]` on the container. This is
unverified. VAAPI and AMD (AMF) encoders are not supported yet.

## Reverse proxy and TLS

Read this first: **dashboard authentication does not protect the playback
surface.** `/playlist.m3u`, `/epg.xml`, `/stream/*`, `/multiview/*` and the
relay routes answer anyone who can reach the port. Put Jellyball on a LAN or
behind a VPN. If you publish it through a proxy, restrict who can reach it
(source address allow-list, VPN, or a client-certificate or SSO layer in the
proxy) rather than exposing it to the internet. Jellyfin itself should keep
talking to Jellyball directly over the LAN, not through the public hostname.

What the proxy must get right:

* **Pass the original `Host` header** (nginx: `$http_host`, not `$host`, so a
  non-default port survives). Jellyball's CSRF check compares the browser's
  `Origin` with `Host`, and the playlist links are built from `Host`. If your
  proxy has to rewrite `Host`, set `TRUST_X_FORWARDED_HOST=1` and send
  `X-Forwarded-Host` with the public name, but only when the proxy overwrites
  that header.
* **Do not buffer streams**: turn off response buffering and allow long reads
  (live segments are served as they arrive).
* **Client address**: the login lockout is per client address. If the proxy
  does not pass the real client address, all users share one lockout (8 failures
  lock everyone for five minutes). Jellyball runs on uvicorn, which honours
  `X-Forwarded-For`/`X-Forwarded-Proto` only from addresses listed in the
  `FORWARDED_ALLOW_IPS` environment variable (default `127.0.0.1`); when the
  proxy runs in another container add its address or network there. This is
  uvicorn behaviour that has not been tested with Jellyball.
* TLS terminates at the proxy; Jellyball itself speaks plain HTTP.

### Caddy

```caddyfile
jellyball.example.com {
    reverse_proxy 127.0.0.1:8000 {
        flush_interval -1
    }
}
```

Caddy gets and renews the certificate itself and keeps the original `Host`.

### nginx

```nginx
server {
    listen 443 ssl;
    server_name jellyball.example.com;
    ssl_certificate     /etc/letsencrypt/live/jellyball.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/jellyball.example.com/privkey.pem;

    # Example restriction: only the LAN and VPN may connect.
    allow 192.168.0.0/16;
    allow 10.8.0.0/24;
    deny  all;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host              $http_host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_buffering off;
        proxy_read_timeout 300s;
    }
}
```

Publish the container only to the proxy when both run on one host:
`ports: ["127.0.0.1:8000:8000"]` in a compose override (then Jellyfin on
another machine cannot use the direct address, so decide which path Jellyfin
takes first).

## Troubleshooting

* Container exits at once: `docker compose logs jellyball`. The usual cause is
  an unwritable `./data` (needs uid 1000).
* `(unhealthy)`: the app did not answer `/healthz`; check the log for a
  startup error (port in use is reported and exits).
* Dashboard unreachable from another machine: check the published port, the
  host firewall, and that you are using the generated password.
* More: [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
