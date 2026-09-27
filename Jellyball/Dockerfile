# Pinned to a Debian release so a rebuild doesn't silently move to a new base OS.
# Playwright's Chromium is only published for amd64 and arm64 Linux.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8000 \
    DB_FILE=/app/data/sports_proxy.db \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PYSTRAY_BACKEND=dummy \
    JELLYBALL_DATA_DIR=/app/data \
    JELLYBALL_HEADLESS=1 \
    JELLYBALL_HOST=0.0.0.0

WORKDIR /app

# Install curl for container health checks and ffmpeg for Multi-View compositing.
# Note: Debian slim's ffmpeg has no NVENC/hardware-encoder support, so Multi-View
# under Docker falls back to software encoding (set MULTIVIEW_HWACCEL=none) unless
# a CUDA-enabled base image + nvidia-container-toolkit is set up separately.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -m playwright install --with-deps chromium && \
    chmod -R a+rX /ms-playwright

# Copy application source code (see .dockerignore for what's excluded — notably the
# local .env, the PyInstaller build/dist output and the packaged .exe, and any local
# sports_proxy.db, none of which belong in the image)
COPY . .

# Ensure storage directory for SQLite database, and don't run the container as root.
RUN mkdir -p /app/data \
    && useradd --system --create-home --uid 1000 jellyball \
    && chown -R jellyball:jellyball /app
USER jellyball

EXPOSE 8000

# /healthz is unauthenticated, in-memory and made for this.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:${PORT}/healthz || exit 1

# main.py runs headless automatically here (JELLYBALL_HEADLESS=1 above): no system
# tray (pystray needs a display this container doesn't have) and bound to 0.0.0.0
# so the EXPOSE'd port is actually reachable from outside the container. Because
# that is a network bind, the dashboard requires a password: set
# DASHBOARD_PASSWORD, or use the one generated on first start (printed in
# `docker logs jellyball`, saved to /app/data/dashboard-password.txt).
CMD ["python", "main.py"]

