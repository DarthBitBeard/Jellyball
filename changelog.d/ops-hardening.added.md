Ops hardening quick wins. `/healthz` stays the lightweight liveness probe and a new
unauthenticated `/readyz` endpoint reports database writability, disk space, and ffmpeg
availability, returning 503 when a critical check fails so orchestrators stop routing
traffic. Expensive endpoints (`/api/test-stream`, `/rescrape/*`, `/api/import-config`)
and the `/stream/*` segment path are now rate-limited per client IP as blast-radius
control. The Docker Compose service sets memory/CPU limits, drops all Linux capabilities,
and runs the root filesystem read-only with tmpfs for its scratch dirs. The opt-in
update check now cross-verifies the reported tag against the canonical GitHub Releases
API before showing the banner, and only accepts release links under this repo's tag
pages. The Linux install guide and README document the back-up-this-one-directory
disaster-recovery story for `JELLYBALL_DATA_DIR`.
