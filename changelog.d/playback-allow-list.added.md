`PLAYBACK_ALLOW_CIDRS` limits who may fetch the playlist, the guide, streams and the
signed relay (for example `192.168.1.0/24`); this machine is always allowed. Unset,
nothing changes. Behind a reverse proxy, list it in `TRUSTED_PROXIES` so the real
client address is checked: `X-Forwarded-For` is only read from a trusted proxy.
