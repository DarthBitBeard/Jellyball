Retired the legacy passthrough proxy: fMP4 and separate-audio sources are now
remuxed through ffmpeg into the normalizing session engine instead of being
handed to a passthrough relay, and the chunk cache, prefetch, startup buffer,
and `/substream.m3u8`, `/resource`, `/chunk*` routes are gone. SAMPLE-AES
sources keep a minimal signed relay shim since ffmpeg cannot decrypt them.
The `STREAM_CHUNK_CACHE_*`, `STREAM_STARTUP_BUFFER_SECONDS`, `PREFETCH_*`,
`MAX_MANIFEST_BYTES`, `MAX_RESOURCE_BYTES`, `MAX_CACHEABLE_CHUNK_BYTES`,
`LEGACY_RETRY_SECONDS`, and `INCOMPATIBLE_RETRY_SECONDS` env knobs are
ignored (a startup warning lists any that are still set).
