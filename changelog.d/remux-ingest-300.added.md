Added per-source ffmpeg remux ingest: fMP4/CMAF and demuxed-audio sources are
remuxed (`-c copy`, no transcoding) to local MPEG-TS HLS and fed through the
normal session pipeline, so discontinuity epochs, failover, alerts, and
metrics behave exactly as for native TS sources. Falls back to `-c:a aac`
transcode when the audio codec does not fit MPEG-TS; the dashboard Remux
Ingest card tracks starts, transcodes, and failures by provider.
