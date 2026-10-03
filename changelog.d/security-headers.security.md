Every response now carries `X-Content-Type-Options: nosniff` and
`Referrer-Policy: same-origin`; dashboard pages also get a Content-Security-Policy
(no framing, no plugins, no `<base>` or cross-site form targets, scripts and
requests only to Jellyball itself) and `X-Frame-Options: DENY`.
