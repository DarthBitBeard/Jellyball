FastAPI's `/docs`, `/redoc` and `/openapi.json` pages are no longer served to
anyone who can reach the port. They are off by default; set
`JELLYBALL_API_DOCS=1` to turn them on, behind the dashboard sign-in.
