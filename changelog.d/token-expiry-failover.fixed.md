On cold start, a 401/403 (expired CDN token) now fails over to the next candidate immediately instead of only scheduling a background rescrape; the rescrape still runs as a backstop.
