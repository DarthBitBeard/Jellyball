"""Built-in provider plugin: StreamedSuScraper."""

import os

from plugins.sdk import HtmlAggregatorProvider


class StreamedSuScraper(HtmlAggregatorProvider):
    def __init__(self):
        super().__init__(
            "Streamed",
            os.getenv("AGGREGATOR_11_URL", "https://streamed.su"),
            ["/category/football", "/category/american-football", "/category/basketball", "/category/baseball", "/category/hockey"],
            ["/watch/", "/live/"],
        )
