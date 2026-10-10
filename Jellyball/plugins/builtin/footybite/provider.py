"""Built-in provider plugin: FootybiteScraper."""

import os

from plugins.sdk import HtmlAggregatorProvider


class FootybiteScraper(HtmlAggregatorProvider):
    def __init__(self):
        super().__init__(
            "Footybite",
            os.getenv("AGGREGATOR_9_URL", "https://footybite.im"),
            [],
            ["/watch/", "/stream/", "/live/", "/match/"],
        )
