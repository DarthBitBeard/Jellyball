"""Built-in provider plugin: OneStreamScraper."""

import os

from plugins.sdk import HtmlAggregatorProvider


class OneStreamScraper(HtmlAggregatorProvider):
    def __init__(self):
        super().__init__(
            "1Stream",
            os.getenv("AGGREGATOR_10_URL", "https://1stream.ws"),
            [],
            ["/match/", "/stream/", "/live/"],
        )
