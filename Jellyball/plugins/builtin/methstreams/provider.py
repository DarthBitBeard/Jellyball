"""Built-in provider plugin: MethStreamsScraper."""

import os

from plugins.sdk import HtmlAggregatorProvider


class MethStreamsScraper(HtmlAggregatorProvider):
    def __init__(self):
        super().__init__(
            "MethStreams",
            os.getenv("AGGREGATOR_3_URL", "https://methstreams.click"),
            [],
            ["/game/", "/match/", "/live/"],
        )
