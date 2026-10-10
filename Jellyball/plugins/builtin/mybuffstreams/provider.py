"""Built-in provider plugin: MyBuffStreamsScraper."""

import os

from plugins.sdk import HtmlAggregatorProvider


class MyBuffStreamsScraper(HtmlAggregatorProvider):
    def __init__(self):
        super().__init__(
            "MyBuffStreams",
            os.getenv("AGGREGATOR_2_URL", "https://mybuffstreams.plus"),
            [
                "/cfbstreams2", "/nflstreams2", "/mlb-live-streams",
                "/nbastreams2", "/nhlstreams2", "/soccer-live-streams",
            ],
            ["/cfb/", "/mlb/", "/nfl/", "/nba/", "/nhl/", "/title-game/", "/watch/", "/soccer/"],
        )
