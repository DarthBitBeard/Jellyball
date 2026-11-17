"""Built-in provider plugin: StreamEastScraper."""

import os

from plugins.sdk import HtmlAggregatorProvider


class StreamEastScraper(HtmlAggregatorProvider):
    def __init__(self):
        super().__init__(
            "StreamEast",
            # thestreameast.top answers 301 to streameast.cool (issue #49).
            os.getenv("AGGREGATOR_4_URL", "https://streameast.cool"),
            ["/nflstreams4", "/nbastreams4", "/nhlstreams4", "/mlbstreams4", "/cfbstreams4", "/ncaabstreams4"],
            ["/stream/", "/match/", "/live/"],
            # Event pages are /<league>/<teams-slug>/<numeric id>, e.g.
            # /nfl/baltimore-ravens-jacksonville-jaguars/49229088.
            event_path_patterns=[r"^/[a-z0-9-]+/[a-z0-9]+(?:-[a-z0-9]+)+/\d+/?$"],
            series_sections={"f1": ["/f1streams4"]},
        )
