"""Short-lived, single-flight cache for the dashboard's heavy database reads.

The dashboard tabs aggregate thousands of telemetry rows. Reloading the page
(or several browsers/tabs polling it) used to repeat that work per request;
here concurrent callers share one in-flight computation and a result is reused
for a few seconds.
"""

import asyncio
import time
from typing import Any, Callable, Dict, Tuple

import db

_ENTRIES: Dict[str, Tuple[float, Any]] = {}
_INFLIGHT: Dict[str, "asyncio.Future[Any]"] = {}

DEFAULT_TTL_SECONDS = 5.0


def clear() -> None:
    _ENTRIES.clear()


async def cached_thread(key: str, func: Callable[[], Any], ttl: float = DEFAULT_TTL_SECONDS) -> Any:
    """`await asyncio.to_thread(func)`, shared and cached for `ttl` seconds."""
    key = f"{db.DB_FILE}|{key}"  # a different database (tests, restores) never shares entries
    hit = _ENTRIES.get(key)
    if hit is not None and time.monotonic() - hit[0] < ttl:
        return hit[1]
    pending = _INFLIGHT.get(key)
    if pending is not None:
        return await asyncio.shield(pending)
    loop = asyncio.get_running_loop()
    future: "asyncio.Future[Any]" = loop.create_future()
    _INFLIGHT[key] = future
    try:
        value = await asyncio.to_thread(func)
    except BaseException as exc:
        if not future.done():
            future.set_exception(exc)
            future.exception()  # mark retrieved: no "never retrieved" noise when nobody else waited
        raise
    else:
        _ENTRIES[key] = (time.monotonic(), value)
        future.set_result(value)
        return value
    finally:
        _INFLIGHT.pop(key, None)
