"""A small dedicated thread pool for CPU-bound scraping work.

Provider searches parse whole HTML pages (BeautifulSoup, fuzzy matching). That
work was already moved off the event loop with asyncio.to_thread, but to_thread
uses the loop's default executor, which is the SAME pool that serves every
dashboard database read, settings lookup and live-stream file operation. With
~150 channels each queueing several page parses, the default pool's few workers
were all busy for minutes and every GET / waited behind hundreds of parse jobs
(issues 49 and 52: 640 s page loads while /healthz stayed instant).

Parse jobs go here instead: a pool of CPU_PARSE_WORKERS threads (default 1) so
scrapes can never occupy the default executor.

Moving the work to threads is not enough on its own, because pure-Python parsing
holds the GIL. Measured with 150 channels and 40 queued parse jobs, warm, GET /
took 0.11 s idle but 5.3 s with 2 parse threads and 1.8 s with 1: the event loop
thread has to win the GIL back after every await, and waited the default 5 ms
switch interval each time (hundreds of times per page). Extra threads add no
throughput for pure-Python work (only one runs at a time), so the default is one,
and configure_gil() lowers the switch interval to 0.5 ms (GIL_SWITCH_INTERVAL_MS):
1 thread 0.3 s, 2 threads 1.3 s.
"""

import asyncio
import contextvars
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

_POOL: Optional[ThreadPoolExecutor] = None


def _worker_count() -> int:
    try:
        return max(1, min(8, int(os.getenv("CPU_PARSE_WORKERS", "1"))))
    except ValueError:
        return 1


def configure_gil() -> float:
    """Shorten the interpreter's thread switch interval so the event loop thread
    gets the GIL back quickly from CPU-bound worker threads. Returns the value set."""
    try:
        milliseconds = float(os.getenv("GIL_SWITCH_INTERVAL_MS", "0.5"))
    except ValueError:
        milliseconds = 0.5
    seconds = min(5.0, max(0.05, milliseconds)) / 1000.0
    sys.setswitchinterval(seconds)
    return seconds


def _pool() -> ThreadPoolExecutor:
    global _POOL
    if _POOL is None:
        _POOL = ThreadPoolExecutor(max_workers=_worker_count(), thread_name_prefix="jellyball-cpu")
    return _POOL


async def run_cpu(func: Callable[..., T], *args, **kwargs) -> T:
    """Run blocking CPU-bound `func` in the dedicated pool.

    Copies the caller's context like asyncio.to_thread does, because provider
    telemetry reports through context variables from inside the parse code."""
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    return await loop.run_in_executor(_pool(), lambda: ctx.run(func, *args, **kwargs))


def shutdown() -> None:
    global _POOL
    pool, _POOL = _POOL, None
    if pool is not None:
        pool.shutdown(wait=False, cancel_futures=True)
