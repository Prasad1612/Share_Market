"""
===================================================================
                    NseKitAsync.py  v6.0
===================================================================

Drop-in async upgrade of NseKit v5.0.

Key improvements over v5 sync version
──────────────────────────────────────
• aiohttp replaces requests  → true async I/O, no thread blocking
• asyncio.Semaphore replaces threading.RLock rate-limiter sleep
• Concurrent multi-fetch via _fetch_many() / asyncio.gather()
• Deduped _fetch / _get / _post into one unified _request() core
• Deduped _chunk_fetch / _chunk_fetch_iter into one async generator
• Cookie vault is async-safe (asyncio.Lock instead of RLock)
• All public methods are async def — call with await
• sync_fetch() helper lets you call from sync code via asyncio.run()
• Backward-compatible return types (DataFrame, dict, list, None)

Usage
─────
    import asyncio
    from NseKitAsync import AsyncNse

    async def main():
        async with AsyncNse() as nse:
            df   = await nse.index_live_all_indices_data()
            mkt  = await nse.nse_market_status()

            # Concurrent batch — all fire at the same time
            results = await nse.fetch_many([
                nse.nse_market_status,
                nse.nse_live_market_turnover,
                nse.index_live_all_indices_data,
            ])

    asyncio.run(main())

    # One-liner sync wrapper
    from NseKitAsync import sync_fetch
    df = sync_fetch(AsyncNse().index_live_all_indices_data)
"""

from __future__ import annotations

import asyncio
import enum
import functools
import hashlib
import heapq
import json
import logging
import random
import re
import time
import warnings
from collections import OrderedDict
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from typing import Any, Callable, Final, Iterator, TypeVar, ParamSpec

import aiohttp
import feedparser
import pandas as pd
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from rich.text import Text

# ─── Type vars ───────────────────────────────────────────────────────────────
T = TypeVar("T")
P = ParamSpec("P")

__all__ = [
    "AsyncNse",
    "AsyncNseSession",
    "Period",
    "CachePolicy",
    "RateLimitConfig",
    "CacheConfig",
    "sync_fetch",
]

log = logging.getLogger("nsekit.async")


# ═════════════════════════════════════════════════════════════════════════════
# Enums
# ═════════════════════════════════════════════════════════════════════════════

class Period(str, enum.Enum):
    D1="1D"; W1="1W"; M1="1M"; M3="3M"; M6="6M"
    Y1="1Y"; Y2="2Y"; Y5="5Y"; Y10="10Y"; YTD="YTD"; MAX="MAX"

    @classmethod
    def _missing_(cls, value: object) -> "Period | None":
        if isinstance(value, str):
            for m in cls:
                if m.value == value.upper().strip():
                    return m
        return None


class CachePolicy(enum.IntFlag):
    NONE      = 0
    READ      = enum.auto()
    WRITE     = enum.auto()
    READWRITE = READ | WRITE


# ═════════════════════════════════════════════════════════════════════════════
# Config dataclasses
# ═════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class RateLimitConfig:
    max_per_second: int   = 5      # raised vs sync (async can safely go faster)
    max_per_minute: int   = 180
    min_gap:        float = 0.2    # tighter gap — aiohttp pipelining helps

@dataclass(frozen=True)
class CacheConfig:
    enabled:     bool  = True
    default_ttl: float = 15.0
    hist_ttl:    float = 300.0
    max_size:    int   = 512


# ═════════════════════════════════════════════════════════════════════════════
# Constants
# ═════════════════════════════════════════════════════════════════════════════

_USER_AGENTS: Final[list[str]] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

_DELTA_MAP: Final[dict[str, timedelta]] = {
    "1D": timedelta(days=1),    "1W": timedelta(weeks=1),
    "1M": timedelta(days=30),   "3M": timedelta(days=90),
    "6M": timedelta(days=180),  "1Y": timedelta(days=365),
    "2Y": timedelta(days=730),  "5Y": timedelta(days=1825),
    "10Y": timedelta(days=3650),
}

_DATE_RE:   Final = re.compile(r"^\d{2}-\d{2}-\d{4}$")
_PERIOD_RE: Final = re.compile(r"^(1D|1W|1M|3M|6M|1Y|2Y|5Y|10Y|YTD|MAX)$", re.IGNORECASE)

_NSE_ROOT:    Final = "https://www.nseindia.com/"
_NSE_OPTION:  Final = "https://www.nseindia.com/option-chain"
_DEFAULT_RL:  Final = RateLimitConfig()

_RETRY_BASE:  Final[float] = 2.0
_WARMUP_WAIT: Final[float] = 0.3

_CACHE_POLICY_CTX: ContextVar[CachePolicy] = ContextVar("_cache_policy", default=CachePolicy.READWRITE)
_MISS = object()


# ═════════════════════════════════════════════════════════════════════════════
# LRU + TTL Cache  (asyncio.Lock — no threads)
# ═════════════════════════════════════════════════════════════════════════════

class _AsyncCache:
    """Thread-safe (coroutine-safe) LRU+TTL cache backed by asyncio.Lock."""

    __slots__ = ("_lock", "_store", "_heap", "_max_size")

    def __init__(self, max_size: int = 512) -> None:
        self._lock     = asyncio.Lock()
        self._store: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._heap: list[tuple[float, str]] = []
        self._max_size = max_size

    async def get(self, key: str) -> Any:
        async with self._lock:
            entry = self._store.get(key, _MISS)
            if entry is _MISS:
                return _MISS
            value, exp = entry  # type: ignore[misc]
            if time.monotonic() > exp:
                del self._store[key]
                return _MISS
            self._store.move_to_end(key)
            return value

    async def set(self, key: str, value: Any, ttl: float) -> None:
        async with self._lock:
            exp = time.monotonic() + ttl
            self._store[key] = (value, exp)
            self._store.move_to_end(key)
            heapq.heappush(self._heap, (exp, key))
            # LRU eviction
            while len(self._store) > self._max_size:
                self._store.popitem(last=False)

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()
            self._heap.clear()

    def stats(self) -> dict:
        return {"size": len(self._store), "max_size": self._max_size}


# ═════════════════════════════════════════════════════════════════════════════
# Async Rate Limiter  (token bucket + sliding window, asyncio.Lock)
# ═════════════════════════════════════════════════════════════════════════════

class _AsyncRateLimiter:
    """Async sliding-window + token-bucket rate limiter.

    Uses asyncio.sleep() so the event loop is never blocked during waits.
    """

    def __init__(self, cfg: RateLimitConfig) -> None:
        self._lock           = asyncio.Lock()
        self.max_per_second  = cfg.max_per_second
        self.max_per_minute  = cfg.max_per_minute
        self.min_gap         = cfg.min_gap
        self._second_window: list[float] = []
        self._minute_window: list[float] = []
        self._last_ts        = 0.0
        self._backoff        = 0.0
        self._total_calls    = 0
        self._total_429s     = 0
        self._total_waited   = 0.0

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self._purge(now)
                delay = self._compute_delay(now)
                if delay <= 0:
                    self._last_ts = now
                    self._second_window.append(now)
                    self._minute_window.append(now)
                    self._total_calls += 1
                    return
                self._total_waited += delay
            await asyncio.sleep(delay)

    async def on_429(self, backoff: float | None = None) -> None:
        async with self._lock:
            self._total_429s += 1
            self._backoff = float(backoff) if backoff is not None else min(
                max(self._backoff * 2, 2.0), 60.0
            )

    async def reset_backoff(self) -> None:
        async with self._lock:
            self._backoff = 0.0

    def stats(self) -> dict:
        return {
            "total_calls":       self._total_calls,
            "total_429s":        self._total_429s,
            "total_waited_secs": round(self._total_waited, 2),
            "current_backoff":   self._backoff,
            "max_per_second":    self.max_per_second,
            "max_per_minute":    self.max_per_minute,
        }

    # ── private ───────────────────────────────────────────────────────────────

    def _compute_delay(self, now: float) -> float:
        delay = 0.0
        gap = self._last_ts + self.min_gap - now
        if gap > 0:
            delay = max(delay, gap)
        if len(self._second_window) >= self.max_per_second:
            w = self._second_window[0] + 1.0 - now
            if w > 0: delay = max(delay, w)
        if len(self._minute_window) >= self.max_per_minute:
            w = self._minute_window[0] + 60.0 - now
            if w > 0: delay = max(delay, w)
        if self._backoff > 0:
            r = self._last_ts + self._backoff - now
            if r > 0: delay = max(delay, r)
            else: self._backoff = 0.0
        return delay

    def _purge(self, now: float) -> None:
        cut1 = now - 1.0;  cut60 = now - 60.0
        self._second_window = [t for t in self._second_window if t >= cut1]
        self._minute_window = [t for t in self._minute_window if t >= cut60]


# ═════════════════════════════════════════════════════════════════════════════
# Async Cookie Vault  (singleton)
# ═════════════════════════════════════════════════════════════════════════════

class _AsyncCookieVault:
    """Process-wide singleton cookie store with async-safe refresh."""

    _instance: "_AsyncCookieVault | None" = None
    _class_lock = asyncio.Lock() if False else None  # initialised lazily

    def __new__(cls) -> "_AsyncCookieVault":
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._lock       = None   # type: ignore[attr-defined]
            inst._cookies: dict[str, str] = {}
            inst._expires_at = 0.0
            inst._ttl        = 60.0
            cls._instance    = inst
        return cls._instance  # type: ignore[return-value]

    def _ensure_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get(self, session: aiohttp.ClientSession, headers: dict) -> dict[str, str]:
        lock = self._ensure_lock()
        async with lock:
            if self._cookies and time.monotonic() < self._expires_at:
                return dict(self._cookies)
            for url in [_NSE_ROOT, "https://www.nseindia.com/market-data/live-equity-market"]:
                try:
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.ok:
                            fresh = {k: v.value for k, v in r.cookies.items()}
                            if fresh:
                                self._cookies    = fresh
                                self._expires_at = time.monotonic() + self._ttl
                                return dict(self._cookies)
                except Exception as exc:
                    log.debug("[CookieVault] refresh failed %s: %s", url, exc)
            return dict(self._cookies)

    async def inject(self, cookies: dict[str, str]) -> None:
        lock = self._ensure_lock()
        async with lock:
            self._cookies.update(cookies)
            self._expires_at = time.monotonic() + self._ttl

    async def invalidate(self) -> None:
        lock = self._ensure_lock()
        async with lock:
            self._expires_at = 0.0

    @classmethod
    def reset(cls) -> None:
        """Force a brand-new singleton (useful in tests)."""
        cls._instance = None


# ═════════════════════════════════════════════════════════════════════════════
# Cache key helper
# ═════════════════════════════════════════════════════════════════════════════

def _cache_key(name: str, args: tuple, kwargs: dict) -> str:
    raw = repr((name, args, tuple(sorted(kwargs.items()))))
    return f"{name}#{hashlib.blake2b(raw.encode(), digest_size=8).hexdigest()}"


# ═════════════════════════════════════════════════════════════════════════════
# Decorators
# ═════════════════════════════════════════════════════════════════════════════

def nse_api(
    *,
    ttl: float = 15.0,
    cache: CachePolicy = CachePolicy.READWRITE,
    retries: int = 3,
) -> Callable:
    """Rate-limit + cache decorator for async NSE methods."""
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(self: "AsyncNse", *args: Any, **kwargs: Any) -> Any:
            policy = _CACHE_POLICY_CTX.get(cache)
            key    = _cache_key(fn.__qualname__, args, kwargs)

            if policy & CachePolicy.READ:
                hit = await self._cache.get(key)
                if hit is not _MISS:
                    return hit

            await self._rate_limiter.acquire()
            result = await fn(self, *args, **kwargs)

            if (policy & CachePolicy.WRITE) and result is not None:
                await self._cache.set(key, result, ttl)

            return result
        return wrapper
    return decorator


# ═════════════════════════════════════════════════════════════════════════════
# Date helpers  (pure, stateless)
# ═════════════════════════════════════════════════════════════════════════════

def _period_to_dates(
    *args: Any,
    from_date: str | None = None,
    to_date:   str | None = None,
    period:    str | None = None,
    default_period: str   = "1Y",
) -> tuple[str, str]:
    """Resolve positional date/period args into (from_str, to_str)."""
    today     = datetime.now()
    today_str = today.strftime("%d-%m-%Y")
    for arg in args:
        if isinstance(arg, datetime):
            if not from_date: from_date = arg.strftime("%d-%m-%Y")
            elif not to_date: to_date   = arg.strftime("%d-%m-%Y")
        elif isinstance(arg, str):
            if _DATE_RE.match(arg):
                if not from_date: from_date = arg
                elif not to_date: to_date   = arg
            elif _PERIOD_RE.match(arg.upper()):
                period = arg.upper()
    if period:
        p = period.upper()
        if p == "YTD":
            from_date = datetime(today.year, 1, 1).strftime("%d-%m-%Y")
            to_date   = today_str
        elif p == "MAX":
            from_date, to_date = "01-01-2008", today_str
        else:
            delta     = _DELTA_MAP.get(p, _DELTA_MAP.get(default_period, timedelta(days=365)))
            from_date = (today - delta).strftime("%d-%m-%Y")
            to_date   = to_date or today_str
    if not from_date:
        from_date = (today - _DELTA_MAP.get(default_period, timedelta(days=365))).strftime("%d-%m-%Y")
    return from_date, to_date or today_str


# ═════════════════════════════════════════════════════════════════════════════
# Main async client
# ═════════════════════════════════════════════════════════════════════════════

class AsyncNse:
    """
    Async NSE / SEBI data client powered by aiohttp.

    All data-fetching methods are ``async def`` — use with ``await``.

    Parameters
    ----------
    max_per_second : burst cap (default 5)
    max_per_minute : sustained cap (default 180)
    min_gap        : minimum inter-request gap seconds (default 0.2)
    cache_ttl      : live-endpoint cache TTL seconds (default 15)
    cache_size     : max LRU entries (default 512)
    verbose        : enable DEBUG logging

    Examples
    --------
    ::

        async with AsyncNse() as nse:
            df = await nse.index_live_all_indices_data()

        # Concurrent batch (all fire in parallel)
        results = await nse.fetch_many([
            nse.nse_market_status,
            nse.nse_live_market_turnover,
        ])
    """

    def __init__(
        self,
        max_per_second: int   = _DEFAULT_RL.max_per_second,
        max_per_minute: int   = _DEFAULT_RL.max_per_minute,
        min_gap:        float = _DEFAULT_RL.min_gap,
        cache_ttl:      float = 15.0,
        cache_size:     int   = 512,
        verbose:        bool  = False,
    ) -> None:
        if verbose:
            log.setLevel(logging.DEBUG)
            if not log.handlers:
                h = logging.StreamHandler()
                h.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
                log.addHandler(h)

        rl_cfg = RateLimitConfig(
            max_per_second=max_per_second,
            max_per_minute=max_per_minute,
            min_gap=min_gap,
        )
        self._rate_limiter = _AsyncRateLimiter(rl_cfg)
        self._cache        = _AsyncCache(max_size=cache_size)
        self._cache_ttl    = cache_ttl
        self._cookie_vault = _AsyncCookieVault()
        self._session: aiohttp.ClientSession | None = None
        self._call_count   = 0

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def __aenter__(self) -> "AsyncNse":
        await self._open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def _open(self) -> None:
        connector = aiohttp.TCPConnector(
            limit=20,           # max concurrent connections
            ttl_dns_cache=300,  # reuse DNS lookups
            ssl=False,          # NSE uses standard TLS — skip verify overhead
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            headers=self._base_headers(),
            cookie_jar=aiohttp.CookieJar(),
        )
        await self._warm_up()

    async def close(self) -> None:
        await self._cache.clear()
        await self._cookie_vault.invalidate()
        if self._session and not self._session.closed:
            await self._session.close()

    # ── headers ───────────────────────────────────────────────────────────────

    def _base_headers(self) -> dict[str, str]:
        return {
            "User-Agent":       random.choice(_USER_AGENTS),
            "Accept":           "application/json, text/javascript, */*; q=0.01",
            "Accept-Language":  "en-US,en;q=0.9",
            "Accept-Encoding":  "gzip, deflate, br",
            "Referer":          _NSE_ROOT,
            "X-Requested-With": "XMLHttpRequest",
            "Connection":       "keep-alive",
            "Origin":           "https://www.nseindia.com",
        }

    def _rotate_ua(self) -> dict[str, str]:
        """Return a header dict with a rotated User-Agent (every 5 calls)."""
        hdrs = dict(self._session.headers) if self._session else self._base_headers()
        if self._call_count % 5 == 0:
            hdrs["User-Agent"] = random.choice(_USER_AGENTS)
        return hdrs

    # ── warm-up ───────────────────────────────────────────────────────────────

    async def _warm_up(self) -> None:
        try:
            hdrs = self._rotate_ua()
            cookies = await self._cookie_vault.get(self._session, hdrs)
            await self._rate_limiter.acquire()
            async with self._session.get(
                "https://www.nseindia.com/market-data/live-equity-market",
                headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.ok:
                    fresh = {k: v.value for k, v in r.cookies.items()}
                    await self._cookie_vault.inject(fresh)
            await asyncio.sleep(_WARMUP_WAIT)
        except Exception:
            pass

    # ── core unified request ──────────────────────────────────────────────────

    async def _request(
        self,
        url:           str,
        *,
        method:        str        = "GET",
        ref_url:       str | None = None,
        params:        dict | None = None,
        data:          dict | None = None,
        extra_headers: dict | None = None,
        timeout:       int        = 10,
        retries:       int        = 3,
        is_json:       bool       = True,
    ) -> Any:
        """
        Unified async HTTP helper — replaces _fetch / _get / _post.

        • Rotates UA every 5 calls
        • Warms cookies via vault (no redundant round-trips)
        • Only hits ref_url when it differs from NSE root (one GET per chain)
        • Returns parsed JSON, raw aiohttp.ClientResponse, or None
        """
        assert self._session, "Call async with AsyncNse() as nse first"

        hdrs = self._rotate_ua()
        if extra_headers:
            hdrs = {**hdrs, **extra_headers}

        # Best-effort referrer cookie warm-up (skipped for root itself)
        if ref_url and ref_url not in (_NSE_ROOT, "https://www.nseindia.com/"):
            try:
                async with self._session.get(
                    ref_url, headers=hdrs, timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.ok:
                        fresh = {k: v.value for k, v in r.cookies.items()}
                        await self._cookie_vault.inject(fresh)
            except Exception as exc:
                log.debug("[AsyncNse] ref_url failed (%s): %s", ref_url[:60], exc)

        for attempt in range(1, retries + 1):
            await self._rate_limiter.acquire()
            self._call_count += 1
            try:
                req_kw: dict = dict(headers=hdrs, timeout=aiohttp.ClientTimeout(total=timeout))
                if params: req_kw["params"] = params
                if data:   req_kw["data"]   = data

                async with (
                    self._session.post(url, **req_kw) if method.upper() == "POST"
                    else self._session.get(url, **req_kw)
                ) as resp:
                    if resp.status == 429:
                        await self._rate_limiter.on_429()
                        if attempt < retries:
                            continue
                        log.warning("[AsyncNse] 429 → %s", url[:80])
                        return None

                    resp.raise_for_status()
                    await self._rate_limiter.reset_backoff()

                    # Inject fresh cookies
                    fresh = {k: v.value for k, v in resp.cookies.items()}
                    if fresh:
                        await self._cookie_vault.inject(fresh)

                    if not is_json:
                        # Caller wants the raw bytes
                        return await resp.read()

                    try:
                        return await resp.json(content_type=None)
                    except Exception:
                        text = await resp.text()
                        log.warning("[AsyncNse] non-JSON response → %s", url[:80])
                        return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < retries:
                    await asyncio.sleep(_RETRY_BASE ** (attempt - 1))
                else:
                    log.debug("[AsyncNse] %s → %s", url[:80], exc)
                    return None
        return None

    # Convenience thin wrappers (for readability in public methods)
    async def _fetch(self, ref_url: str, api_url: str, **kw: Any) -> Any:
        return await self._request(api_url, ref_url=ref_url, **kw)

    async def _get(self, url: str, *, is_json: bool = True,
                   ref_url: str = _NSE_ROOT, **kw: Any) -> Any:
        return await self._request(url, ref_url=ref_url, is_json=is_json, **kw)

    async def _post(self, url: str, *, data: dict | None = None,
                    extra_headers: dict | None = None, **kw: Any) -> Any:
        return await self._request(
            url, method="POST", data=data, extra_headers=extra_headers, **kw
        )

    # ── chunked date-range downloader (async generator) ───────────────────────

    async def _chunk_fetch(
        self,
        ref_url:           str,
        base_api_template: str,
        from_date:         str,
        to_date:           str,
        *,
        chunk_days:  int = 89,
        max_retries: int = 3,
        data_key:    str = "data",
    ) -> list:
        """Materialise the async chunk generator into a list."""
        results: list = []
        async for rec in self._chunk_fetch_iter(
            ref_url, base_api_template, from_date, to_date,
            chunk_days=chunk_days, max_retries=max_retries, data_key=data_key,
        ):
            results.append(rec)
        return results

    async def _chunk_fetch_iter(
        self,
        ref_url:           str,
        base_api_template: str,
        from_date:         str,
        to_date:           str,
        *,
        chunk_days:  int = 89,
        max_retries: int = 3,
        data_key:    str = "data",
    ):
        """Async generator: yields records from date-chunked NSE endpoints."""
        start_dt = datetime.strptime(from_date, "%d-%m-%Y")
        end_dt   = datetime.strptime(to_date,   "%d-%m-%Y")

        while start_dt <= end_dt:
            chunk_end = min(start_dt + timedelta(days=chunk_days), end_dt)
            api_url   = base_api_template.format(
                start_dt.strftime("%d-%m-%Y"),
                chunk_end.strftime("%d-%m-%Y"),
            )
            payload = await self._request(
                api_url, ref_url=ref_url,
                timeout=15 + max_retries * 5,
                retries=max_retries,
            )
            if payload is not None:
                records = payload.get(data_key) if isinstance(payload, dict) else payload
                if isinstance(records, list):
                    for rec in records:
                        yield rec
            start_dt = chunk_end + timedelta(days=1)

    # ── concurrent batch fetch ────────────────────────────────────────────────

    async def fetch_many(
        self,
        callables: list[Callable[[], Any]],
        *,
        return_exceptions: bool = True,
    ) -> list[Any]:
        """
        Fire multiple zero-arg async callables concurrently.

        ::

            results = await nse.fetch_many([
                nse.nse_market_status,
                nse.nse_live_market_turnover,
                nse.index_live_all_indices_data,
            ])
        """
        coros = [fn() for fn in callables]
        return list(await asyncio.gather(*coros, return_exceptions=return_exceptions))

    # ── cache helpers ─────────────────────────────────────────────────────────

    async def clear_cache(self) -> None:
        await self._cache.clear()

    def cache_stats(self) -> dict:
        return self._cache.stats()

    def rate_limiter_stats(self) -> dict:
        return self._rate_limiter.stats()

    # ── symbol validation ─────────────────────────────────────────────────────

    @staticmethod
    def _validate_symbol(symbol: str) -> str:
        cleaned = symbol.strip().upper()
        if not cleaned:
            raise ValueError("symbol must not be empty")
        if not re.fullmatch(r"[A-Z0-9&\-]+", cleaned):
            raise ValueError(f"Invalid symbol: {cleaned!r}")
        return cleaned

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  NSE MARKET  ███████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def nse_market_status(self, mode: str = "Market Status") -> pd.DataFrame | dict | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/marketStatus",
        )
        if not data:
            return None

        def _df(raw, renames):
            if not isinstance(raw, (dict, list)): return None
            return pd.DataFrame([raw] if isinstance(raw, dict) else raw).rename(columns=renames)

        ms  = _df(data.get("marketState"), {})
        if ms is not None:
            keep = ["market","marketStatus","tradeDate","index","last","variation","percentChange","marketStatusMessage"]
            ms = ms[[c for c in keep if c in ms.columns]]

        mc  = _df(data.get("marketcap"), {
            "timeStamp": "Date",
            "marketCapinTRDollars": "MarketCap_USD_Trillion",
            "marketCapinLACCRRupees": "MarketCap_INR_LakhCr",
            "marketCapinCRRupees": "MarketCap_INR_Cr",
        })
        n50 = _df(data.get("indicativenifty50"), {
            "dateTime": "DateTime", "indexName": "Index",
            "closingValue": "ClosingValue", "finalClosingValue": "FinalClose",
            "change": "Change", "perChange": "PercentChange",
        })
        gn  = _df(data.get("giftnifty"), {
            "SYMBOL": "Symbol", "EXPIRYDATE": "ExpiryDate",
            "LASTPRICE": "LastPrice", "DAYCHANGE": "DayChange",
            "PERCHANGE": "PercentChange", "CONTRACTSTRADED": "ContractsTraded",
            "TIMESTMP": "Timestamp",
        })
        mapping = {"market status": ms, "mcap": mc, "nifty50": n50, "gift nifty": gn}
        m = mode.strip().lower()
        if m in mapping: return mapping[m]
        if m == "all":   return {"Market Status": ms, "Mcap": mc, "Nifty50": n50, "Gift Nifty": gn}
        return ms

    async def nse_is_market_open(self, market: str = "Capital Market") -> Text:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/marketStatus",
        )
        if not data:
            return Text("Error fetching NSE Market Status", style="bold red")
        sel = next((m for m in data.get("marketState", []) if m.get("market") == market), None)
        if not sel:
            return Text(f"[{market}] → Market data not found.", style="bold yellow")
        msg  = sel.get("marketStatusMessage", "").strip()
        text = Text(f"[{market}] → ", style="bold white")
        text.append(
            msg,
            style="bold red" if any(w in msg.lower() for w in ("closed","halted","suspended"))
            else "bold green",
        )
        return text

    async def nse_trading_holidays(self, list_only: bool = False) -> pd.DataFrame | list | None:
        data = await self._get("https://www.nseindia.com/api/holiday-master?type=trading")
        if not data or "CM" not in data: return None
        df = pd.DataFrame(data["CM"], columns=[
            "Sr_no","tradingDate","weekDay","description","morning_session","evening_session"
        ])
        return df["tradingDate"].tolist() if list_only else df

    async def nse_clearing_holidays(self, list_only: bool = False) -> pd.DataFrame | list | None:
        data = await self._get("https://www.nseindia.com/api/holiday-master?type=clearing")
        if not data or "CD" not in data: return None
        df = pd.DataFrame(data["CD"], columns=[
            "Sr_no","tradingDate","weekDay","description","morning_session","evening_session"
        ])
        return df["tradingDate"].tolist() if list_only else df

    async def is_nse_trading_holiday(self, date_str: str | None = None) -> bool | None:
        holidays = await self.nse_trading_holidays(list_only=True)
        if holidays is None: return None
        try:
            d = datetime.strptime(date_str, "%d-%b-%Y") if date_str else datetime.today()
            return d.strftime("%d-%b-%Y") in holidays
        except ValueError: return None

    @nse_api(ttl=15.0)
    async def nse_live_market_turnover(self) -> pd.DataFrame:
        data = await self._fetch(
            "https://www.nseindia.com/option-chain",
            "https://www.nseindia.com/api/NextApi/apiClient?functionName=getMarketTurnoverSummary",
        )
        raw = (data or {}).get("data") or {}
        if not raw: return pd.DataFrame()
        rows = []
        for seg, recs in raw.items():
            if isinstance(recs, list):
                for item in recs:
                    rows.append({
                        "Segment":                  seg.upper(),
                        "Product":                  item.get("instrument", ""),
                        "Vol (Shares/Contracts)":   item.get("volume", 0),
                        "Value (₹ Cr)":             round(item.get("value", 0) / 1e7, 2),
                        "OI (Contracts)":           item.get("oivalue", 0),
                        "No. of Trades":            item.get("noOfTrades", 0),
                        "Updated At":               item.get("mktTimeStamp", ""),
                    })
        return pd.DataFrame(rows).replace([float("nan"), float("inf"), float("-inf")], None) if rows else pd.DataFrame()

    @nse_api(ttl=15.0)
    async def nse_live_hist_circulars(
        self,
        from_date_str: str | None = None,
        to_date_str:   str | None = None,
        filter:        str | None = None,
    ) -> pd.DataFrame:
        today         = datetime.now()
        from_date_str = from_date_str or (today - timedelta(days=1)).strftime("%d-%m-%Y")
        to_date_str   = to_date_str   or today.strftime("%d-%m-%Y")
        _cols = ["Date","Circulars No","Category","Department","Subject","Attachment"]
        data  = await self._fetch(
            "https://www.nseindia.com/resources/exchange-communication-circulars",
            f"https://www.nseindia.com/api/circulars?&fromDate={from_date_str}&toDate={to_date_str}",
        )
        items = (data or {}).get("data", []) if isinstance(data, dict) else []
        if not items: return pd.DataFrame(columns=_cols)
        df = pd.DataFrame(items).rename(columns={
            "cirDisplayDate": "Date", "circDisplayNo": "Circulars No",
            "circCategory": "Category", "circDepartment": "Department",
            "sub": "Subject", "circFilelink": "Attachment",
        })
        df = df[[c for c in _cols if c in df.columns]]
        if filter: df = df[df["Department"].str.contains(filter, case=False, na=False)]
        return df.reset_index(drop=True)

    async def nse_reference_rates(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/option-chain",
            "https://www.nseindia.com/api/NextApi/apiClient?functionName=getReferenceRates&type=null&flag=CUR",
        )
        raw   = data.get("data") if isinstance(data, dict) else data
        rates = (raw or {}).get("currencySpotRates") or (raw or {}).get("spotRates") or (raw if isinstance(raw, list) else [])
        if not rates: return None
        df   = pd.DataFrame(rates)
        cols = [c for c in ["currency","unit","value","prevDayValue"] if c in df.columns]
        return df[cols] if cols else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  LISTS  ████████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def nse_6m_nifty_50(self, list_only: bool = False) -> pd.DataFrame | list | None:
        raw = await self._get("https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv", is_json=False)
        if not raw: return None
        df = pd.read_csv(BytesIO(raw)); df.columns = df.columns.str.strip()
        df = df[["Company Name","Industry","Symbol","Series","ISIN Code"]]
        return df["Symbol"].tolist() if list_only else df

    async def nse_6m_nifty_500(self, list_only: bool = False) -> pd.DataFrame | list | None:
        raw = await self._get("https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv", is_json=False)
        if not raw: return None
        df = pd.read_csv(BytesIO(raw)); df.columns = df.columns.str.strip()
        df = df[["Company Name","Industry","Symbol","Series","ISIN Code"]]
        return df["Symbol"].tolist() if list_only else df

    async def nse_eod_equity_full_list(self, list_only: bool = False) -> pd.DataFrame | list | None:
        raw = await self._get("https://archives.nseindia.com/content/equities/EQUITY_L.csv", is_json=False)
        if not raw: return None
        df = pd.read_csv(BytesIO(raw))
        df = df[["SYMBOL","NAME OF COMPANY"," SERIES"," DATE OF LISTING"," FACE VALUE"]]
        return df["SYMBOL"].tolist() if list_only else df

    async def nse_eom_fno_full_list(self, mode: str = "stocks", list_only: bool = False) -> pd.DataFrame | list | None:
        data = await self._fetch(
            "https://www.nseindia.com/products-services/equity-derivatives-list-underlyings-information",
            "https://www.nseindia.com/api/underlying-information",
        )
        if not data: return None
        try:
            raw = data["data"]["IndexList"] if mode.strip().lower() == "index" else data["data"]["UnderlyingList"]
        except (KeyError, TypeError): return None
        df = pd.DataFrame(raw).rename(columns={"serialNumber":"Serial Number","symbol":"Symbol","underlying":"Underlying"})
        return df["Symbol"].tolist() if list_only else df[["Serial Number","Symbol","Underlying"]]

    async def list_of_indices(self, as_dataframe: bool = False) -> dict | pd.DataFrame | None:
        data = await self._get(
            "https://www.nseindia.com/api/equity-master",
            ref_url="https://www.nseindia.com/option-chain",
        )
        if not data: return None
        if not as_dataframe: return data
        rows = [{"indexCategory": cat, "index": idx}
                for cat, indices in data.items()
                for idx in (indices if isinstance(indices, list) else [indices])]
        return pd.DataFrame(rows) if rows else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  INDEX — LIVE  █████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def index_live_all_indices_data(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/index-performances",
            "https://www.nseindia.com/api/allIndices",
        )
        if not isinstance(data, dict) or "data" not in data: return None
        df = pd.DataFrame(data["data"])
        num_cols = ["last","variation","percentChange","open","high","low","previousClose",
                    "yearHigh","yearLow","perChange30d","perChange365d"]
        for c in num_cols:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.reset_index(drop=True) if not df.empty else None

    @nse_api(ttl=15.0)
    async def index_live_specific_data(self, index_name: str) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/index-performances",
            "https://www.nseindia.com/api/allIndices",
        )
        if not isinstance(data, dict): return None
        matches = [r for r in data.get("data", []) if r.get("index","").upper() == index_name.upper()]
        return pd.DataFrame(matches).reset_index(drop=True) if matches else None

    @nse_api(ttl=15.0)
    async def cm_live_market_statistics(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/market-data-pre-open?key=ALL",
        )
        if not isinstance(data, dict): return None
        return pd.DataFrame([{
            "advances":  data.get("advances", 0),
            "declines":  data.get("declines", 0),
            "unchanged": data.get("unchanged", 0),
            "timestamp": data.get("timestamp", ""),
        }])

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  INDEX — HISTORICAL  ████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def index_historical_data(
        self, index_name: str, *args: Any,
        from_date: str | None = None, to_date: str | None = None, period: str | None = None,
    ) -> pd.DataFrame | None:
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1Y")
        enc   = index_name.replace(" ", "%20").replace("&", "%26")
        tmpl  = (f"https://www.nseindia.com/api/historical/indicesHistory?"
                 f"indexType={enc}&from={{0}}&to={{1}}")
        rows  = await self._chunk_fetch(
            "https://www.nseindia.com/market-data/index-performances",
            tmpl, from_str, to_str,
        )
        if not rows: return None
        df = pd.DataFrame(rows)
        rename = {"EOD_TIMESTAMP":"Date","EOD_OPEN_INDEX_VAL":"Open","EOD_HIGH_INDEX_VAL":"High",
                  "EOD_LOW_INDEX_VAL":"Low","EOD_CLOSE_INDEX_VAL":"Close",
                  "EOD_INDEX_VAL":"Close","OPEN_INDEX_VAL":"Open","HIGH_INDEX_VAL":"High",
                  "LOW_INDEX_VAL":"Low","CLOSE_INDEX_VAL":"Close",
                  "EOD_TRADED_QTY":"Volume","ISIN_CODE":"ISIN"}
        df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            df = df.sort_values("Date").reset_index(drop=True)
        return df

    async def india_vix_historical_data(
        self, *args: Any,
        from_date: str | None = None, to_date: str | None = None, period: str = "1M",
    ) -> pd.DataFrame | None:
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1M")
        tmpl = ("https://www.nseindia.com/api/historical/vixhistory?"
                "from={0}&to={1}")
        rows = await self._chunk_fetch(
            "https://www.nseindia.com/market-data/india-vix",
            tmpl, from_str, to_str,
        )
        if not rows: return None
        df = pd.DataFrame(rows).rename(columns={
            "EOD_TIMESTAMP":"Date","EOD_OPEN_INDEX_VAL":"Open Price","EOD_HIGH_INDEX_VAL":"High Price",
            "EOD_LOW_INDEX_VAL":"Low Price","EOD_CLOSE_INDEX_VAL":"Close Price",
            "EOD_PREV_CLOSE":"Prev Close","CHANGE":"Change","PCHANGE":"% Change",
        })
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            df = df.sort_values("Date").reset_index(drop=True)
        return df

    async def index_pe_pb_div_historical_data(
        self, index_name: str, *args: Any,
        from_date: str | None = None, to_date: str | None = None, period: str | None = None,
    ) -> pd.DataFrame | None:
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1Y")
        enc   = index_name.replace(" ", "%20")
        tmpl  = (f"https://www.nseindia.com/api/historical/index/fundvals?"
                 f"index={enc}&from={{0}}&to={{1}}")
        rows  = await self._chunk_fetch(
            "https://www.nseindia.com/report-detail/eq_priceband",
            tmpl, from_str, to_str,
        )
        if not rows: return None
        df = pd.DataFrame(rows).rename(columns={
            "DATE": "Date", "PE": "P/E", "PB": "P/B", "DIVYIELD": "Div Yield%",
        })
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            df = df.sort_values("Date").reset_index(drop=True)
        return df

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  EQUITY — LIVE  ████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def cm_live_equity_info(self, symbol: str) -> dict | None:
        sym  = self._validate_symbol(symbol)
        data = await self._fetch(
            f"https://www.nseindia.com/get-quotes/equity?symbol={sym}",
            f"https://www.nseindia.com/api/quote-equity?symbol={sym}",
        )
        return data if isinstance(data, dict) else None

    @nse_api(ttl=15.0)
    async def cm_live_equity_price_info(self, symbol: str) -> dict | None:
        sym  = self._validate_symbol(symbol)
        data = await self._fetch(
            f"https://www.nseindia.com/get-quotes/equity?symbol={sym}",
            f"https://www.nseindia.com/api/quote-equity?symbol={sym}&section=trade_info",
        )
        return data if isinstance(data, dict) else None

    @nse_api(ttl=15.0)
    async def cm_live_equity_market(self, market: str = "NIFTY 50") -> pd.DataFrame | None:
        enc  = market.replace(" ", "%20")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            f"https://www.nseindia.com/api/equity-stockIndices?index={enc}",
        )
        if not isinstance(data, dict) or "data" not in data: return None
        return pd.DataFrame(data["data"])

    @nse_api(ttl=15.0)
    async def cm_live_gifty_nifty(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/NextApi/apiClient?functionName=getGiftNiftyData",
        )
        items = (data or {}).get("data") if isinstance(data, dict) else []
        return pd.DataFrame(items) if items else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  EQUITY — HISTORICAL  ██████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def cm_hist_security_wise_data(
        self, symbol: str, *args: Any,
        from_date: str | None = None, to_date: str | None = None, period: str | None = None,
    ) -> pd.DataFrame | None:
        sym = self._validate_symbol(symbol)
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1Y")
        tmpl = (f"https://www.nseindia.com/api/historical/cm/equity?"
                f"symbol={sym}&from={{0}}&to={{1}}")
        rows = await self._chunk_fetch(
            f"https://www.nseindia.com/get-quotes/equity?symbol={sym}",
            tmpl, from_str, to_str,
        )
        if not rows: return None
        df = pd.DataFrame(rows)
        if "CH_TIMESTAMP" in df.columns:
            df = df.rename(columns={"CH_TIMESTAMP":"Date","CH_OPENING_PRICE":"Open",
                                    "CH_TRADE_HIGH_PRICE":"High","CH_TRADE_LOW_PRICE":"Low",
                                    "CH_CLOSING_PRICE":"Close","CH_TOT_TRADED_VAL":"Turnover",
                                    "CH_TOT_TRADED_QTY":"Volume","CH_SERIES":"Series"})
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.sort_values("Date").reset_index(drop=True)
        return df

    async def cm_live_hist_insider_trading(
        self, symbol: str, *args: Any,
        from_date: str | None = None, to_date: str | None = None, period: str | None = None,
    ) -> pd.DataFrame | None:
        sym = self._validate_symbol(symbol)
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="3M")
        data = await self._fetch(
            f"https://www.nseindia.com/companies-listing/corporate-filings-insider-trading-data",
            f"https://www.nseindia.com/api/corporates-pit?symbol={sym}&from={from_str}&to={to_str}&csvDownload=false",
        )
        items = (data or {}).get("data", []) if isinstance(data, dict) else []
        return pd.DataFrame(items) if items else None

    async def cm_live_hist_board_meetings(
        self, symbol: str, *args: Any,
        from_date: str | None = None, to_date: str | None = None, period: str | None = None,
    ) -> pd.DataFrame | None:
        sym = self._validate_symbol(symbol)
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="3M")
        data = await self._fetch(
            f"https://www.nseindia.com/companies-listing/corporate-filings-board-meetings",
            f"https://www.nseindia.com/api/corporate-board-meetings?symbol={sym}&from={from_str}&to={to_str}",
        )
        items = (data or {}).get("data", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return pd.DataFrame(items) if items else None

    async def cm_live_hist_corporate_action(
        self, *args: Any,
        symbol:    str | None = None,
        from_date: str | None = None,
        to_date:   str | None = None,
        period:    str | None = None,
        filter:    str | None = None,
    ) -> pd.DataFrame | None:
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="3M")
        sym_q = f"&symbol={self._validate_symbol(symbol)}" if symbol else ""
        data  = await self._fetch(
            "https://www.nseindia.com/companies-listing/corporate-filings-actions",
            f"https://www.nseindia.com/api/corporates-corporateActions?from={from_str}&to={to_str}{sym_q}",
        )
        items = data if isinstance(data, list) else (data or {}).get("data", [])
        if not items: return None
        df = pd.DataFrame(items)
        if filter and "PURPOSE" in df.columns:
            df = df[df["PURPOSE"].str.contains(filter, case=False, na=False)]
        return df.reset_index(drop=True) if not df.empty else None

    async def cm_live_hist_annual_reports(
        self, symbol: str, *args: Any,
        from_date: str | None = None, to_date: str | None = None, period: str | None = None,
    ) -> pd.DataFrame | None:
        sym = self._validate_symbol(symbol)
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="3M")
        data = await self._fetch(
            f"https://www.nseindia.com/companies-listing/corporate-filings-annual-reports",
            f"https://www.nseindia.com/api/annual-reports?symbol={sym}&from={from_str}&to={to_str}",
        )
        items = (data or {}).get("data", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return pd.DataFrame(items) if items else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  PRE-OPEN MARKET  ██████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    _PRE_OPEN_XREF = {
        "NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY",
        "Emerge": "SME", "Securities in F&O": "FO",
        "Others": "OTHERS", "All": "ALL",
    }

    @nse_api(ttl=15.0)
    async def pre_market_info(self, category: str = "All") -> pd.DataFrame | None:
        key  = self._PRE_OPEN_XREF.get(category, "ALL")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market",
            f"https://www.nseindia.com/api/market-data-pre-open?key={key}",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        try:
            rows = [
                {
                    "symbol":             i["metadata"]["symbol"],
                    "previousClose":      i["metadata"]["previousClose"],
                    "iep":                i["metadata"]["iep"],
                    "change":             i["metadata"]["change"],
                    "pChange":            i["metadata"]["pChange"],
                    "lastPrice":          i["metadata"]["lastPrice"],
                    "finalQuantity":      i["metadata"]["finalQuantity"],
                    "totalTurnover":      i["metadata"]["totalTurnover"],
                    "totalBuyQuantity":   i["detail"]["preOpenMarket"]["totalBuyQuantity"],
                    "totalSellQuantity":  i["detail"]["preOpenMarket"]["totalSellQuantity"],
                    "lastUpdateTime":     i["detail"]["preOpenMarket"]["lastUpdateTime"],
                }
                for i in data["data"]
            ]
        except (KeyError, TypeError):
            rows = [i.get("metadata", i) for i in data["data"]]
        df = pd.DataFrame(rows)
        return df if not df.empty else None

    @nse_api(ttl=15.0)
    async def pre_market_nifty_info(self, category: str = "All") -> pd.DataFrame | None:
        key  = self._PRE_OPEN_XREF.get(category, "ALL")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market",
            f"https://www.nseindia.com/api/market-data-pre-open?key={key}",
        )
        if not data: return None
        ns = data.get("niftyPreopenStatus", {})
        return pd.DataFrame([{
            "lastPrice":  ns.get("lastPrice", "N/A"),
            "change":     ns.get("change", "N/A"),
            "pChange":    ns.get("pChange", "N/A"),
            "advances":   data.get("advances", 0),
            "declines":   data.get("declines", 0),
            "unchanged":  data.get("unchanged", 0),
            "timestamp":  data.get("timestamp", ""),
        }])

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  F&O — LIVE  ████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def fno_live_option_chain(
        self, symbol: str = "NIFTY", oi_mode: str = "full"
    ) -> pd.DataFrame | None:
        sym  = self._validate_symbol(symbol)
        data = await self._fetch(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/option-chain-indices?symbol={sym}"
            if sym in ("NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY")
            else f"https://www.nseindia.com/api/option-chain-equities?symbol={sym}",
        )
        if not isinstance(data, dict) or "records" not in data: return None
        records   = data["records"].get("data", [])
        timestamp = data["records"].get("timestamp", "")
        exp_dates = data["records"].get("expiryDates", [])
        rows: list[dict] = []
        for rec in records:
            for opt_type in ("CE", "PE"):
                opt = rec.get(opt_type)
                if not opt: continue
                rows.append({
                    "ExpiryDate":     rec.get("expiryDate", ""),
                    "StrikePrice":    rec.get("strikePrice", ""),
                    "OptionType":     opt_type,
                    "OI":             opt.get("openInterest", 0),
                    "ChngOI":         opt.get("changeinOpenInterest", 0),
                    "Volume":         opt.get("totalTradedVolume", 0),
                    "IV":             opt.get("impliedVolatility", 0),
                    "LTP":            opt.get("lastPrice", 0),
                    "Bid":            opt.get("bidprice", 0),
                    "Ask":            opt.get("askPrice", 0),
                    "Timestamp":      timestamp,
                })
        return pd.DataFrame(rows) if rows else None

    @nse_api(ttl=15.0)
    async def fno_live_derivatives_snapshot(self, symbol: str = "NIFTY") -> pd.DataFrame | None:
        sym  = self._validate_symbol(symbol)
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            f"https://www.nseindia.com/api/liveEquity-derivatives?index={sym.replace(' ','%20')}",
        )
        if not isinstance(data, dict) or "data" not in data: return None
        return pd.DataFrame(data["data"])

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  F&O — HISTORICAL  █████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def future_price_volume_data(
        self,
        symbol:       str,
        instrument:   str,
        *args:        Any,
        from_date:    str | None = None,
        to_date:      str | None = None,
        period:       str | None = None,
    ) -> pd.DataFrame | None:
        sym        = self._validate_symbol(symbol)
        inst_enc   = instrument.replace(" ", "%20")
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1M")
        tmpl = (f"https://www.nseindia.com/api/historical/fo/derivatives?"
                f"symbol={sym}&instrumentType={inst_enc}"
                f"&optionType=&strikePrice=&from={{0}}&to={{1}}")
        rows = await self._chunk_fetch(
            "https://www.nseindia.com/market-data/derivatives-market",
            tmpl, from_str, to_str,
        )
        return pd.DataFrame(rows) if rows else None

    async def option_price_volume_data(
        self,
        symbol:     str,
        instrument: str,
        *args:      Any,
        expiry:     str | None = None,
        strike:     str | None = None,
        opt_type:   str | None = None,
        from_date:  str | None = None,
        to_date:    str | None = None,
        period:     str | None = None,
    ) -> pd.DataFrame | None:
        sym      = self._validate_symbol(symbol)
        inst_enc = instrument.replace(" ", "%20")
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1M")
        expiry_q    = f"&expiryDate={expiry}" if expiry else ""
        strike_q    = f"&strikePrice={strike}" if strike else ""
        opt_type_q  = f"&optionType={opt_type}" if opt_type else ""
        tmpl = (f"https://www.nseindia.com/api/historical/fo/derivatives?"
                f"symbol={sym}&instrumentType={inst_enc}"
                f"{expiry_q}{strike_q}{opt_type_q}"
                f"&from={{0}}&to={{1}}")
        rows = await self._chunk_fetch(
            "https://www.nseindia.com/market-data/derivatives-market",
            tmpl, from_str, to_str,
        )
        return pd.DataFrame(rows) if rows else None

    async def fno_eod_participant_wise_oi(self, trade_date: str) -> pd.DataFrame | None:
        url = (f"https://www.nseindia.com/api/historical/fo/participant-oi?"
               f"date={trade_date}")
        data = await self._get(url, ref_url="https://www.nseindia.com/market-data/derivatives-market")
        if not isinstance(data, dict): return None
        rows = data.get("data") or data.get("clientTypeData") or []
        return pd.DataFrame(rows) if rows else None

    async def fno_eod_participant_wise_vol(self, trade_date: str) -> pd.DataFrame | None:
        url = (f"https://www.nseindia.com/api/historical/fo/participant-vol?"
               f"date={trade_date}")
        data = await self._get(url, ref_url="https://www.nseindia.com/market-data/derivatives-market")
        if not isinstance(data, dict): return None
        rows = data.get("data") or data.get("clientTypeData") or []
        return pd.DataFrame(rows) if rows else None

    async def fno_dmy_biz_growth(
        self,
        report_type: str = "monthly",
        *,
        month: str | None = None,
        year:  int | None = None,
    ) -> pd.DataFrame | None:
        rt   = report_type.lower()
        base = "https://www.nseindia.com/api/market-data-biz-growth"
        if rt == "daily":
            m = month or datetime.now().strftime("%b").upper()
            y = year  or datetime.now().year
            url = f"{base}?reportType=daily&month={m}&year={y}"
        elif rt == "yearly":
            url = f"{base}?reportType=yearly"
        else:
            y   = year or datetime.now().year
            url = f"{base}?reportType=monthly&year={y}"
        data = await self._fetch("https://www.nseindia.com/market-data/derivatives-market", url)
        rows = (data or {}).get("data", []) if isinstance(data, dict) else (data or [])
        return pd.DataFrame(rows) if rows else None

    async def fno_monthly_settlement_report(
        self,
        from_year: int | None = None,
        to_year:   int | None = None,
    ) -> pd.DataFrame | None:
        this_year = datetime.now().year
        fy_start  = this_year if datetime.now().month >= 4 else this_year - 1
        from_year = from_year or fy_start
        to_year   = to_year   or from_year
        all_dfs: list[pd.DataFrame] = []
        for yr in range(from_year, to_year + 1):
            data = await self._fetch(
                "https://www.nseindia.com/option-chain",
                f"https://www.nseindia.com/api/monthly-settlement-report?segment=fno&year={yr}",
            )
            if data and isinstance(data.get("data"), list):
                all_dfs.append(pd.DataFrame(data["data"]))
        return pd.concat(all_dfs, ignore_index=True) if all_dfs else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  IPO  ███████████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def ipo_tracker_summary(self, filter: str | None = None) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/ipo-tracker?type=ipo_year",
            "https://www.nseindia.com/api/NextApi/apiClient?functionName=getIPOTrackerSummary&type=ipo_year",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"])
        if df.empty: return None
        df["MARKETTYPE"] = df["MARKETTYPE"].str.upper().fillna("")
        if filter:
            df = df[df["MARKETTYPE"].str.contains(filter.upper(), case=False, na=False)]
        keep = ["SYMBOL","COMPANYNAME","LISTED_ON","ISSUE_PRICE","LISTED_DAY_CLOSE",
                "LISTED_DAY_GAIN","LISTED_DAY_GAIN_PER","LTP","GAIN_LOSS","GAIN_LOSS_PER","MARKETTYPE"]
        df = df[[c for c in keep if c in df.columns]]
        if "LISTED_ON" in df.columns:
            df["LISTED_ON"] = pd.to_datetime(df["LISTED_ON"], format="%d-%m-%Y", errors="coerce")
            df = df.sort_values("LISTED_ON", ascending=False)
            df["LISTED_ON"] = df["LISTED_ON"].dt.strftime("%Y-%m-%d")
        return df.reset_index(drop=True)

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  SEBI  ██████████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def sebi_circulars(self, *args: Any, period: str = "1W") -> pd.DataFrame:
        from_str, to_str = _period_to_dates(*args, period=period, default_period="1W")
        hdrs = {
            "User-Agent":       "Mozilla/5.0",
            "Referer":          "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0",
            "Origin":           "https://www.sebi.gov.in",
            "X-Requested-With": "XMLHttpRequest",
        }
        payload = {
            "fromDate": from_str, "toDate": to_str,
            "fromYear": "", "toYear": "", "deptId": "-1",
            "sid": "1", "ssid": "7", "smid": "0", "ssidhidden": "7",
            "intmid": "-1", "sText": "", "ssText": "Circulars", "smText": "",
            "doDirect": "-1", "nextValue": "1", "nextDel": "1", "totalpage": "1",
        }
        raw = await self._post(
            "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp",
            data=payload, extra_headers=hdrs,
        )
        # raw is None (JSON failed) — re-fetch as text
        text = await self._request(
            "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp",
            method="POST", data=payload, extra_headers=hdrs, is_json=False,
        )
        if not text: return pd.DataFrame(columns=["Date","Title","Link"])
        soup  = BeautifulSoup(text if isinstance(text, str) else text.decode(), "html.parser")
        table = soup.find("table", {"id": "sample_1"})
        rows: list[dict] = []
        if table:
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if len(tds) < 2: continue
                link_tag = tds[1].find("a")
                href     = link_tag.get("href") if link_tag else None
                if href and not href.startswith("http"):
                    href = "https://www.sebi.gov.in" + href
                rows.append({
                    "Date":  pd.to_datetime(tds[0].text.strip(), errors="coerce"),
                    "Title": link_tag.get("title") if link_tag else tds[1].text.strip(),
                    "Link":  href,
                })
        df = pd.DataFrame(rows)
        if not df.empty:
            df = (df.sort_values("Date", ascending=False)
                    .drop_duplicates(subset=["Date","Title"])
                    .reset_index(drop=True))
            df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")
        return df

    async def recent_annual_reports(self) -> pd.DataFrame:
        raw = await self._get(
            "https://nsearchives.nseindia.com/content/RSS/Annual_Reports.xml",
            is_json=False,
        )
        if not raw: return pd.DataFrame()
        feed    = feedparser.parse(raw if isinstance(raw, str) else raw.decode())
        records = []
        for item in feed.entries:
            link     = item.get("link", "")
            filename = link.split("/")[-1]
            sme      = "SME" if filename.startswith("SME_AR_") else ""
            m        = re.search(r"(?:SME_)?AR_\d+_(?P<symbol>[A-Z0-9]+)_(?P<fyFrom>\d{4})_(?P<fyTo>\d{4})_", filename)
            if not m: continue
            dm  = re.search(r"(\d{2}-[A-Z]{3}-\d{2})", item.get("description", ""))
            sub = None
            if dm:
                try: sub = datetime.strptime(dm.group(1), "%d-%b-%y").strftime("%d-%b-%Y")
                except ValueError: pass
            records.append({
                "symbol":         m.group("symbol"),
                "companyName":    item.get("title", ""),
                "fyFrom":         int(m.group("fyFrom")),
                "fyTo":           int(m.group("fyTo")),
                "link":           link,
                "submissionDate": sub,
                "SME":            sme,
            })
        return pd.DataFrame(records)

    async def quarterly_financial_results(self, symbol: str) -> dict | None:
        sym = self._validate_symbol(symbol)
        return await self._fetch(
            f"https://www.nseindia.com/get-quotes/equity?symbol={sym}",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getIntegratedFilingData&symbol={sym}",
        )

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████████████  BULK DEALS / FII / DII  ████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=30.0)
    async def cm_live_bulk_deals(self, *args: Any,
                                  from_date: str | None = None,
                                  to_date: str | None = None,
                                  period: str | None = None) -> pd.DataFrame | None:
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1M")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/bulk-deals",
            f"https://www.nseindia.com/api/bulk-deals?from={from_str}&to={to_str}",
        )
        items = (data or {}).get("data") if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return pd.DataFrame(items) if items else None

    @nse_api(ttl=30.0)
    async def cm_live_block_deals(self, *args: Any,
                                   from_date: str | None = None,
                                   to_date: str | None = None,
                                   period: str | None = None) -> pd.DataFrame | None:
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1M")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/block-deals",
            f"https://www.nseindia.com/api/block-deals?from={from_str}&to={to_str}",
        )
        items = (data or {}).get("data") if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return pd.DataFrame(items) if items else None

    @nse_api(ttl=30.0)
    async def fno_eod_fii_dii(self, *args: Any,
                                from_date: str | None = None,
                                to_date: str | None = None,
                                period: str | None = None) -> pd.DataFrame | None:
        from_str, to_str = _period_to_dates(*args, from_date=from_date, to_date=to_date,
                                             period=period, default_period="1M")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/fii-dii-trades",
            f"https://www.nseindia.com/api/fii-dii?from={from_str}&to={to_str}",
        )
        items = (data or {}).get("data") if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return pd.DataFrame(items) if items else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  CONCURRENT DASHBOARD HELPERS  ██████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def morning_dashboard(self) -> dict[str, Any]:
        """
        Fetch 5 core morning metrics in parallel — uses asyncio.gather().

        Returns a dict with keys: market, turnover, indices, vix, preopen
        """
        market, turnover, indices, vix, preopen = await asyncio.gather(
            self.nse_market_status("Market Status"),
            self.nse_live_market_turnover(),
            self.index_live_all_indices_data(),
            self.india_vix_historical_data(period="5D"),
            self.pre_market_info("All"),
            return_exceptions=True,
        )
        return {
            "market":   market,
            "turnover": turnover,
            "indices":  indices,
            "vix":      vix,
            "preopen":  preopen,
        }

    async def watchlist_snapshot(self, symbols: list[str]) -> dict[str, dict | None]:
        """Fetch live equity info for multiple symbols concurrently."""
        results = await asyncio.gather(
            *[self.cm_live_equity_info(sym) for sym in symbols],
            return_exceptions=True,
        )
        return {
            sym: (r if not isinstance(r, Exception) else None)
            for sym, r in zip(symbols, results)
        }



    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  EQUITY LIVE — EXTENDED  ████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def cm_live_equity_full_info(self, symbol: str) -> dict | None:
        enc  = self._validate_symbol(symbol).replace("&", "%26")
        data = await self._fetch(
            f"https://www.nseindia.com/get-quotes/equity?symbol={enc}",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getSymbolData&marketType=N&series=EQ&symbol={enc}",
        )
        if not data: return None
        equity = data.get("equityResponse", [])
        return equity[0] if equity else None

    @nse_api(ttl=15.0)
    async def cm_live_most_active_equity_by_value(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-equities-by-value",
            "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"])
        return df if not df.empty else None

    @nse_api(ttl=15.0)
    async def cm_live_most_active_equity_by_vol(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-equities-by-volume",
            "https://www.nseindia.com/api/live-analysis-most-active-securities?index=volume",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"])
        return df if not df.empty else None

    @nse_api(ttl=15.0)
    async def cm_live_volume_spurts(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/volume-gainers-spurts",
            "https://www.nseindia.com/api/live-analysis-volume-spurts",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"])
        df.rename(columns={"symbol":"Symbol","series":"Series","previousClose":"Prev Close",
                            "ltp":"LTP","pChange":"% Change","turnover":"Turnover (\u20b9 Lakhs)"}, inplace=True)
        return df if not df.empty else None

    @nse_api(ttl=15.0)
    async def cm_live_52week_high(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/52-week-high-equity-market",
            "https://www.nseindia.com/api/live-analysis-data-52weekhighstock",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"])
        keep = ["symbol","series","ltp","pChange","new52WHL","prev52WHL","prevHLDate"]
        return df[[c for c in keep if c in df.columns]] if not df.empty else None

    @nse_api(ttl=15.0)
    async def cm_live_52week_low(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/52-week-low-equity-market",
            "https://www.nseindia.com/api/live-analysis-data-52weeklowstock",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"])
        keep = ["symbol","series","ltp","pChange","new52WHL","prev52WHL","prevHLDate"]
        return df[[c for c in keep if c in df.columns]] if not df.empty else None

    @nse_api(ttl=15.0)
    async def cm_live_block_deal(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/block-deal-watch",
            "https://www.nseindia.com/api/block-deal",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df   = pd.DataFrame(data["data"])
        cols = ["session","symbol","series","open","dayHigh","dayLow","lastPrice","previousClose",
                "pchange","totalTradedVolume","totalTradedValue"]
        return df[[c for c in cols if c in df.columns]] if not df.empty else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  INDEX — LIVE EXTENDED  ████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def index_live_indices_stocks_data(self, category: str = "NIFTY 50", list_only: bool = False) -> pd.DataFrame | list | None:
        enc  = category.upper().replace("&", "%26").replace(" ", "%20")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            f"https://www.nseindia.com/api/equity-stockIndices?index={enc}",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"]).drop(["meta"], axis=1, errors="ignore")
        if df.empty: return None
        if list_only: return df["symbol"].tolist() if "symbol" in df.columns else []
        col_order = ["symbol","previousClose","open","dayHigh","dayLow","lastPrice","change","pChange",
                     "totalTradedVolume","totalTradedValue","nearWKH","nearWKL","perChange30d","perChange365d","ffmc"]
        return df[[c for c in col_order if c in df.columns]].replace(
            [pd.NA, float("nan"), float("inf"), float("-inf")], None
        )

    @nse_api(ttl=15.0)
    async def index_live_nifty_50_returns(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/historicalOR/niftyIndicesReturn",
        )
        if not data: return None
        records = data.get("data") or data
        return pd.DataFrame(records) if isinstance(records, list) and records else None

    @nse_api(ttl=15.0)
    async def index_live_contribution(self, *args: Any, Index: str = "NIFTY 50", Mode: str = "First Five") -> pd.DataFrame | None:
        index_ = Index; mode_ = Mode
        if len(args) == 1:
            if args[0] in ("First Five", "Full"): mode_  = args[0]
            else:                                 index_ = args[0]
        elif len(args) == 2:
            index_, mode_ = args
        index_ = str(index_).upper()
        if mode_ not in ("First Five", "Full"):
            raise ValueError("Mode must be 'First Five' or 'Full'")
        enc = index_.replace("&", "%26").replace(" ", "%20")
        url = (f"https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi"
               f"?functionName=getContributionData&index={enc}"
               + ("&flag=0" if mode_ == "First Five" else "&noofrecords=0&flag=1"))
        data = await self._fetch("https://www.nseindia.com", url)
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df   = pd.DataFrame(data["data"])
        cols = ["icSymbol","icSecurity","lastTradedPrice","changePer","isPositive","rnNegative","changePoints"]
        return df[[c for c in cols if c in df.columns]] if not df.empty else None

    async def index_eod_bhav_copy(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{d.strftime('%d%m%Y')}.csv",
            is_json=False,
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  CORPORATE FILINGS — EXTENDED  █████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def cm_live_hist_corporate_announcement(
        self, *args: Any,
        from_date: str | None = None, to_date: str | None = None,
        period: str | None = None, symbol: str | None = None,
    ) -> pd.DataFrame | None:
        today_str = datetime.now().strftime("%d-%m-%Y")
        for arg in args:
            if not isinstance(arg, str): continue
            if _DATE_RE.match(arg):        from_date = from_date or arg
            elif _PERIOD_RE.match(arg.upper()): period = arg.upper()
            else:                          symbol = arg.upper()
        if period:
            from_date = (datetime.now() - _DELTA_MAP.get(period, timedelta(days=365))).strftime("%d-%m-%Y")
            to_date   = to_date or today_str
        from_date = from_date or today_str; to_date = to_date or today_str
        if symbol and from_date:
            url = f"https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_date}&to_date={to_date}&symbol={symbol}&reqXbrl=false"
        elif symbol:
            url = f"https://www.nseindia.com/api/corporate-announcements?index=equities&symbol={symbol}&reqXbrl=false"
        else:
            url = f"https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_date}&to_date={to_date}&reqXbrl=false"
        data = await self._fetch("https://www.nseindia.com/companies-listing/corporate-filings-announcements", url)
        if not isinstance(data, list) or not data: return None
        df   = pd.DataFrame(data)
        cols = ["symbol","sm_name","smIndustry","desc","attchmntText","attchmntFile","fileSize","an_dt"]
        return df[[c for c in cols if c in df.columns]].fillna("")

    @nse_api(ttl=15.0)
    async def cm_live_today_event_calendar(self, from_date: str | None = None, to_date: str | None = None) -> pd.DataFrame | None:
        today_str = datetime.now().strftime("%d-%m-%Y")
        from_date = from_date or today_str; to_date = to_date or today_str
        data = await self._fetch(
            "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar",
            f"https://www.nseindia.com/api/event-calendar?index=equities&from_date={from_date}&to_date={to_date}",
        )
        if not isinstance(data, list) or not data: return None
        df = pd.DataFrame(data)
        return df[[c for c in ["symbol","company","purpose","bm_desc","date"] if c in df.columns]] or None

    @nse_api(ttl=15.0)
    async def cm_live_upcoming_event_calendar(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar",
            "https://www.nseindia.com/api/event-calendar?",
        )
        if not isinstance(data, list) or not data: return None
        df = pd.DataFrame(data)
        return df[[c for c in ["symbol","company","purpose","bm_desc","date"] if c in df.columns]] or None

    @nse_api(ttl=15.0)
    async def cm_live_hist_Shareholder_meetings(
        self, *args: Any,
        from_date: str | None = None, to_date: str | None = None,
        period: str | None = None, symbol: str | None = None,
    ) -> pd.DataFrame | None:
        for arg in args:
            if not isinstance(arg, str): continue
            if _DATE_RE.match(arg):         from_date = from_date or arg
            elif _PERIOD_RE.match(arg.upper()): period = arg.upper()
            else:                           symbol = arg.upper()
        if period or from_date:
            from_date, to_date = _period_to_dates(from_date=from_date, to_date=to_date, period=period, default_period="1D")
            params = f"index=equities&from_date={from_date}&to_date={to_date}" + (f"&symbol={symbol}" if symbol else "")
        elif symbol: params = f"index=equities&symbol={symbol}"
        else:        params = "index=equities"
        data    = await self._fetch(
            "https://www.nseindia.com/companies-listing/corporate-filings-postal-ballot",
            f"https://www.nseindia.com/api/postal-ballot?{params}",
        )
        records = data if isinstance(data, list) else (data.get("data",[]) if isinstance(data,dict) else [])
        if not records: return None
        df   = pd.DataFrame(records)
        cols = ["symbol","sLN","bdt","text","type","attachment","date"]
        return df[[c for c in cols if c in df.columns]].fillna("") if not df.empty else None

    async def _corporate_further_issue(
        self, issue_type: str, *args: Any,
        from_date: str | None = None, to_date: str | None = None,
        period: str | None = None, symbol: str | None = None, stage: str | None = None,
    ) -> pd.DataFrame | None:
        today     = datetime.now(); today_str = today.strftime("%d-%m-%Y")
        stage_pat = re.compile(r"^(in-principle|listing stage)$", re.IGNORECASE)
        for arg in args:
            if not isinstance(arg, str): continue
            if stage_pat.match(arg):         stage = arg.title()
            elif _DATE_RE.match(arg):        from_date = from_date or arg
            elif _PERIOD_RE.match(arg.upper()): period = arg.upper()
            else:                            symbol = arg.upper()
        if period and not (from_date and to_date):
            from_date = (today - _DELTA_MAP.get(period.upper(), timedelta(days=365))).strftime("%d-%m-%Y")
            to_date   = today_str
        stage   = (stage or "In-Principle").title()
        idx_map = ({"In-Principle":"FIQIPIP","Listing Stage":"FIQIPLS"} if issue_type == "qip"
                   else {"In-Principle":"FIPREFIP","Listing Stage":"FIPREFLS"})
        api_map = {"qip":"corporate-further-issues-qip","pref":"corporate-further-issues-pref","ri":"corporate-further-issues-ri"}
        params  = f"index={idx_map.get(stage,'FIQIPIP')}" + (
            f"&symbol={symbol}" if symbol else (
                f"&from_date={from_date}&to_date={to_date}" if from_date and to_date else ""
            )
        )
        data    = await self._fetch(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/{api_map[issue_type]}?{params}",
        )
        records = (data or {}).get("data",[]) if isinstance(data,dict) else (data if isinstance(data,list) else [])
        if not records: return None
        return pd.DataFrame(records).fillna("") or None

    @nse_api(ttl=300.0)
    async def cm_live_hist_qualified_institutional_placement(self, *args: Any, **kw: Any) -> pd.DataFrame | None:
        return await self._corporate_further_issue("qip", *args, **kw)

    @nse_api(ttl=300.0)
    async def cm_live_hist_preferential_issue(self, *args: Any, **kw: Any) -> pd.DataFrame | None:
        return await self._corporate_further_issue("pref", *args, **kw)

    @nse_api(ttl=300.0)
    async def cm_live_hist_right_issue(self, *args: Any, **kw: Any) -> pd.DataFrame | None:
        return await self._corporate_further_issue("ri", *args, **kw)

    @nse_api(ttl=15.0)
    async def cm_live_voting_results(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/companies-listing/corporate-filings-voting-results",
            "https://www.nseindia.com/api/corporate-voting-results?",
        )
        if not isinstance(data, list): return None
        rows = []
        for item in data:
            meta = item.get("metadata", {}); agendas = meta.get("agendas",[]) or item.get("agendas",[])
            rows += [{**meta, **ag} for ag in agendas] if agendas else [meta]
        if not rows: return None
        import json as _json
        df = pd.DataFrame(rows).fillna("")
        for col in df.columns:
            df[col] = df[col].map(lambda v: _json.dumps(v,ensure_ascii=False) if isinstance(v,(list,dict)) else str(v))
        return df.reset_index(drop=True)

    @nse_api(ttl=15.0)
    async def cm_live_qtly_shareholding_patterns(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern",
            "https://www.nseindia.com/api/corporate-share-holdings-master?index=equities",
        )
        if not isinstance(data, list) or not data: return None
        df   = pd.DataFrame(data)
        cols = ["symbol","name","pr_and_prgrp","public_val","employeeTrusts","revisedStatus",
                "date","submissionDate","revisionDate","xbrl","broadcastDate","systemDate","timeDifference"]
        return df[[c for c in cols if c in df.columns]] if not df.empty else None

    async def cm_live_hist_br_sr(self, symbol: str) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/companies-listing/corporate-filings-bsr",
            f"https://www.nseindia.com/api/corporate-bsr-reports?symbol={self._validate_symbol(symbol)}",
        )
        if not data: return None
        records = data.get("data") or data
        return pd.DataFrame(records) if isinstance(records, list) and records else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  CHARTS  ████████████████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def index_chart(self, index: str, period: str = "1M") -> dict | None:
        return await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            f"https://www.nseindia.com/api/historicalOR/chartData"
            f"?indexType={index.replace(' ','%20').upper()}&type={period.upper()}",
        )

    async def stock_chart(self, symbol: str, period: str = "1M") -> dict | None:
        sym = self._validate_symbol(symbol)
        return await self._fetch(
            f"https://www.nseindia.com/get-quotes/equity?symbol={sym}",
            f"https://www.nseindia.com/api/chart-databyindex?index={sym}&indices=false&type={period.upper()}",
        )

    async def fno_chart(self, symbol: str, period: str = "1M") -> dict | None:
        sym = self._validate_symbol(symbol)
        return await self._fetch(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/chart-databyindex?index={sym}&indices=false&type={period.upper()}",
        )

    async def india_vix_chart(self, period: str = "1M") -> dict | None:
        return await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            f"https://www.nseindia.com/api/historicalOR/chartData?indexType=INDIA%20VIX&type={period.upper()}",
        )

    async def identifier_based_fno_contracts_live_chart_data(self, identifier: str) -> dict | None:
        return await self._fetch(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/chart-databyindex?index={identifier}&indices=false",
        )

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  F&O LIVE — EXTENDED  ███████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def symbol_full_fno_live_data(self, symbol: str) -> dict | None:
        return await self._fetch(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/quote-derivative?symbol={self._validate_symbol(symbol)}",
        )

    @nse_api(ttl=15.0)
    async def symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI(
        self, symbol: str, opt_type: str = "CE"
    ) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings",
        )
        if not data or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"])
        if "instrumentType" in df.columns: df = df[df["instrumentType"].str.upper() == opt_type.upper()]
        return df if not df.empty else None

    @nse_api(ttl=15.0)
    async def fno_live_futures_data(self, symbol: str = "NIFTY") -> pd.DataFrame | None:
        enc  = self._validate_symbol(symbol).replace("&", "%26")
        data = await self._fetch(
            f"https://www.nseindia.com/get-quotes/derivatives?symbol={enc}",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getSymbolDerivativesData&symbol={enc}&instrumentType=FUT",
        )
        if not isinstance(data, dict): return None
        items = data.get("data", [])
        if not items: return None
        df = pd.DataFrame(items)
        num_cols = ["openPrice","highPrice","lowPrice","closePrice","prevClose","lastPrice","change",
                    "totalTradedVolume","totalTurnover","openInterest","changeinOpenInterest","pchangeinOpenInterest"]
        for col in num_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @nse_api(ttl=15.0)
    async def fno_live_top_20_derivatives_contracts(self, symbol: str = "NIFTY") -> pd.DataFrame | None:
        enc  = self._validate_symbol(symbol).replace("&", "%26")
        data = await self._fetch(
            f"https://www.nseindia.com/get-quotes/derivatives?symbol={enc}",
            f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
            f"?functionName=getSymbolDerivativesData&symbol={enc}",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        return pd.DataFrame(data["data"])

    @nse_api(ttl=15.0)
    async def fno_live_most_active_futures_contracts(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            "https://www.nseindia.com/api/live-analysis-most-active-futures?key=volume",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        return pd.DataFrame(data["data"]) or None

    @nse_api(ttl=15.0)
    async def fno_live_most_active(self, mode: str = "Index", opt_type: str = "Call", metric: str = "Volume") -> pd.DataFrame | None:
        m, o, k = mode.lower(), opt_type.lower(), metric.lower()
        suffix  = "vol" if k == "volume" else "val"
        api     = f"{o}s-{'index' if m=='index' else 'stocks'}-{suffix}"
        key     = "OPTIDX" if m == "index" else "OPTSTK"
        data    = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            f"https://www.nseindia.com/api/snapshot-derivatives-equity?index={api}",
        )
        if not isinstance(data, dict) or key not in data: return None
        df = pd.DataFrame(data[key].get("data", []))
        return df if not df.empty else None

    @nse_api(ttl=15.0)
    async def fno_live_most_active_contracts_by_oi(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            "https://www.nseindia.com/api/snapshot-derivatives-equity?index=oi",
        )
        if not isinstance(data, dict): return None
        df = pd.DataFrame((data.get("volume") or {}).get("data", []))
        return df if not df.empty else None

    @nse_api(ttl=15.0)
    async def fno_live_most_active_contracts_by_volume(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            "https://www.nseindia.com/api/live-analysis-most-active-futures?key=volume",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        return pd.DataFrame(data["data"]) or None

    @nse_api(ttl=15.0)
    async def fno_live_most_active_options_contracts_by_volume(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            "https://www.nseindia.com/api/live-analysis-most-active-options?key=index_call_volume",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        return pd.DataFrame(data["data"]) or None

    @nse_api(ttl=15.0)
    async def fno_live_most_active_underlying(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/most-active-contracts",
            "https://www.nseindia.com/api/live-analysis-most-active-underlying",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        return pd.DataFrame(data["data"]) or None

    @nse_api(ttl=15.0)
    async def fno_live_change_in_oi(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/oi-spurts",
            "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        df = pd.DataFrame(data["data"])
        df.rename(columns={"symbol":"Symbol","latestOI":"Latest OI","prevOI":"Prev OI",
                            "changeInOI":"chng in OI","avgInOI":"chng in OI %","volume":"Vol (Cntr)",
                            "futValue":"Fut Val (\u20b9 Lakhs)","premValue":"Opt Val (\u20b9 Lakhs)(Premium)",
                            "total":"Total Val (\u20b9 Lakhs)","underlyingValue":"Price"}, inplace=True)
        ordered = ["Symbol","Latest OI","Prev OI","chng in OI","chng in OI %","Vol (Cntr)",
                   "Fut Val (\u20b9 Lakhs)","Opt Val (\u20b9 Lakhs)(Premium)","Total Val (\u20b9 Lakhs)","Price"]
        return df[[c for c in ordered if c in df.columns]] if not df.empty else None

    @nse_api(ttl=15.0)
    async def fno_live_oi_vs_price(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/oi-spurts",
            "https://www.nseindia.com/api/live-analysis-oi-spurts-contracts",
        )
        if not isinstance(data, dict): return None
        rows: list[dict] = []
        for block in data.get("data", []):
            for category, contracts in block.items():
                for c in contracts:
                    c["OI_Price_Signal"] = category; rows.append(c)
        if not rows: return None
        df = pd.DataFrame(rows)
        df.rename(columns={"symbol":"Symbol","instrument":"Instrument","expiryDate":"Expiry",
                            "optionType":"Type","strikePrice":"Strike","ltp":"LTP",
                            "prevClose":"Prev Close","pChange":"% Price Chg","latestOI":"Latest OI",
                            "prevOI":"Prev OI","changeInOI":"Chg in OI","pChangeInOI":"% OI Chg",
                            "volume":"Volume","turnover":"Turnover \u20b9L","premTurnover":"Premium \u20b9L",
                            "underlyingValue":"Underlying Price"}, inplace=True)
        ordered = ["OI_Price_Signal","Symbol","Instrument","Expiry","Type","Strike","LTP","% Price Chg",
                   "Latest OI","Prev OI","Chg in OI","% OI Chg","Volume","Turnover \u20b9L","Premium \u20b9L","Underlying Price"]
        return df[[c for c in ordered if c in df.columns]]

    async def fno_expiry_dates_raw(self, symbol: str = "NIFTY") -> dict | None:
        return await self._fetch(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/option-chain-contract-info?symbol={self._validate_symbol(symbol)}",
        )

    async def fno_expiry_dates(self, symbol: str = "NIFTY", label_filter: Any = None) -> pd.DataFrame | str | list | None:
        from datetime import time as _time
        data = await self._fetch(
            "https://www.nseindia.com/option-chain",
            f"https://www.nseindia.com/api/option-chain-contract-info?symbol={self._validate_symbol(symbol)}",
        )
        if not data: return None
        raw = data.get("expiryDates") or data.get("records", {}).get("expiryDates")
        if not raw: return None
        now          = datetime.now()
        expiry_dates = pd.Series(pd.to_datetime(raw, format="%d-%b-%Y").sort_values().unique())
        expiry_dates = expiry_dates[expiry_dates >= pd.Timestamp(now.date())].reset_index(drop=True)
        if len(expiry_dates) > 0 and expiry_dates.iloc[0].date() == now.date() and now.time() > _time(15, 30):
            expiry_dates = expiry_dates.iloc[1:].reset_index(drop=True)
        if expiry_dates.empty: return None
        expiry_info = []
        for i, date in enumerate(expiry_dates):
            if i + 1 < len(expiry_dates):
                expiry_info.append("Monthly Expiry" if expiry_dates.iloc[i+1].month != date.month else "Weekly Expiry")
            else:
                expiry_info.append("Monthly Expiry")
        df = pd.DataFrame({"Expiry Date": expiry_dates.dt.strftime("%d-%b-%Y"), "Expiry Type": expiry_info})
        df["Label"] = ""
        if len(df) > 0: df.loc[0, "Label"] = "Current"
        weekly_after  = [i for i in df[df["Expiry Type"] == "Weekly Expiry"].index  if i > 0]
        monthly_after = [i for i in df[df["Expiry Type"] == "Monthly Expiry"].index if i > 0]
        if weekly_after:  df.loc[weekly_after[0],  "Label"] = "Next Week"
        if monthly_after: df.loc[monthly_after[0], "Label"] = "Month"
        df["Days Remaining"] = (expiry_dates - pd.Timestamp(now.date())).dt.days
        def _zone(e):
            if e.month == now.month and e.year == now.year:            return "Current Month"
            elif e.month == ((now.month % 12) + 1) and e.year in (now.year, now.year+1): return "Next Month"
            elif e.month in [3, 6, 9, 12]:                             return "Quarterly"
            else:                                                       return "Far Month"
        df["Contract Zone"] = expiry_dates.apply(_zone)
        df = df[["Expiry Date","Expiry Type","Label","Days Remaining","Contract Zone"]]
        if label_filter is None: return df.reset_index(drop=True)
        if label_filter == "All":
            return df[df["Label"].isin(["Current","Next Week","Month"])]["Expiry Date"].apply(
                lambda x: pd.to_datetime(x, format="%d-%b-%Y").strftime("%d-%m-%Y")).tolist()
        row = df[df["Label"] == label_filter].reset_index(drop=True)
        return None if row.empty else pd.to_datetime(row.loc[0,"Expiry Date"], format="%d-%b-%Y").strftime("%d-%m-%Y")

    async def fno_live_option_chain_raw(self, symbol: str = "NIFTY") -> dict | None:
        sym = self._validate_symbol(symbol)
        url = (f"https://www.nseindia.com/api/option-chain-indices?symbol={sym}"
               if sym in ("NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY","NIFTY NEXT 50")
               else f"https://www.nseindia.com/api/option-chain-equities?symbol={sym}")
        return await self._fetch("https://www.nseindia.com/option-chain", url)

    @nse_api(ttl=15.0)
    async def fno_live_active_contracts(self, symbol: str = "NIFTY", expiry_date: str | None = None) -> list | None:
        enc  = self._validate_symbol(symbol).replace("&", "%26")
        url  = (f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
                f"?functionName=getSymbolDerivativesData&symbol={enc}"
                + (f"&expiryDate={expiry_date}" if expiry_date else ""))
        data = await self._fetch(f"https://www.nseindia.com/get-quotes/derivatives?symbol={enc}", url)
        return data.get("data") if isinstance(data, dict) else data

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  IPO EXTENDED  ██████████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def ipo_current(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/upcoming-issues-other-than-sme",
            "https://www.nseindia.com/api/ipo?status=current",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        return pd.DataFrame(data["data"]) or None

    @nse_api(ttl=15.0)
    async def ipo_preopen(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market",
            "https://www.nseindia.com/api/market-data-pre-open?key=ITPD",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        try:
            rows = [
                {
                    "symbol":            i["metadata"]["symbol"],
                    "previousClose":     i["metadata"]["previousClose"],
                    "iep":               i["metadata"]["iep"],
                    "change":            i["metadata"]["change"],
                    "pChange":           i["metadata"]["pChange"],
                    "lastPrice":         i["metadata"]["lastPrice"],
                    "finalQuantity":     i["metadata"]["finalQuantity"],
                    "totalTurnover":     i["metadata"]["totalTurnover"],
                    "totalBuyQuantity":  i["detail"]["preOpenMarket"]["totalBuyQuantity"],
                    "totalSellQuantity": i["detail"]["preOpenMarket"]["totalSellQuantity"],
                    "lastUpdateTime":    i["detail"]["preOpenMarket"]["lastUpdateTime"],
                }
                for i in data["data"]
            ]
        except (KeyError, TypeError):
            rows = [i.get("metadata", i) for i in data["data"]]
        return pd.DataFrame(rows) or None

    @nse_api(ttl=15.0)
    async def pre_market_all_nse_adv_dec_info(self, category: str = "All") -> pd.DataFrame | None:
        key  = self._PRE_OPEN_XREF.get(category, "ALL")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market",
            f"https://www.nseindia.com/api/market-data-pre-open?key={key}",
        )
        if not data: return None
        return pd.DataFrame([{"advances":  data.get("advances", 0),
                               "declines":  data.get("declines", 0),
                               "unchanged": data.get("unchanged", 0),
                               "timestamp": data.get("timestamp", "")}])

    @nse_api(ttl=15.0)
    async def pre_market_derivatives_info(self, category: str = "Index Futures") -> pd.DataFrame | None:
        key  = {"Index Futures":"FUTIDX","Stock Futures":"FUTSTK"}.get(category,"FUTIDX")
        data = await self._fetch(
            "https://www.nseindia.com/market-data/pre-open-market-fno",
            f"https://www.nseindia.com/api/market-data-pre-open-fno?key={key}",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        try:
            rows = [
                {
                    "symbol":            i["metadata"]["symbol"],
                    "expiryDate":        i["metadata"]["expiryDate"],
                    "previousClose":     i["metadata"]["previousClose"],
                    "iep":               i["metadata"]["iep"],
                    "change":            i["metadata"]["change"],
                    "pChange":           i["metadata"]["pChange"],
                    "lastPrice":         i["metadata"]["lastPrice"],
                    "finalQuantity":     i["metadata"]["finalQuantity"],
                    "totalTurnover":     i["metadata"]["totalTurnover"],
                    "totalBuyQuantity":  i["detail"]["preOpenMarket"]["totalBuyQuantity"],
                    "totalSellQuantity": i["detail"]["preOpenMarket"]["totalSellQuantity"],
                    "lastUpdateTime":    i["detail"]["preOpenMarket"]["lastUpdateTime"],
                }
                for i in data["data"]
            ]
        except (KeyError, TypeError):
            rows = [i.get("metadata", i) for i in data["data"]]
        return pd.DataFrame(rows) or None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  CM EOD DATA  ████████████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def cm_eod_fii_dii_activity(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/fiidiiTradeReact",
        )
        return pd.DataFrame(data) if isinstance(data, list) and data else None

    async def cm_eod_market_activity_report(self) -> pd.DataFrame | None:
        data    = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            "https://www.nseindia.com/api/historicalOR/market-activity",
        )
        records = (data or {}).get("data") or data
        return pd.DataFrame(records) if isinstance(records, list) and records else None

    async def cm_eod_bhavcopy_with_delivery(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d.strftime('%d%m%Y')}.csv",
            is_json=False,
        )
        if not raw: return None
        df = pd.read_csv(BytesIO(raw)); df.columns = [c.replace(" ","") for c in df.columns]
        for col in ["SERIES","DATE1"]:
            if col in df.columns: df[col] = df[col].str.replace(" ","")
        return df

    async def cm_eod_equity_bhavcopy(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip",
            is_json=False, timeout=15,
        )
        return self._extract_csv_from_zip(raw) if raw else None

    async def cm_eod_52_week_high_low(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/equities/52_wk_hilo_equity_{d.strftime('%d%m%Y')}.csv",
            is_json=False,
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def cm_eod_bulk_deal(self) -> pd.DataFrame | None:
        raw = await self._get("https://nsearchives.nseindia.com/content/equities/bulk.csv", is_json=False)
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def cm_eod_block_deal(self) -> pd.DataFrame | None:
        raw = await self._get("https://nsearchives.nseindia.com/content/equities/block.csv", is_json=False)
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def cm_eod_shortselling(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/archives/equities/shortSelling/shortselling_{d.strftime('%d%m%Y').upper()}.csv",
            is_json=False,
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def cm_eod_surveillance_indicator(self, trade_date: str) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            f"https://www.nseindia.com/api/live-analysis-data-surv?date={trade_date}",
        )
        if not data or not isinstance(data.get("data"), list): return None
        return pd.DataFrame(data["data"]) or None

    async def cm_eod_series_change(self, trade_date: str) -> pd.DataFrame | None:
        data    = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            f"https://www.nseindia.com/api/series-changes?date={trade_date}",
        )
        records = (data or {}).get("data") or data
        return pd.DataFrame(records) if isinstance(records, list) and records else None

    async def cm_eod_eq_band_changes(self, trade_date: str) -> pd.DataFrame | None:
        data    = await self._fetch(
            "https://www.nseindia.com/market-data/live-equity-market",
            f"https://www.nseindia.com/api/price-band-changes?date={trade_date}",
        )
        records = (data or {}).get("data") or data
        return pd.DataFrame(records) if isinstance(records, list) and records else None

    async def cm_eod_eq_price_band(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/equities/sec_list_{d.strftime('%d%m%Y').upper()}.csv",
            is_json=False,
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def cm_eod_pe_ratio(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/equities/peDetail/PE_{d.strftime('%d%m%y').upper()}.csv",
            is_json=False,
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def cm_eod_mcap(self, trade_date: str) -> pd.DataFrame | None:
        import zipfile as _zipfile
        d   = datetime.strptime(trade_date, "%d-%m-%y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{d.strftime('%d%m%y').upper()}.zip",
            is_json=False,
        )
        if not raw: return None
        with _zipfile.ZipFile(BytesIO(raw)) as zf:
            for name in zf.namelist():
                if name.lower().startswith("mcap") and name.endswith(".csv"):
                    return pd.read_csv(zf.open(name))
        return None

    async def cm_eod_eq_name_change(self) -> pd.DataFrame | None:
        raw = await self._get("https://nsearchives.nseindia.com/content/equities/namechange.csv", is_json=False)
        if not raw: return None
        df = pd.read_csv(BytesIO(raw))
        if df.shape[1] >= 4:
            df.iloc[:,3] = pd.to_datetime(df.iloc[:,3], format="%d-%b-%Y", errors="coerce").dt.strftime("%Y-%m-%d")
            df = df.sort_values(by=df.columns[3], ascending=False).reset_index(drop=True)
        return df

    async def cm_eod_eq_symbol_change(self) -> pd.DataFrame | None:
        raw = await self._get("https://nsearchives.nseindia.com/content/equities/symbolchange.csv", is_json=False)
        if not raw: return None
        df = pd.read_csv(BytesIO(raw), header=None)
        if df.shape[1] >= 4:
            df.iloc[:,3] = pd.to_datetime(df.iloc[:,3], format="%d-%b-%Y", errors="coerce").dt.strftime("%Y-%m-%d")
            df = df.sort_values(by=df.columns[3], ascending=False).reset_index(drop=True)
        return df

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  CM HISTORICAL — EXTENDED  ██████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def _csv_deals_fetch(self, api: str, ref_url: str = "https://www.nseindia.com/report-detail/display-bulk-and-block-deals") -> pd.DataFrame | None:
        raw = await self._request(api, ref_url=ref_url, is_json=False, timeout=15)
        if not raw: return None
        try:
            content = raw[3:] if raw[:3] == b"\xef\xbb\xbf" else raw
            df = pd.read_csv(StringIO(content.decode("utf-8")))
            if df.empty or len(df.columns) < 2: return None
            df.columns = [c.strip().replace('"','') for c in df.columns]
            return df.fillna("").replace({float("inf"):"",float("-inf"):""})
        except Exception as exc:
            log.warning("[AsyncNse] csv_deals_fetch failed: %s", exc)
            return None

    @nse_api(ttl=300.0)
    async def cm_hist_bulk_deals(self, *args: Any,
                                   from_date: str | None = None, to_date: str | None = None,
                                   period: str | None = None, symbol: str | None = None) -> pd.DataFrame | None:
        for arg in args:
            if not isinstance(arg, str): continue
            if _DATE_RE.match(arg):        from_date = from_date or arg
            elif _PERIOD_RE.match(arg.upper()): period = arg.upper()
            else:                          symbol = arg.upper()
        from_date, to_date = _period_to_dates(from_date=from_date, to_date=to_date, period=period, default_period="1D")
        base = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"
        api  = f"{base}?optionType=bulk_deals&from={from_date}&to={to_date}" + (f"&symbol={symbol}" if symbol else "") + "&csv=true"
        return await self._csv_deals_fetch(api)

    @nse_api(ttl=300.0)
    async def cm_hist_block_deals(self, *args: Any,
                                    from_date: str | None = None, to_date: str | None = None,
                                    period: str | None = None, symbol: str | None = None) -> pd.DataFrame | None:
        for arg in args:
            if not isinstance(arg, str): continue
            if _DATE_RE.match(arg):        from_date = from_date or arg
            elif _PERIOD_RE.match(arg.upper()): period = arg.upper()
            else:                          symbol = arg.upper()
        from_date, to_date = _period_to_dates(from_date=from_date, to_date=to_date, period=period, default_period="1D")
        base = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"
        api  = f"{base}?optionType=block_deals&from={from_date}&to={to_date}" + (f"&symbol={symbol}" if symbol else "") + "&csv=true"
        return await self._csv_deals_fetch(api)

    @nse_api(ttl=300.0)
    async def cm_hist_short_selling(self, *args: Any,
                                      from_date: str | None = None, to_date: str | None = None,
                                      period: str | None = None, symbol: str | None = None) -> pd.DataFrame | None:
        for arg in args:
            if not isinstance(arg, str): continue
            if _DATE_RE.match(arg):        from_date = from_date or arg
            elif _PERIOD_RE.match(arg.upper()): period = arg.upper()
            else:                          symbol = arg.upper()
        from_date, to_date = _period_to_dates(from_date=from_date, to_date=to_date, period=period, default_period="1D")
        base = "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"
        api  = f"{base}?optionType=short_selling&from={from_date}&to={to_date}" + (f"&symbol={symbol}" if symbol else "") + "&csv=true"
        return await self._csv_deals_fetch(api)

    @nse_api(ttl=300.0)
    async def cm_hist_eq_price_band(self, *args: Any,
                                      from_date: str | None = None, to_date: str | None = None,
                                      period: str | None = None, symbol: str | None = None) -> pd.DataFrame | None:
        for arg in args:
            if not isinstance(arg, str): continue
            if _DATE_RE.match(arg):        from_date = from_date or arg
            elif _PERIOD_RE.match(arg.upper()): period = arg.upper()
            else:                          symbol = arg.upper()
        from_date, to_date = _period_to_dates(from_date=from_date, to_date=to_date, period=period, default_period="1M")
        api = (f"https://www.nseindia.com/api/historicalOR/price-band-changes-equities"
               f"?from={from_date}&to={to_date}" + (f"&symbol={symbol}" if symbol else "") + "&csv=true")
        return await self._csv_deals_fetch(api, ref_url="https://www.nseindia.com/market-data/price-band-changes")

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  CM BUSINESS GROWTH  ████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @nse_api(ttl=15.0)
    async def cm_dmy_biz_growth(self, *args: Any, mode: str = "monthly",
                                  month: int | None = None, year: int | None = None) -> list | None:
        now = datetime.now()
        _mm = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
        _rv = {v: k for k, v in _mm.items()}
        for arg in args:
            if isinstance(arg, str):
                u = arg.strip().upper()
                if u in ["YEARLY","MONTHLY","DAILY"]: mode = u.lower()
                elif u.isdigit() and len(u) == 4:     year = int(u)
                elif u[:3] in _mm:                    month = _mm[u[:3]]
            elif isinstance(arg, int):
                if 1900 <= arg <= 2100: year = arg
                elif 1 <= arg <= 12:    month = arg
        year = year or now.year; month = month or now.month
        if   mode == "yearly":  url = "https://www.nseindia.com/api/historicalOR/cm/tbg/yearly"
        elif mode == "monthly": url = f"https://www.nseindia.com/api/historicalOR/cm/tbg/monthly?from={year}&to={year+1}"
        else:                   url = f"https://www.nseindia.com/api/historicalOR/cm/tbg/daily?month={_rv.get(month,str(month)).title()}&year={year}"
        data = await self._fetch("https://www.nseindia.com", url)
        if not data: return None
        data_list = [item["data"] for item in (data.get("data") or []) if "data" in item]
        if not data_list: return None
        df = pd.DataFrame(data_list)
        for col in df.columns:
            if df[col].dtype == object:
                cleaned = df[col].astype(str).str.replace(",","",regex=False).str.strip()
                df[col] = pd.to_numeric(cleaned, errors="coerce").fillna(df[col])
        return df.where(pd.notnull(df), None).to_dict(orient="records")

    @nse_api(ttl=300.0)
    async def cm_monthly_settlement_report(self, *args: Any,
                                             from_year: int | None = None,
                                             to_year:   int | None = None,
                                             period:    str | None = None) -> pd.DataFrame | None:
        for arg in args:
            if isinstance(arg, int):
                if not from_year: from_year = arg
                elif not to_year: to_year = arg
        today    = datetime.now(); fy_start = today.year if today.month >= 4 else today.year - 1
        if not from_year: from_year = fy_start
        if not to_year:   to_year   = from_year + 1
        all_data: list = []
        for fy in range(from_year, to_year):
            data = await self._fetch(
                "https://www.nseindia.com/report-detail/monthly-settlement-statistics",
                f"https://www.nseindia.com/api/historicalOR/monthly-sett-stats-data?finYear={fy}-{fy+1}",
            )
            if isinstance(data, dict) and isinstance(data.get("data"), list):
                for rec in data["data"]: rec["FinancialYear"] = f"{fy}-{fy+1}"
                all_data.extend(data["data"])
        if not all_data: return None
        df = pd.DataFrame(all_data)
        df.rename(columns={"ST_DATE":"Month","ST_TURNOVER_CRORES":"Turnover (\u20b9 Cr)",
                            "ST_NO_OF_TRADES_LACS":"No of Trades (lakhs)"}, inplace=True)
        return df

    @nse_api(ttl=15.0)
    async def cm_monthly_most_active_equity(self) -> pd.DataFrame | None:
        data = await self._fetch(
            "https://www.nseindia.com/market-data/monthly-most-active-equity",
            "https://www.nseindia.com/api/live-analysis-most-active-securities?index=monthly",
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list): return None
        return pd.DataFrame(data["data"]) or None

    @nse_api(ttl=15.0)
    async def historical_advances_decline(self, *args: Any, mode: str = "Month_wise",
                                            month: int | None = None, year: int | None = None) -> pd.DataFrame | None:
        now  = datetime.now()
        _mm  = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
        _rv  = {v:k for k,v in _mm.items()}
        for arg in args:
            if isinstance(arg, str):
                u = arg.strip().upper()
                if u in ["DAY_WISE","MONTH_WISE"]: mode = u.title()
                elif u.isdigit() and len(u)==4:   year = int(u)
                elif u[:3] in _mm:                month = _mm[u[:3]]
            elif isinstance(arg, int):
                if 1900 <= arg <= 2100: year = arg
                elif 1 <= arg <= 12:    month = arg
        year = year or now.year
        if month is None:
            prev = now.month - 1 or 12; year = year - 1 if now.month == 1 else year; month = prev
        if mode.lower() == "month_wise":
            url = f"https://www.nseindia.com/api/historicalOR/advances-decline-monthly?year={year}"
        else:
            mc  = _rv.get(int(month), now.strftime("%b").upper())
            url = f"https://www.nseindia.com/api/historicalOR/advances-decline-monthly?year={mc}-{year}"
        data = await self._fetch("https://www.nseindia.com/option-chain", url)
        if not data or not isinstance(data.get("data"), list): return None
        df   = pd.DataFrame(data["data"])
        cols = ({"ADM_MONTH":"Month","ADM_ADVANCES":"Advances","ADM_DECLINES":"Declines","ADM_ADV_DCLN_RATIO":"Adv_Decline_Ratio"}
                if mode.lower() == "month_wise" else
                {"ADD_DAY_STRING":"Day","ADD_ADVANCES":"Advances","ADD_DECLINES":"Declines","ADD_ADV_DCLN_RATIO":"Adv_Decline_Ratio"})
        df = df[[c for c in cols if c in df.columns]].rename(columns=cols)
        return df.fillna(0).replace({float("inf"):0,float("-inf"):0}) if not df.empty else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  F&O EOD EXTENDED  ██████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _extract_csv_from_zip(zip_content: bytes) -> pd.DataFrame:
        import zipfile as _zipfile
        with _zipfile.ZipFile(BytesIO(zip_content)) as zf:
            for name in zf.namelist():
                if name.endswith(".csv"): return pd.read_csv(zf.open(name))
        return pd.DataFrame()

    @staticmethod
    def detect_excel_format(file_content: BytesIO) -> str:
        sig = file_content.read(8); file_content.seek(0)
        if sig[:4] == b"PK\x03\x04":                    return "xlsx"
        if sig[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1": return "xls"
        if sig[:4] == b"\x50\x4b\x07\x08":               return "xlsb"
        return "unknown"

    @staticmethod
    def clean_mwpl_data(df: pd.DataFrame) -> pd.DataFrame:
        df.dropna(how="all", inplace=True); df.columns = df.iloc[0]; df = df[1:].reset_index(drop=True)
        new_cols: list = []; client_counter = 1
        for col in df.columns:
            if "Unnamed" in str(col) or pd.isna(col): new_cols.append(f"Client {client_counter}"); client_counter += 1
            else:                                      new_cols.append(str(col).strip())
        df.columns = new_cols; return df

    async def fno_eod_bhav_copy(self, trade_date: str = "") -> pd.DataFrame | None:
        import zipfile as _zipfile
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip",
            is_json=False,
        )
        return self._extract_csv_from_zip(raw) if raw else None

    async def fno_eod_fii_stats(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        fmt = d.strftime("%d-%b-%Y"); fmt = fmt[:3] + fmt[3:].capitalize()
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/fo/fii_stats_{fmt}.xls",
            is_json=False,
        )
        if not raw: return None
        buf    = BytesIO(raw)
        engine = {"xls":"xlrd","xlsx":"openpyxl","xlsb":"pyxlsb"}.get(self.detect_excel_format(buf))
        return pd.read_excel(buf, engine=engine, dtype=str) if engine else None

    async def fno_eod_top10_fut(self, trade_date: str) -> list | None:
        import zipfile as _zipfile, csv as _csv
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/archives/fo/mkt/fo{d.strftime('%d%m%Y').upper()}.zip",
            is_json=False,
        )
        if not raw: return None
        with _zipfile.ZipFile(BytesIO(raw)) as zf:
            for name in zf.namelist():
                if name.lower().startswith("ttfut") and name.endswith(".csv"):
                    return list(_csv.reader(zf.open(name).read().decode("utf-8", errors="ignore").splitlines()))
        return None

    async def fno_eod_top20_opt(self, trade_date: str) -> list | None:
        import zipfile as _zipfile, csv as _csv
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/archives/fo/mkt/fo{d.strftime('%d%m%Y')}.zip",
            is_json=False,
        )
        if not raw: return None
        with _zipfile.ZipFile(BytesIO(raw)) as zf:
            for name in zf.namelist():
                if name.lower().startswith("ttopt") and name.endswith(".csv"):
                    return list(_csv.reader(zf.open(name).read().decode("utf-8", errors="ignore").splitlines()))
        return None

    async def fno_eod_sec_ban(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/archives/fo/sec_ban/fo_secban_{d.strftime('%d%m%Y').upper()}.csv",
            is_json=False,
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def fno_eod_mwpl_3(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/nsccl/mwpl_cli_{d.strftime('%d%m%Y').upper()}.xls",
            is_json=False,
        )
        if not raw: return None
        buf    = BytesIO(raw)
        engine = {"xls":"xlrd","xlsx":"openpyxl","xlsb":"pyxlsb"}.get(self.detect_excel_format(buf))
        return self.clean_mwpl_data(pd.read_excel(buf, engine=engine, dtype=str)) if engine else None

    async def fno_eod_combine_oi(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%Y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{d.strftime('%d%m%Y')}.csv",
            is_json=False,
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def fno_eom_lot_size(self, symbol: str | None = None) -> pd.DataFrame | None:
        raw = await self._get("https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv", is_json=False)
        if not raw: return None
        df = pd.read_csv(BytesIO(raw)); df.columns = [c.strip() for c in df.columns]
        if symbol: df = df[df.iloc[:,1].str.strip().str.upper() == symbol.strip().upper()]
        return df if not df.empty else None

    # ═══════════════════════════════════════════════════════════════════════
    # ██████████████  MISC  ██████████████████████████████████████████████████
    # ═══════════════════════════════════════════════════════════════════════

    async def state_wise_registered_investors(self) -> dict | None:
        return await self._fetch(
            "https://www.nseindia.com/registered-investors/",
            "https://www.nseindia.com/api/registered-investors",
        )

    async def nse_eod_top10_nifty50(self, trade_date: str) -> pd.DataFrame | None:
        d   = datetime.strptime(trade_date, "%d-%m-%y")
        raw = await self._get(
            f"https://nsearchives.nseindia.com/content/indices/top10nifty50_{d.strftime('%d%m%y').upper()}.csv",
            is_json=False,
        )
        return pd.read_csv(BytesIO(raw)) if raw else None

    async def nse_live_hist_press_releases(
        self,
        from_date_str: str | None = None,
        to_date_str:   str | None = None,
        filter:        str | None = None,
    ) -> pd.DataFrame:
        today         = datetime.now()
        from_date_str = from_date_str or (today - timedelta(days=1)).strftime("%d-%m-%Y")
        to_date_str   = to_date_str   or today.strftime("%d-%m-%Y")
        empty = pd.DataFrame(columns=["DATE","DEPARTMENT","SUBJECT","ATTACHMENT URL","LAST UPDATED"])
        data  = await self._fetch(
            "https://www.nseindia.com/resources/exchange-communication-press-releases",
            f"https://www.nseindia.com/api/press-release-cms20?fromDate={from_date_str}&toDate={to_date_str}",
        )
        if not isinstance(data, list): return empty
        rows: list[dict] = []
        for item in data:
            if not isinstance(item, dict) or "content" not in item: continue
            content = item["content"]
            body    = content.get("body", "")
            if body and "<" in body and ">" in body:
                try:
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
                        body = BeautifulSoup(body, "html.parser").get_text(separator=" ").strip()
                except Exception: pass
            changed = item.get("changed", "")
            try: changed = datetime.strptime(changed, "%a, %m/%d/%Y - %H:%M").strftime("%a %d-%b-%Y %I:%M %p")
            except ValueError: pass
            att = content.get("field_file_attachement") or {}
            rows.append({"DATE": content.get("field_date",""), "DEPARTMENT": content.get("field_type",""),
                         "SUBJECT": body, "ATTACHMENT URL": att.get("url") if isinstance(att, dict) else None,
                         "LAST UPDATED": changed})
        df = pd.DataFrame(rows)
        if filter: df = df[df["DEPARTMENT"].str.contains(filter, case=False, na=False)]
        return df[["DATE","DEPARTMENT","SUBJECT","ATTACHMENT URL","LAST UPDATED"]]

    async def sebi_data(self, pages: int = 1) -> pd.DataFrame:
        hdrs = {"User-Agent":"Mozilla/5.0","Origin":"https://www.sebi.gov.in",
                "X-Requested-With":"XMLHttpRequest",
                "Referer":"https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0"}
        all_rows: list[dict] = []
        for page in range(1, pages + 1):
            text = await self._request(
                "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp",
                method="POST",
                data={"nextValue": str(page), "nextDel": str(page),
                      "totalpage": str(pages), "nextPage": "", "doDirect": "1"},
                extra_headers=hdrs, is_json=False, timeout=15,
            )
            if not text: break
            soup  = BeautifulSoup(text if isinstance(text, str) else text.decode(), "html.parser")
            table = soup.find("table", {"id": "sample_1"})
            if not table: break
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if len(tds) < 2: continue
                link_tag = tds[1].find("a")
                href     = link_tag.get("href") if link_tag else None
                if href and not href.startswith("http"): href = "https://www.sebi.gov.in" + href
                all_rows.append({"Date": tds[0].get_text(strip=True),
                                  "Title": link_tag.get("title","").strip() if link_tag else tds[1].get_text(strip=True),
                                  "Link": href})
        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = (df.sort_values("Date", ascending=False)
                    .drop_duplicates(subset=["Date","Title"], keep="first")
                    .reset_index(drop=True))
            df["Date"] = df["Date"].dt.strftime("%d-%b-%Y")
        return df

    async def html_tables(self, url: str, show_tables: bool = False, output: str = "json") -> list | None:
        raw = await self._request(url, ref_url="https://www.nseindia.com/option-chain", is_json=False, timeout=30)
        if not raw: return None
        try:
            tables = pd.read_html(StringIO(raw.decode() if isinstance(raw, bytes) else raw))
            if show_tables:
                for i, t in enumerate(tables): log.info("[AsyncNse] html_tables[%d]:\n%s", i, t.head().to_string())
            return [t.to_dict(orient="records") for t in tables] if output.lower() == "json" else tables
        except Exception as exc:
            log.warning("[AsyncNse] html_tables failed: %s", exc)
            return None

    async def is_nse_clearing_holiday(self, date_str: str | None = None) -> bool | None:
        holidays = await self.nse_clearing_holidays(list_only=True)
        if holidays is None: return None
        try:
            d = datetime.strptime(date_str, "%d-%b-%Y") if date_str else datetime.today()
            return d.strftime("%d-%b-%Y") in holidays
        except ValueError: return None


    @property
    def rate_limiter(self) -> "_AsyncRateLimiter":
        """Expose rate limiter stats/configure (mirrors sync API)."""
        return self._rate_limiter

    @property
    def query(self) -> "AsyncNseQueryBuilder":
        """Pre-bound fluent query builder for this client."""
        if not hasattr(self, "_query"):
            self._query = AsyncNseQueryBuilder(self)
        return self._query

    def rotate_user_agent(self) -> None:
        """Rotate User-Agent header (every 5 calls). No-op in async — handled inside _request."""
        pass

    def no_cache(self) -> "_AsyncNoCacheCtx":
        """Context manager to bypass cache for an async block::

            async with nse.no_cache():
                df = await nse.index_live_all_indices_data()
        """
        return _AsyncNoCacheCtx()


# ═════════════════════════════════════════════════════════════════════════════
# Async context manager factory
# ═════════════════════════════════════════════════════════════════════════════

from contextlib import asynccontextmanager
from typing import AsyncIterator

@asynccontextmanager
async def AsyncNseSession(
    *,
    max_per_second: int   = _DEFAULT_RL.max_per_second,
    max_per_minute: int   = _DEFAULT_RL.max_per_minute,
    min_gap:        float = _DEFAULT_RL.min_gap,
    cache_ttl:      float = 15.0,
    cache_size:     int   = 512,
) -> AsyncIterator[AsyncNse]:
    """
    Async context manager for AsyncNse — ensures clean teardown.

    ::

        async with AsyncNseSession(max_per_second=5) as nse:
            df = await nse.index_live_all_indices_data()
    """
    client = AsyncNse(
        max_per_second=max_per_second,
        max_per_minute=max_per_minute,
        min_gap=min_gap,
        cache_ttl=cache_ttl,
        cache_size=cache_size,
    )
    await client._open()
    try:
        yield client
    finally:
        await client.close()


# ═════════════════════════════════════════════════════════════════════════════
# Sync convenience wrapper
# ═════════════════════════════════════════════════════════════════════════════

def sync_fetch(coro_factory: Callable[[], Any]) -> Any:
    """
    Run an AsyncNse coroutine from synchronous code.

    ::

        from NseKitAsync import AsyncNse, sync_fetch

        nse = AsyncNse()
        df  = sync_fetch(lambda: _run(nse))

        async def _run(nse):
            async with nse:
                return await nse.index_live_all_indices_data()

        df = sync_fetch(_run)   # wrong — see below

    Preferred pattern::

        async def fetch():
            async with AsyncNse() as nse:
                return await nse.index_live_all_indices_data()

        df = sync_fetch(fetch)
    """
    return asyncio.run(coro_factory())


# ═════════════════════════════════════════════════════════════════════════════
# AsyncNseQueryBuilder  — fluent builder (mirrors NseQueryBuilder)
# ═════════════════════════════════════════════════════════════════════════════

class AsyncNseQueryBuilder:
    """Fluent builder for AsyncNse queries.

    ::

        df = await (AsyncNseQueryBuilder(nse)
                    .symbol("RELIANCE")
                    .period("3M")
                    .fetch(nse.cm_live_hist_insider_trading))
    """
    __slots__ = ("_client","_symbol","_period","_from","_to","_filter","_policy")

    def __init__(self, client: "AsyncNse") -> None:
        self._client = client
        self._symbol: str | None = None
        self._period: str | None = None
        self._from:   str | None = None
        self._to:     str | None = None
        self._filter: str | None = None
        self._policy: CachePolicy = CachePolicy.READWRITE

    def symbol(self, s: str) -> "AsyncNseQueryBuilder":
        self._symbol = s.upper(); return self

    def period(self, p: str) -> "AsyncNseQueryBuilder":
        self._period = p.upper(); return self

    def date_range(self, from_date: str, to_date: str) -> "AsyncNseQueryBuilder":
        self._from, self._to = from_date, to_date; return self

    def filter(self, f: str) -> "AsyncNseQueryBuilder":
        self._filter = f; return self

    def cache_policy(self, p: CachePolicy) -> "AsyncNseQueryBuilder":
        self._policy = p; return self

    async def fetch(self, method: Callable[..., Any]) -> Any:
        """Execute the built query by calling `method` with resolved params."""
        token = _CACHE_POLICY_CTX.set(self._policy)
        try:
            kwargs: dict = {}
            if self._symbol: kwargs["symbol"]    = self._symbol
            if self._period: kwargs["period"]    = self._period
            if self._from:   kwargs["from_date"] = self._from
            if self._to:     kwargs["to_date"]   = self._to
            if self._filter: kwargs["filter"]    = self._filter
            return await method(**kwargs)
        finally:
            _CACHE_POLICY_CTX.reset(token)


# ═════════════════════════════════════════════════════════════════════════════
# _AsyncNoCacheCtx  — async context manager to bypass cache
# ═════════════════════════════════════════════════════════════════════════════

class _AsyncNoCacheCtx:
    """Async context manager that disables cache for an enclosed block."""
    __slots__ = ("_token",)

    async def __aenter__(self) -> None:
        self._token = _CACHE_POLICY_CTX.set(CachePolicy.NONE)

    async def __aexit__(self, *_: Any) -> None:
        _CACHE_POLICY_CTX.reset(self._token)

