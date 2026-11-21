"""Rate limiting.

* `TokenBucket`          per-destination-host limiter used by the worker. Its state lives in Postgres,
                         so the limit holds across ALL worker processes, not just one.
* `SlidingWindowLimiter` small in-memory limiter that protects our own ingestion endpoint and the
                         mock receiver.
"""
from __future__ import annotations

import time
from collections import deque

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .models import DomainRateLimit, utcnow


class TokenBucket:
    """How a token bucket works: each host has a bucket holding up to `burst` tokens. Every delivery
    takes one token. Tokens refill continuously at `rate_per_sec`. An empty bucket means "wait"."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], rate_per_sec: float, burst: int) -> None:
        self.session_factory = session_factory
        self.rate = rate_per_sec
        self.burst = float(burst)

    async def acquire(self, host: str) -> float:
        """Take one token for `host`. Returns 0.0 if allowed, otherwise how many seconds to wait."""
        async with self.session_factory() as session, session.begin():
            bucket = await self._load_locked(session, host)

            # Add the tokens earned since we last looked, up to the bucket size.
            now = utcnow()  # taken after the lock, so waiting for another worker doesn't skew it
            elapsed = max((now - bucket.updated_at).total_seconds(), 0.0)
            bucket.tokens = min(self.burst, bucket.tokens + elapsed * self.rate)
            bucket.updated_at = now

            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return 0.0
            return (1.0 - bucket.tokens) / self.rate

    async def _load_locked(self, session: AsyncSession, host: str) -> DomainRateLimit:
        """Get this host's bucket row, locked so concurrent workers take turns. Creates it if new."""
        # No-op if the row exists; this makes the first-ever request for a host race-free.
        await session.execute(
            insert(DomainRateLimit).values(host=host, tokens=self.burst, updated_at=utcnow()).on_conflict_do_nothing()
        )
        return await session.get(DomainRateLimit, host, with_for_update=True)


class SlidingWindowLimiter:
    """In-memory sliding-window log limiter, keyed by an arbitrary string (e.g. client IP)."""

    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = {}
        self._last_sweep = time.monotonic()

    def hit(self, key: str, limit: int, window_s: float) -> tuple[bool, float]:
        """Record a request. Returns (allowed, retry_after_seconds)."""
        now = time.monotonic()
        self._sweep(now, window_s)
        q = self._hits.setdefault(key, deque())
        while q and now - q[0] >= window_s:
            q.popleft()
        if len(q) >= limit:
            return False, max(window_s - (now - q[0]), 0.0)
        q.append(now)
        return True, 0.0

    def _sweep(self, now: float, window_s: float) -> None:
        """Drop idle keys occasionally so memory stays bounded."""
        if now - self._last_sweep < 60:
            return
        self._last_sweep = now
        for key in [k for k, q in self._hits.items() if not q or now - q[-1] >= window_s]:
            del self._hits[key]
