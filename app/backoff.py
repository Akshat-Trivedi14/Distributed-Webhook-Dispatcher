"""Retry delay calculation."""
from __future__ import annotations

import random


def compute_backoff(
    failed_attempts: int,
    *,
    base_s: float,
    max_s: float,
    jitter: float = 0.0,
    retry_after_s: float | None = None,
) -> float:
    """Seconds to wait before the next attempt, after `failed_attempts` consecutive failures.

    Exponential: base * 2^(n-1)  ->  2s, 4s, 8s with the default base of 2.
    A server-provided Retry-After (429/503) is honoured when it asks for longer than our schedule.
    Optional jitter adds up to +jitter*100% to spread out retry storms.
    """
    delay = base_s * (2 ** max(failed_attempts - 1, 0))
    if retry_after_s is not None:
        delay = max(delay, retry_after_s)
    delay = min(delay, max_s)
    if jitter:
        delay += delay * jitter * random.random()
    return delay
