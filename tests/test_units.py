"""Fast tests for the small pure pieces: backoff, limiter, URL safety, Retry-After parsing."""
import asyncio

import pytest

from app.backoff import compute_backoff
from app.delivery import parse_retry_after
from app.ratelimit import SlidingWindowLimiter
from app.security import BlockedDestination, check_host_is_public, validate_destination_url


# ---- backoff
def test_backoff_is_2_4_8_seconds():
    delays = [compute_backoff(n, base_s=2, max_s=300) for n in (1, 2, 3)]
    assert delays == [2, 4, 8]


def test_backoff_respects_retry_after_and_cap():
    assert compute_backoff(1, base_s=2, max_s=300, retry_after_s=30) == 30  # server asked for longer
    assert compute_backoff(1, base_s=2, max_s=300, retry_after_s=1) == 2  # never shorter than our schedule
    assert compute_backoff(3, base_s=2, max_s=5) == 5  # capped


def test_backoff_jitter_only_adds_time():
    for _ in range(50):
        assert 2 <= compute_backoff(1, base_s=2, max_s=300, jitter=0.5) <= 3


# ---- Retry-After header
def test_parse_retry_after():
    assert parse_retry_after("7") == 7
    assert parse_retry_after(None) is None
    assert parse_retry_after("garbage") is None
    assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0  # a date in the past


# ---- sliding window limiter
def test_sliding_window_blocks_after_limit():
    limiter = SlidingWindowLimiter()
    results = [limiter.hit("ip", limit=3, window_s=60)[0] for _ in range(5)]
    assert results == [True, True, True, False, False]
    assert limiter.hit("other-ip", limit=3, window_s=60)[0] is True  # separate key, separate budget


# ---- URL validation (SSRF protection)
@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/x",
        "javascript:alert(1)",
        "http://",
        "https://user:pass@example.com/",
        "http://localhost/hook",
        "http://127.0.0.1/hook",
        "http://10.0.0.5/hook",
        "http://192.168.1.1/hook",
        "http://169.254.169.254/latest/meta-data",  # cloud metadata service
        "http://[::1]/hook",
        "http://service.internal/hook",
    ],
)
def test_unsafe_urls_are_rejected(url):
    with pytest.raises(ValueError):
        validate_destination_url(url, allow_private=False)


def test_public_url_is_accepted_and_host_extracted():
    url, host = validate_destination_url("https://Hooks.Example.com:8443/a/b?x=1", allow_private=False)
    assert host == "hooks.example.com"


def test_private_urls_allowed_only_when_enabled():
    validate_destination_url("http://127.0.0.1:9000/x", allow_private=True)


def test_runtime_check_blocks_hosts_resolving_to_private_ips():
    with pytest.raises(BlockedDestination):
        asyncio.run(check_host_is_public("localhost", 80, allow_private=False))
    asyncio.run(check_host_is_public("localhost", 80, allow_private=True))  # allowed in dev mode
