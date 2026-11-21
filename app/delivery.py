"""Outbound HTTP delivery: one POST to the customer's endpoint, classified into a `DeliveryResult`."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from .config import Settings
from .security import BlockedDestination, check_host_is_public

USER_AGENT = "WebhookDispatcher/1.0"
_SNIPPET_BYTES = 512


@dataclass
class DeliveryResult:
    ok: bool
    retryable: bool
    status_code: int | None
    error: str | None
    duration_ms: int
    snippet: str | None = None
    retry_after_s: float | None = None


def build_client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(settings.request_timeout_s),
        follow_redirects=False,  # a webhook target must answer directly; redirects can bypass our checks
        trust_env=False,
    )


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return f"sha256={mac.hexdigest()}"


def parse_retry_after(value: str | None) -> float | None:
    """Retry-After is either delta-seconds or an HTTP date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max((dt - datetime.now(timezone.utc)).total_seconds(), 0.0)


def _is_retryable_status(code: int) -> bool:
    # 5xx = their server is unwell; 429 = they asked us to slow down; 408 = request timeout.
    # Any other 4xx (400/401/403/404/410/422...) will not change on retry, so we fail fast.
    return code >= 500 or code in (408, 429)


async def deliver(
    client: httpx.AsyncClient,
    settings: Settings,
    *,
    job_id: str,
    event_id: str,
    url: str,
    payload: dict[str, Any],
    attempt_number: int,
) -> DeliveryResult:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "X-Webhook-Id": job_id,
        "X-Webhook-Event-Id": event_id,
        "X-Webhook-Attempt": str(attempt_number),
        "X-Webhook-Timestamp": timestamp,
    }
    if settings.signing_secret:
        headers["X-Webhook-Signature"] = sign(settings.signing_secret, timestamp, body)

    started = time.perf_counter()

    def elapsed_ms() -> int:
        return int((time.perf_counter() - started) * 1000)

    try:
        # httpx timeouts are per-phase; this bounds the *whole* attempt (connect + send + read).
        async with asyncio.timeout(settings.request_timeout_s):
            parts = urlsplit(url)
            default_port = 443 if parts.scheme == "https" else 80
            await check_host_is_public(
                parts.hostname, parts.port or default_port, allow_private=settings.allow_private_destinations
            )
            async with client.stream("POST", url, content=body, headers=headers) as resp:
                chunks, size = [], 0
                async for chunk in resp.aiter_bytes():
                    chunks.append(chunk)
                    size += len(chunk)
                    if size >= _SNIPPET_BYTES:
                        break  # never download an unbounded response body
                snippet = b"".join(chunks)[:_SNIPPET_BYTES].decode("utf-8", errors="replace") or None
                code = resp.status_code
                retry_after = parse_retry_after(resp.headers.get("retry-after"))
    except (TimeoutError, httpx.TimeoutException):
        return DeliveryResult(False, True, None, f"timeout after {settings.request_timeout_s:g}s", elapsed_ms())
    except BlockedDestination as exc:
        return DeliveryResult(False, False, None, f"blocked destination: {exc}", elapsed_ms())
    except (httpx.HTTPError, ConnectionError, OSError) as exc:
        return DeliveryResult(False, True, None, f"{type(exc).__name__}: {exc}"[:500], elapsed_ms())
    except Exception as exc:  # never let one bad response kill the worker
        return DeliveryResult(False, True, None, f"unexpected {type(exc).__name__}: {exc}"[:500], elapsed_ms())

    if 200 <= code < 300:
        return DeliveryResult(True, False, code, None, elapsed_ms(), snippet)
    return DeliveryResult(
        False, _is_retryable_status(code), code, f"HTTP {code}", elapsed_ms(), snippet, retry_after
    )
