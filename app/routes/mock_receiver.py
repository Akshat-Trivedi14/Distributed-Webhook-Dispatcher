"""A configurable fake customer endpoint, so the whole pipeline can be exercised end-to-end.

Point `destinationUrl` at  <this-service>/mock/receiver  and steer its behaviour with query params:

  status=500        always answer with this HTTP status (default 200)
  fail_first=2      answer 503 to the first N requests per key, then succeed  (tests retry/backoff)
  delay_ms=10000    sleep before answering                                      (tests non-blocking ingest)
  rate_limit=3      sliding window: at most N requests per 10s per key, else 429 + Retry-After
  key=abc           state key for fail_first/rate_limit (default: the X-Webhook-Event-Id header)

State is in-memory (per process) and is for demos/tests only.
"""
from __future__ import annotations

import asyncio
import json
import math
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Query, Request, Response

from ..delivery import sign
from ..ratelimit import SlidingWindowLimiter

router = APIRouter(prefix="/mock/receiver", tags=["mock receiver"])

_MAX_DELAY_MS = 30_000
_RATE_WINDOW_S = 10.0
_hits: defaultdict[str, int] = defaultdict(int)
_limiter = SlidingWindowLimiter()
_log: deque[dict[str, Any]] = deque(maxlen=500)


@router.post("", summary="Fake webhook target with scriptable failures")
@router.post("/", include_in_schema=False)
async def receive(
    request: Request,
    status: Annotated[int, Query(ge=100, le=599)] = 200,
    fail_first: Annotated[int, Query(ge=0, le=100)] = 0,
    delay_ms: Annotated[int, Query(ge=0, le=_MAX_DELAY_MS)] = 0,
    rate_limit: Annotated[int | None, Query(ge=1, le=1000)] = None,
    key: Annotated[str | None, Query(max_length=255)] = None,
) -> Response:
    raw = await request.body()
    event_id = request.headers.get("x-webhook-event-id", "")
    state_key = key or event_id or "default"
    received_at = datetime.now(timezone.utc)

    _hits[state_key] += 1
    n = _hits[state_key]

    if delay_ms:
        await asyncio.sleep(delay_ms / 1000)

    headers: dict[str, str] = {}
    allowed, retry_after = (True, 0.0) if rate_limit is None else _limiter.hit(state_key, rate_limit, _RATE_WINDOW_S)
    if not allowed:
        code, headers = 429, {"Retry-After": str(max(1, math.ceil(retry_after)))}
    elif n <= fail_first:
        code = 503
    else:
        code = status

    secret = request.app.state.settings.signing_secret
    sig = request.headers.get("x-webhook-signature")
    signature_valid = None
    if secret and sig:
        signature_valid = sig == sign(secret, request.headers.get("x-webhook-timestamp", ""), raw)

    try:
        body: Any = json.loads(raw) if raw else None
    except ValueError:
        body = raw.decode("utf-8", errors="replace")
    _log.append(
        {
            "receivedAt": received_at,
            "key": state_key,
            "eventId": event_id or None,
            "attempt": request.headers.get("x-webhook-attempt"),
            "requestNumber": n,
            "respondedWith": code,
            "signatureValid": signature_valid,
            "body": body,
        }
    )
    return Response(
        content=json.dumps({"received": code < 400, "requestNumber": n}),
        status_code=code,
        media_type="application/json",
        headers=headers,
    )


@router.get("/log", summary="Recent requests this mock has received (newest last)")
async def get_log(key: str | None = None, limit: Annotated[int, Query(ge=1, le=500)] = 100) -> dict:
    items = [e for e in _log if key is None or e["key"] == key or e["eventId"] == key]
    return {"count": len(items), "items": items[-limit:]}


@router.delete("/log", summary="Reset the mock's log and counters")
async def reset_log() -> dict:
    _log.clear()
    _hits.clear()
    return {"cleared": True}

