"""Public API: ingestion + job inspection."""
from __future__ import annotations

import hmac
import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response

from ..errors import ApiError
from ..logging_setup import log
from ..models import WebhookJob
from ..repository import Repository
from ..schemas import DispatchRequest, DispatchResponse, JobDetail, JobList, JobStatus
from ..security import validate_destination_url

logger = logging.getLogger("api")


def require_api_key(request: Request) -> None:
    """Optional shared-secret auth: enforced only when API_KEY is configured."""
    expected = request.app.state.settings.api_key
    if expected and not hmac.compare_digest(request.headers.get("x-api-key", ""), expected):
        raise ApiError(401, "unauthorized", "Missing or invalid X-API-Key header")


router = APIRouter(prefix="/api/webhooks", tags=["webhooks"], dependencies=[Depends(require_api_key)])


def _job_out(job: WebhookJob) -> dict:
    """Convert a database row into the JSON shape returned to API clients."""
    still_pending = job.status in ("queued", "retrying")
    return {
        "jobId": job.id,
        "eventId": job.event_id,
        "destinationUrl": job.destination_url,
        "status": job.status,
        "attempts": job.attempts,
        "maxAttempts": job.max_attempts,
        "nextAttemptAt": job.next_attempt_at if still_pending else None,
        "lastStatusCode": job.last_status_code,
        "lastError": job.last_error,
        "createdAt": job.created_at,
        "updatedAt": job.updated_at,
        "completedAt": job.completed_at,
    }


@router.post(
    "/dispatch",
    status_code=202,
    response_model=DispatchResponse,
    response_model_exclude_none=True,
    summary="Queue a webhook for asynchronous delivery",
)
async def dispatch(body: DispatchRequest, request: Request, response: Response) -> DispatchResponse:
    """Validates, persists the job, and returns **202 immediately**. Delivery happens in the background."""
    state = request.app.state
    settings = state.settings

    # Protect ingestion itself (per client IP, sliding window).
    client_ip = request.client.host if request.client else "unknown"
    allowed, retry_after = state.ingest_limiter.hit(client_ip, settings.ingest_rate_limit_per_min, 60.0)
    if not allowed:
        raise ApiError(
            429, "rate_limited", "Too many requests", headers={"Retry-After": str(int(retry_after) + 1)}
        )

    try:
        url, host = validate_destination_url(body.destinationUrl, allow_private=settings.allow_private_destinations)
    except ValueError as exc:
        raise ApiError(
            400, "validation_error", "Request validation failed",
            details=[{"field": "destinationUrl", "message": str(exc)}],
        ) from None

    repo: Repository = state.repo
    job, created = await repo.enqueue(
        event_id=body.eventId,
        destination_url=url,
        host=host,
        payload=body.payload,
        max_attempts=settings.max_attempts,
    )
    response.headers["Location"] = f"/api/webhooks/jobs/{job.id}"
    if created:
        log(logger, "job_queued", job_id=str(job.id), event_id=body.eventId, host=host)
        if state.worker is not None:
            state.worker.wake.set()  # skip the poll interval for in-process workers
        return DispatchResponse(status="queued", jobId=job.id)
    log(logger, "job_duplicate", job_id=str(job.id), event_id=body.eventId)
    return DispatchResponse(status=job.status, jobId=job.id, duplicate=True)


@router.get("/jobs/{job_id}", response_model=JobDetail, summary="Job status and full delivery log")
async def get_job(job_id: UUID, request: Request) -> dict:
    job = await request.app.state.repo.get_job(job_id)
    if job is None:
        raise ApiError(404, "not_found", f"No job with id {job_id}")
    deliveries = [
        {
            "attempt": d.attempt_number,
            "status": d.status,
            "responseCode": d.response_code,
            "error": d.error,
            "responseSnippet": d.response_snippet,
            "durationMs": d.duration_ms,
            "startedAt": d.started_at,
            "finishedAt": d.finished_at,
            "nextRetryAt": d.next_retry_at,
        }
        for d in job.deliveries
    ]
    return {**_job_out(job), "payload": job.payload, "deliveries": deliveries}


@router.get("/jobs", response_model=JobList, summary="List jobs, newest first")
async def list_jobs(
    request: Request,
    status: Annotated[JobStatus | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict:
    rows = await request.app.state.repo.list_jobs(status, limit, offset)
    return {"items": [_job_out(r) for r in rows], "limit": limit, "offset": offset}


@router.get("/stats", summary="Queue depth and counts by status")
async def stats(request: Request) -> dict:
    return await request.app.state.repo.stats()
