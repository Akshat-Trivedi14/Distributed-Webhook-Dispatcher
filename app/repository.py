"""Every database operation the app needs, in one place.

The queue is the `webhook_jobs` table. Workers claim rows with `SELECT ... FOR UPDATE SKIP LOCKED`,
so many workers can poll the same table without blocking each other or delivering a job twice.

A claim is a *lease*: `locked_by` / `locked_at` say who is working on the job. Every update checks the
lease first, so a worker that stalled and lost its job cannot overwrite the new owner's progress.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from .models import JOB_STATUSES, DeliveryAttempt, WebhookJob, utcnow

DUE_STATUSES = ("queued", "retrying")


def _give_back(job: WebhookJob, when: datetime) -> None:
    """Release a claimed job so another worker can pick it up at time `when`."""
    job.status = "queued" if job.attempts == 0 else "retrying"
    job.next_attempt_at = when
    job.locked_at = None
    job.locked_by = None


class Repository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    # ------------------------------------------------------------------ ingestion
    async def enqueue(
        self, *, event_id: str, destination_url: str, host: str, payload: dict[str, Any], max_attempts: int
    ) -> tuple[WebhookJob, bool]:
        """Add a job to the queue. Returns (job, created).

        If this event was already queued for this destination, nothing is added and the existing job
        is returned with created=False. The database's unique constraint makes this safe under races.
        """
        job = WebhookJob(
            event_id=event_id, destination_url=destination_url, host=host, payload=payload, max_attempts=max_attempts
        )
        async with self.session_factory() as session:
            session.add(job)
            try:
                await session.commit()
                return job, True
            except IntegrityError:
                await session.rollback()
            existing = await session.scalar(
                select(WebhookJob).where(
                    WebhookJob.destination_url == destination_url, WebhookJob.event_id == event_id
                )
            )
            return existing, False

    # ------------------------------------------------------------------ worker side
    async def claim(self, limit: int, worker_id: str) -> list[WebhookJob]:
        """Take up to `limit` jobs that are due, and mark them as being processed by this worker."""
        now = utcnow()
        async with self.session_factory() as session, session.begin():
            due_jobs = await session.scalars(
                select(WebhookJob)
                .where(WebhookJob.status.in_(DUE_STATUSES), WebhookJob.next_attempt_at <= now)
                .order_by(WebhookJob.next_attempt_at)
                .limit(limit)
                .with_for_update(skip_locked=True)  # skip rows another worker is claiming right now
            )
            jobs = list(due_jobs)
            for job in jobs:
                job.status = "processing"
                job.locked_at = now
                job.locked_by = worker_id
        return jobs

    async def _get_leased_job(self, session: AsyncSession, job_id: UUID, worker_id: str) -> WebhookJob | None:
        """Fetch (and row-lock) a job, but only if `worker_id` still holds its lease."""
        job = await session.get(WebhookJob, job_id, with_for_update=True)
        if job is None or job.status != "processing" or job.locked_by != worker_id:
            return None
        return job

    async def defer(self, job_id: UUID, worker_id: str, delay_s: float) -> None:
        """Put a claimed job back for later WITHOUT counting an attempt.

        Used when the rate limiter says "not yet": the destination never failed, so the job
        keeps all its retries.
        """
        async with self.session_factory() as session, session.begin():
            job = await self._get_leased_job(session, job_id, worker_id)
            if job is not None:
                _give_back(job, utcnow() + timedelta(seconds=delay_s))

    async def record_attempt(
        self,
        *,
        job_id: UUID,
        worker_id: str,
        attempt_number: int,
        new_status: str,  # 'succeeded', 'retrying' or 'failed'
        response_code: int | None,
        error: str | None,
        snippet: str | None,
        duration_ms: int,
        started_at: datetime,
        retry_delay_s: float = 0.0,
    ) -> bool:
        """Save the delivery log row and update the job together. Returns False if the lease was lost."""
        async with self.session_factory() as session, session.begin():
            job = await self._get_leased_job(session, job_id, worker_id)
            if job is None:
                return False

            now = utcnow()
            job.status = new_status
            job.attempts = attempt_number
            job.last_status_code = response_code
            job.last_error = error
            job.locked_at = None
            job.locked_by = None
            if new_status == "retrying":
                job.next_attempt_at = now + timedelta(seconds=retry_delay_s)
            else:
                job.completed_at = now

            log_status = {"succeeded": "success", "retrying": "retrying", "failed": "failed"}[new_status]
            session.add(
                DeliveryAttempt(
                    job_id=job_id,
                    attempt_number=attempt_number,
                    status=log_status,
                    response_code=response_code,
                    error=error,
                    response_snippet=snippet,
                    duration_ms=duration_ms,
                    started_at=started_at,
                    finished_at=now,
                    next_retry_at=job.next_attempt_at if new_status == "retrying" else None,
                )
            )
            return True

    async def reap_expired_leases(self, lease_timeout_s: float) -> int:
        """Recover jobs whose worker died mid-delivery (crash, deploy, out of memory).

        Their status is stuck at 'processing'. Once the lease is older than the timeout we make them
        due again, so no accepted webhook is ever lost (delivery is "at least once").
        """
        now = utcnow()
        async with self.session_factory() as session, session.begin():
            stuck = await session.scalars(
                select(WebhookJob)
                .where(WebhookJob.status == "processing", WebhookJob.locked_at < now - timedelta(seconds=lease_timeout_s))
                .with_for_update(skip_locked=True)
            )
            count = 0
            for job in stuck:
                _give_back(job, now)
                count += 1
            return count

    async def release_worker_jobs(self, worker_id: str) -> int:
        """On graceful shutdown, hand back any jobs this worker still holds so others pick them up."""
        now = utcnow()
        async with self.session_factory() as session, session.begin():
            held = await session.scalars(
                select(WebhookJob).where(WebhookJob.status == "processing", WebhookJob.locked_by == worker_id)
            )
            count = 0
            for job in held:
                _give_back(job, now)
                count += 1
            return count

    # ------------------------------------------------------------------ read side
    async def get_job(self, job_id: UUID) -> WebhookJob | None:
        """A job together with its delivery log."""
        async with self.session_factory() as session:
            return await session.scalar(
                select(WebhookJob).where(WebhookJob.id == job_id).options(selectinload(WebhookJob.deliveries))
            )

    async def list_jobs(self, status: str | None, limit: int, offset: int) -> list[WebhookJob]:
        query = select(WebhookJob).order_by(WebhookJob.created_at.desc()).limit(limit).offset(offset)
        if status:
            query = query.where(WebhookJob.status == status)
        async with self.session_factory() as session:
            return list(await session.scalars(query))

    async def stats(self) -> dict[str, Any]:
        async with self.session_factory() as session:
            rows = await session.execute(select(WebhookJob.status, func.count()).group_by(WebhookJob.status))
            jobs = {status: 0 for status in JOB_STATUSES}
            jobs.update({status: count for status, count in rows})

            oldest_due = await session.scalar(
                select(func.min(WebhookJob.next_attempt_at)).where(
                    WebhookJob.status.in_(DUE_STATUSES), WebhookJob.next_attempt_at <= utcnow()
                )
            )
        lag = (utcnow() - oldest_due).total_seconds() if oldest_due else 0.0
        return {"jobs": jobs, "total": sum(jobs.values()), "oldestDueJobAgeSeconds": round(lag, 3)}

    async def ping(self) -> None:
        async with self.session_factory() as session:
            await session.execute(select(1))
