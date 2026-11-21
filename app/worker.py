"""Background worker: claims due jobs from Postgres and delivers them.

Runs either inside the API process (default) or standalone: `python -m app.worker`.
Run as many copies as you like; Postgres coordinates them (SKIP LOCKED + shared token buckets).
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .backoff import compute_backoff
from .config import Settings, get_settings
from .db import create_db, create_tables
from .delivery import DeliveryResult, build_client, deliver
from .logging_setup import configure_logging, log
from .models import WebhookJob
from .ratelimit import TokenBucket
from .repository import Repository

logger = logging.getLogger("worker")


class Worker:
    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], client: httpx.AsyncClient, settings: Settings
    ) -> None:
        self.repo = Repository(session_factory)
        self.bucket = TokenBucket(session_factory, settings.rate_limit_per_sec, settings.rate_limit_burst)
        self.client = client
        self.settings = settings
        self.worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.wake = asyncio.Event()  # set on enqueue / task completion to skip the poll sleep
        self._tasks: set[asyncio.Task[None]] = set()
        self._inflight_per_host: defaultdict[str, int] = defaultdict(int)
        self._stopping = asyncio.Event()

    @property
    def running(self) -> bool:
        return not self._stopping.is_set()

    # ------------------------------------------------------------------ lifecycle
    async def run(self) -> None:
        log(logger, "worker_started", worker_id=self.worker_id, concurrency=self.settings.worker_concurrency)
        reaper = asyncio.create_task(self._reaper_loop(), name="reaper")
        try:
            while not self._stopping.is_set():
                free = self.settings.worker_concurrency - len(self._tasks)
                claimed = 0
                if free > 0:
                    try:
                        jobs = await self.repo.claim(free, self.worker_id)
                    except Exception:
                        logger.exception("claim_failed")  # e.g. transient DB outage; keep polling
                        jobs = []
                    claimed = len(jobs)
                    for job in jobs:
                        task = asyncio.create_task(self._process(job))
                        self._tasks.add(task)
                        task.add_done_callback(self._on_task_done)
                if claimed and claimed == free:
                    await asyncio.sleep(0)  # backlog likely: poll again immediately
                    continue
                await self._wait_for_work()
        finally:
            reaper.cancel()
            await self._drain()
            log(logger, "worker_stopped", worker_id=self.worker_id)

    def stop(self) -> None:
        self._stopping.set()
        self.wake.set()

    async def _wait_for_work(self) -> None:
        try:
            await asyncio.wait_for(self.wake.wait(), timeout=self.settings.poll_interval_s)
        except TimeoutError:
            pass
        self.wake.clear()

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        self.wake.set()  # a slot freed up

    async def _drain(self) -> None:
        """Graceful shutdown: let in-flight deliveries finish, then release whatever is left."""
        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=self.settings.request_timeout_s + 2)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        try:
            released = await self.repo.release_worker_jobs(self.worker_id)
            if released:
                log(logger, "jobs_released", worker_id=self.worker_id, count=released)
        except Exception:
            logger.exception("release_failed")  # the lease reaper will recover them anyway

    async def _reaper_loop(self) -> None:
        while True:
            try:
                n = await self.repo.reap_expired_leases(self.settings.lease_timeout_s)
                if n:
                    log(logger, "leases_reaped", count=n, level=logging.WARNING)
            except Exception:
                logger.exception("reap_failed")
            await asyncio.sleep(self.settings.reaper_interval_s)

    # ------------------------------------------------------------------ one job
    async def _process(self, job: WebhookJob) -> None:
        job_id, host = job.id, job.host
        try:
            # 1) Protect the destination: simultaneous-request cap, then the shared token bucket.
            #    The slot is reserved *before* any await so concurrent tasks can't all slip past the cap.
            if self._inflight_per_host.get(host, 0) >= self.settings.max_inflight_per_host:
                await self._defer(job, 0.25, "host_concurrency_cap")
                return
            self._inflight_per_host[host] += 1
            try:
                wait = await self.bucket.acquire(host)
                if wait > 0:
                    await self._defer(job, wait, "rate_limited")
                    return

                # 2) Deliver.
                attempt_number = job.attempts + 1
                started_at = datetime.now(timezone.utc)
                result = await deliver(
                    self.client,
                    self.settings,
                    job_id=str(job_id),
                    event_id=job.event_id,
                    url=job.destination_url,
                    payload=job.payload,
                    attempt_number=attempt_number,
                )
            finally:
                self._inflight_per_host[host] -= 1
                if self._inflight_per_host[host] <= 0:
                    del self._inflight_per_host[host]

            # 3) Decide what happens next and persist it.
            await self._settle(job, attempt_number, started_at, result)
        except asyncio.CancelledError:
            raise  # shutdown: _drain() releases the lease
        except Exception:
            # Don't strand the job in 'processing' on a bug/DB hiccup: the lease reaper recovers it.
            logger.exception("process_failed", extra={"fields": {"job_id": str(job_id)}})

    async def _defer(self, job: WebhookJob, delay_s: float, reason: str) -> None:
        await self.repo.defer(job.id, self.worker_id, delay_s)
        log(logger, "delivery_deferred", job_id=str(job.id), host=job.host, reason=reason, delay_s=round(delay_s, 3))

    async def _settle(
        self, job: WebhookJob, attempt_number: int, started_at: datetime, result: DeliveryResult
    ) -> None:
        s = self.settings
        retry_delay = 0.0
        if result.ok:
            outcome = "succeeded"
        elif result.retryable and attempt_number < job.max_attempts:
            outcome = "retrying"
            retry_delay = compute_backoff(
                attempt_number,
                base_s=s.backoff_base_s,
                max_s=s.backoff_max_s,
                jitter=s.backoff_jitter,
                retry_after_s=result.retry_after_s,
            )
        else:
            outcome = "failed"

        saved = await self.repo.record_attempt(
            job_id=job.id,
            worker_id=self.worker_id,
            attempt_number=attempt_number,
            new_status=outcome,
            response_code=result.status_code,
            error=result.error,
            snippet=result.snippet,
            duration_ms=result.duration_ms,
            started_at=started_at,
            retry_delay_s=retry_delay,
        )
        log(
            logger,
            "delivery_attempt",
            level=logging.INFO if outcome != "failed" else logging.WARNING,
            job_id=str(job.id),
            event_id=job.event_id,
            host=job.host,
            attempt=attempt_number,
            max_attempts=job.max_attempts,
            outcome=outcome,
            status_code=result.status_code,
            error=result.error,
            duration_ms=result.duration_ms,
            retry_in_s=round(retry_delay, 3) if outcome == "retrying" else None,
            lease_lost=None if saved else True,
        )


async def _amain() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine, session_factory = create_db(settings)
    await create_tables(engine)
    client = build_client(settings)
    worker = Worker(session_factory, client, settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        await client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(_amain())
