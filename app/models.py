"""Database tables, defined as SQLAlchemy ORM models.

Three tables:
  webhook_jobs        the queue. One row per webhook to deliver, with its current status.
  delivery_attempts   the delivery log. One row per HTTP attempt made for a job.
  domain_rate_limits  token-bucket state for the rate limiter, one row per destination host.

Job lifecycle:   queued -> processing -> succeeded
                              |-> retrying -> processing -> ... (up to 3 retries)
                              |-> failed
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Index, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

JOB_STATUSES = ("queued", "processing", "retrying", "succeeded", "failed")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    # Every `datetime` column is stored as timestamptz; every `dict` column as JSONB.
    type_annotation_map = {datetime: DateTime(timezone=True), dict[str, Any]: JSONB}


class WebhookJob(Base):
    __tablename__ = "webhook_jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4, server_default=func.gen_random_uuid())
    event_id: Mapped[str]
    destination_url: Mapped[str]
    host: Mapped[str]  # hostname of destination_url; the rate limiter groups by this
    payload: Mapped[dict[str, Any]]
    status: Mapped[str] = mapped_column(default="queued", server_default="queued")
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")  # attempts completed so far
    max_attempts: Mapped[int]
    next_attempt_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    locked_at: Mapped[datetime | None]  # when a worker claimed the job (while 'processing')
    locked_by: Mapped[str | None]  # which worker claimed it
    last_status_code: Mapped[int | None]
    last_error: Mapped[str | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now(), onupdate=utcnow)
    completed_at: Mapped[datetime | None]

    deliveries: Mapped[list["DeliveryAttempt"]] = relationship(
        back_populates="job", order_by="DeliveryAttempt.attempt_number", cascade="all, delete-orphan"
    )

    __table_args__ = (
        # Idempotency: the same event sent to the same destination is only queued once.
        UniqueConstraint("destination_url", "event_id", name="webhook_jobs_event_dedup"),
        CheckConstraint(f"status IN {JOB_STATUSES}", name="webhook_jobs_status_valid"),
        # Workers only ever look for due jobs, so index just those rows.
        Index("idx_webhook_jobs_due", "next_attempt_at", postgresql_where="status IN ('queued', 'retrying')"),
        Index("idx_webhook_jobs_status_created", "status", "created_at"),
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("webhook_jobs.id", ondelete="CASCADE"))
    attempt_number: Mapped[int]
    status: Mapped[str]  # 'success' | 'retrying' (failed, will retry) | 'failed' (gave up)
    response_code: Mapped[int | None]
    error: Mapped[str | None]
    response_snippet: Mapped[str | None]  # first bytes of the response body
    duration_ms: Mapped[int]
    started_at: Mapped[datetime]
    finished_at: Mapped[datetime] = mapped_column(default=utcnow)
    next_retry_at: Mapped[datetime | None]  # when the retry is scheduled, if status is 'retrying'

    job: Mapped[WebhookJob] = relationship(back_populates="deliveries")

    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="delivery_attempts_unique"),
        CheckConstraint("status IN ('success', 'retrying', 'failed')", name="delivery_attempts_status_valid"),
    )


class DomainRateLimit(Base):
    __tablename__ = "domain_rate_limits"

    host: Mapped[str] = mapped_column(primary_key=True)
    tokens: Mapped[float]  # tokens currently in the bucket
    updated_at: Mapped[datetime]  # when `tokens` was last calculated
