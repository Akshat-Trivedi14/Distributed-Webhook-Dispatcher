"""Request/response models (also drive the OpenAPI docs at /docs)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

JobStatus = Literal["queued", "processing", "retrying", "succeeded", "failed"]


class DispatchRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "destinationUrl": "https://target-server.com/endpoint",
                "eventId": "evt_123456",
                "payload": {"event": "user.signup", "userId": "usr_999"},
            }
        }
    )

    destinationUrl: str = Field(description="http(s) endpoint that will receive the POST")
    eventId: str = Field(min_length=1, max_length=255, description="Caller's unique id for the event (idempotency key)")
    payload: dict[str, Any] = Field(description="JSON object delivered as the request body")

    @field_validator("eventId")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("eventId must not be blank")
        return v


class DispatchResponse(BaseModel):
    status: JobStatus
    jobId: UUID
    duplicate: bool | None = Field(None, description="true when this (destinationUrl, eventId) was already queued")


class DeliveryAttempt(BaseModel):
    attempt: int
    status: Literal["success", "failed", "retrying"]
    responseCode: int | None
    error: str | None
    responseSnippet: str | None
    durationMs: int
    startedAt: datetime
    finishedAt: datetime
    nextRetryAt: datetime | None


class Job(BaseModel):
    jobId: UUID
    eventId: str
    destinationUrl: str
    status: JobStatus
    attempts: int
    maxAttempts: int
    nextAttemptAt: datetime | None
    lastStatusCode: int | None
    lastError: str | None
    createdAt: datetime
    updatedAt: datetime
    completedAt: datetime | None


class JobDetail(Job):
    payload: dict[str, Any]
    deliveries: list[DeliveryAttempt]


class JobList(BaseModel):
    items: list[Job]
    limit: int
    offset: int
