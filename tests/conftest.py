"""Test helpers.

The tests start the *real* app (API + worker) on a local port, backed by a real Postgres, and talk
to it over HTTP. The built-in mock receiver plays the role of the customer's server.

Needs a Postgres database; set TEST_DATABASE_URL (default: a local scratch DB on port 54329).
WARNING: the tables in that database are emptied before each test.
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import threading
import time
import uuid

import asyncpg
import httpx
import pytest
import uvicorn

from app.config import Settings
from app.main import create_app

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "postgresql://postgres@localhost:54329/webhooks_test")


def run_sql(sql: str, *args):
    """Run one SQL statement against the test database (used to reset and inspect state)."""

    async def _run():
        conn = await asyncpg.connect(TEST_DATABASE_URL)
        try:
            return await conn.fetch(sql, *args)
        finally:
            await conn.close()

    return asyncio.run(_run())


class LiveServer:
    """Runs the FastAPI app in a background thread so tests can call it like a deployed service."""

    def __init__(self, **setting_overrides):
        defaults = dict(
            database_url=TEST_DATABASE_URL,
            allow_private_destinations=True,  # tests deliver to localhost
            backoff_base_s=0.1,  # retries wait 0.1s, 0.2s, 0.4s instead of 2s, 4s, 8s
            request_timeout_s=1.0,
            rate_limit_per_sec=1000,  # effectively off, unless a test turns it on
            rate_limit_burst=1000,
            poll_interval_s=0.1,
            reaper_interval_s=0.5,
            log_level="WARNING",
        )
        self.settings = Settings(**{**defaults, **setting_overrides})
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(create_app(self.settings), host="127.0.0.1", port=self.port, log_level="warning")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self) -> "LiveServer":
        self._thread.start()
        deadline = time.time() + 10
        while not self._server.started:
            if time.time() > deadline:
                raise RuntimeError("test server did not start")
            time.sleep(0.05)
        return self

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=15)


@pytest.fixture(autouse=True)
def clean_database():
    """Every test starts with an empty queue."""
    try:
        run_sql("TRUNCATE webhook_jobs, delivery_attempts, domain_rate_limits CASCADE")
    except asyncpg.UndefinedTableError:
        pass  # first run: the server creates the tables when it starts


@pytest.fixture(scope="module")
def server():
    srv = LiveServer(signing_secret="test-secret").start()
    yield srv
    srv.stop()


@pytest.fixture
def api(server):
    with httpx.Client(base_url=server.url, timeout=10) as client:
        yield client


# ---- small helpers shared by the test files -------------------------------------------------
def new_event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:10]}"


def dispatch(api: httpx.Client, destination: str, event_id: str | None = None, payload: dict | None = None):
    return api.post(
        "/api/webhooks/dispatch",
        json={
            "destinationUrl": destination,
            "eventId": event_id or new_event_id(),
            "payload": payload or {"event": "user.signup", "userId": "usr_999"},
        },
    )


def wait_for_job(api: httpx.Client, job_id: str, statuses=("succeeded", "failed"), timeout: float = 15) -> dict:
    """Poll the job until it reaches one of `statuses`."""
    deadline = time.time() + timeout
    job = {}
    while time.time() < deadline:
        job = api.get(f"/api/webhooks/jobs/{job_id}").json()
        if job["status"] in statuses:
            return job
        time.sleep(0.05)
    raise AssertionError(f"job did not reach {statuses} in {timeout}s; last state: {json.dumps(job, default=str)}")
