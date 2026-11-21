"""The service should recover from trouble by itself: sleepy hosts, database hiccups, a dead worker."""
import asyncio
import time

import httpx
import pytest
from sqlalchemy.exc import OperationalError

from app.db import create_db, create_tables
from app.keepalive import keep_awake
from app.config import Settings
from tests.conftest import LiveServer


def test_keepalive_pings_health_and_survives_failures():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if len(calls) == 2:
            raise httpx.ConnectError("network blip")  # one failed ping must not stop the loop
        return httpx.Response(200, json={"status": "ok"})

    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        task = asyncio.create_task(keep_awake("https://example.onrender.com/", 0.02, client))
        await asyncio.sleep(0.3)
        assert not task.done()  # still running after the failure
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await client.aclose()

    asyncio.run(run())
    assert len(calls) >= 4
    assert set(calls) == {"/health"}


def test_startup_gives_up_cleanly_when_database_never_comes_up():
    settings = Settings(database_url="postgresql://postgres@127.0.0.1:1/nothing")  # nothing listens on port 1
    engine, _ = create_db(settings)
    with pytest.raises((OperationalError, OSError)):
        asyncio.run(create_tables(engine, attempts=2, delay_s=0.01))


def test_health_turns_unhealthy_if_the_worker_dies():
    """Then the host's health check fails and Render restarts the service."""
    server = LiveServer().start()
    try:
        with httpx.Client(base_url=server.url, timeout=5) as api:
            assert api.get("/health").status_code == 200

            worker_task = server.app.state.worker_task
            worker_task.get_loop().call_soon_threadsafe(worker_task.cancel)  # simulate the worker crashing

            deadline = time.time() + 5
            while time.time() < deadline and api.get("/health").status_code == 200:
                time.sleep(0.05)
            response = api.get("/health")
            assert response.status_code == 503
            assert response.json()["worker"] == "stopped"
    finally:
        server.stop()
