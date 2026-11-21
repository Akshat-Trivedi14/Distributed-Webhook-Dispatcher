"""Requirement 3: the dispatcher must not flood a single destination."""
from datetime import datetime

import httpx
import pytest

from tests.conftest import LiveServer, dispatch, new_event_id, wait_for_job


@pytest.fixture(scope="module")
def slow_server():
    # Allow only 2 deliveries per second to any one host, with a bucket of 2.
    srv = LiveServer(rate_limit_per_sec=2, rate_limit_burst=2, max_inflight_per_host=10).start()
    yield srv
    srv.stop()


def test_deliveries_to_one_host_are_spread_out(slow_server):
    with httpx.Client(base_url=slow_server.url, timeout=10) as api:
        key = new_event_id()  # one shared key lets us read all deliveries back from the mock's log
        destination = f"{slow_server.url}/mock/receiver?key={key}"
        job_ids = [dispatch(api, destination).json()["jobId"] for _ in range(6)]  # a burst of 6

        jobs = [wait_for_job(api, job_id) for job_id in job_ids]

        # Being rate limited is not a failure: every job succeeds on its FIRST attempt.
        assert all(j["status"] == "succeeded" and j["attempts"] == 1 for j in jobs)

        received = api.get("/mock/receiver/log", params={"key": key}).json()["items"]
        assert len(received) == 6
        times = sorted(item["receivedAt"] for item in received)
        seconds = [datetime.fromisoformat(t).timestamp() for t in times]
        # 2 go out immediately (the burst), the other 4 follow at 2 per second: that takes ~2 seconds.
        assert seconds[-1] - seconds[0] >= 1.5
        # And no one-second window ever contains more than burst + rate = 4 requests.
        for i, start in enumerate(seconds):
            assert sum(1 for t in seconds[i:] if t < start + 1.0) <= 4
