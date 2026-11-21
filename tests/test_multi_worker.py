"""Two separate workers sharing one database must never deliver the same job twice."""
from collections import Counter

import httpx

from tests.conftest import LiveServer, dispatch, new_event_id, wait_for_job


def test_two_workers_share_the_queue_without_duplicates():
    server_a = LiveServer().start()
    server_b = LiveServer().start()  # a second instance: its own API and its own worker
    try:
        with httpx.Client(base_url=server_a.url, timeout=10) as api:
            api.delete("/mock/receiver/log")  # forget deliveries recorded by earlier tests
            # Every delivery is sent to server A's mock receiver, no matter which worker sends it.
            event_ids = [new_event_id() for _ in range(30)]
            job_ids = [
                dispatch(api, f"{server_a.url}/mock/receiver", event_id).json()["jobId"] for event_id in event_ids
            ]
            for job_id in job_ids:
                assert wait_for_job(api, job_id)["status"] == "succeeded"

            received = api.get("/mock/receiver/log", params={"limit": 500}).json()["items"]
            deliveries_per_event = Counter(item["eventId"] for item in received)
            assert set(deliveries_per_event) == set(event_ids)
            assert set(deliveries_per_event.values()) == {1}, "a job was delivered more than once"
    finally:
        server_a.stop()
        server_b.stop()
