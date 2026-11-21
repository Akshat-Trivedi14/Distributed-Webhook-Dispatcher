"""End-to-end behaviour of the ingestion endpoint, the worker, retries and the delivery log."""
import time
from datetime import datetime

from tests.conftest import dispatch, new_event_id, run_sql, wait_for_job


def mock_url(server, query: str = "") -> str:
    return f"{server.url}/mock/receiver{query}"


# ------------------------------------------------------------------ requirement 1: ingestion
def test_returns_202_with_job_id(api, server):
    resp = dispatch(api, mock_url(server))
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"
    assert body["jobId"]


def test_rejects_invalid_requests_with_400(api):
    bad_bodies = [
        {},  # everything missing
        {"destinationUrl": "not-a-url", "eventId": "e1", "payload": {}},
        {"destinationUrl": "ftp://example.com/x", "eventId": "e1", "payload": {}},
        {"destinationUrl": "https://example.com", "eventId": "", "payload": {}},
        {"destinationUrl": "https://example.com", "eventId": "e1", "payload": "not-an-object"},
        {"destinationUrl": "https://example.com", "eventId": "e1"},
    ]
    for body in bad_bodies:
        resp = api.post("/api/webhooks/dispatch", json=body)
        assert resp.status_code == 400, body
        assert resp.json()["error"] == "validation_error"


def test_rejects_malformed_json(api):
    resp = api.post("/api/webhooks/dispatch", content="{oops", headers={"content-type": "application/json"})
    assert resp.status_code == 400


def test_same_event_to_same_destination_is_queued_once(api, server):
    url, event_id = mock_url(server), new_event_id()
    first = dispatch(api, url, event_id).json()
    second = dispatch(api, url, event_id).json()
    assert second["jobId"] == first["jobId"]
    assert second["duplicate"] is True


# ------------------------------------------------------------------ non-blocking architecture
def test_ingestion_does_not_wait_for_slow_destination(api, server):
    started = time.perf_counter()
    resp = dispatch(api, mock_url(server, "?delay_ms=700"))  # destination takes 0.7s to answer
    elapsed = time.perf_counter() - started

    assert resp.status_code == 202
    assert elapsed < 0.3, f"ingestion blocked for {elapsed:.2f}s"
    job_id = resp.json()["jobId"]
    assert api.get(f"/api/webhooks/jobs/{job_id}").json()["status"] != "succeeded"  # still in flight
    assert wait_for_job(api, job_id)["status"] == "succeeded"  # ...and it completes in the background


# ------------------------------------------------------------------ requirement 2: worker + retries
def test_delivers_payload_to_destination(api, server):
    event_id = new_event_id()
    payload = {"event": "user.signup", "userId": "usr_999"}
    job_id = dispatch(api, mock_url(server), event_id, payload).json()["jobId"]

    job = wait_for_job(api, job_id)
    assert job["status"] == "succeeded"
    assert job["attempts"] == 1

    received = api.get("/mock/receiver/log", params={"key": event_id}).json()["items"]
    assert len(received) == 1
    assert received[0]["body"] == payload
    assert received[0]["signatureValid"] is True  # HMAC signature header checks out


def test_retries_5xx_then_succeeds(api, server):
    job_id = dispatch(api, mock_url(server, "?fail_first=2")).json()["jobId"]  # 503, 503, then 200

    job = wait_for_job(api, job_id)
    assert job["status"] == "succeeded"
    assert job["attempts"] == 3
    assert [d["status"] for d in job["deliveries"]] == ["retrying", "retrying", "success"]
    assert [d["responseCode"] for d in job["deliveries"]] == [503, 503, 200]


def test_backoff_doubles_between_attempts(api, server):
    job_id = dispatch(api, mock_url(server, "?status=500")).json()["jobId"]
    job = wait_for_job(api, job_id)

    # The test server uses a 0.1s base, so the waits are 0.1s, 0.2s, 0.4s (production: 2s, 4s, 8s).
    def ts(value: str) -> datetime:
        return datetime.fromisoformat(value)

    waits = [(ts(d["nextRetryAt"]) - ts(d["finishedAt"])).total_seconds() for d in job["deliveries"][:3]]
    for actual, expected in zip(waits, [0.1, 0.2, 0.4]):
        assert abs(actual - expected) < 0.02

    # ...and the next attempt really did wait that long before starting.
    for earlier, later in zip(job["deliveries"], job["deliveries"][1:]):
        assert ts(later["startedAt"]) >= ts(earlier["nextRetryAt"])


def test_gives_up_after_three_retries(api, server):
    job_id = dispatch(api, mock_url(server, "?status=500")).json()["jobId"]
    job = wait_for_job(api, job_id)

    assert job["status"] == "failed"
    assert job["attempts"] == 4  # 1 initial attempt + 3 retries
    assert [d["status"] for d in job["deliveries"]] == ["retrying", "retrying", "retrying", "failed"]
    assert job["lastStatusCode"] == 500


def test_retries_429_rate_limit_responses(api, server):
    job_id = dispatch(api, mock_url(server, "?status=429")).json()["jobId"]
    job = wait_for_job(api, job_id)
    assert job["attempts"] == 4
    assert job["lastStatusCode"] == 429


def test_retries_timeouts(api, server):
    # Server timeout is 1s; the destination takes 3s, so every attempt times out.
    job_id = dispatch(api, mock_url(server, "?delay_ms=3000")).json()["jobId"]
    job = wait_for_job(api, job_id, timeout=20)
    assert job["status"] == "failed"
    assert job["attempts"] == 4
    assert all("timeout" in d["error"] for d in job["deliveries"])


def test_retries_connection_errors(api):
    job_id = dispatch(api, "http://127.0.0.1:9/nothing-listens-here").json()["jobId"]  # port 9: refused
    job = wait_for_job(api, job_id)
    assert job["status"] == "failed"
    assert job["attempts"] == 4


def test_does_not_retry_permanent_client_errors(api, server):
    job_id = dispatch(api, mock_url(server, "?status=404")).json()["jobId"]
    job = wait_for_job(api, job_id)
    assert job["status"] == "failed"
    assert job["attempts"] == 1  # a 404 will never start working, so we stop immediately


# ------------------------------------------------------------------ delivery logging
def test_delivery_log_and_job_listing(api, server):
    job_id = dispatch(api, mock_url(server, "?fail_first=1")).json()["jobId"]
    job = wait_for_job(api, job_id)

    first = job["deliveries"][0]
    assert first["attempt"] == 1 and first["responseCode"] == 503 and first["durationMs"] >= 0
    assert first["error"] == "HTTP 503"

    listed = api.get("/api/webhooks/jobs", params={"status": "succeeded"}).json()["items"]
    assert [j["jobId"] for j in listed] == [job_id]
    assert api.get("/api/webhooks/stats").json()["jobs"]["succeeded"] == 1
    assert api.get("/api/webhooks/jobs/00000000-0000-0000-0000-000000000000").status_code == 404


# ------------------------------------------------------------------ durability
def test_job_abandoned_by_a_crashed_worker_is_recovered(api, server):
    """Simulate a worker that died mid-delivery: the job is stuck in 'processing' with an old lease."""
    url = mock_url(server)
    rows = run_sql(
        "INSERT INTO webhook_jobs (event_id, destination_url, host, payload, status, max_attempts, "
        "                          locked_at, locked_by) "
        "VALUES ('evt_crashed', $1, '127.0.0.1', '{\"x\": 1}'::jsonb, 'processing', 4, "
        "        now() - interval '1 hour', 'dead-worker') RETURNING id",
        url,
    )
    job = wait_for_job(api, str(rows[0]["id"]))
    assert job["status"] == "succeeded"


def test_health_endpoint(api):
    body = api.get("/health").json()
    assert body == {"status": "ok", "db": "up", "worker": "running"}


def test_health_accepts_head_requests_for_uptime_monitors(api):
    assert api.head("/health").status_code == 200
