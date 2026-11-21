# Distributed Webhook Dispatcher

FastAPI + PostgreSQL (SQLAlchemy) service that queues webhook events and delivers them in the background
with exponential-backoff retries and per-domain rate limiting.

**Live URL:** `https://<your-service>.onrender.com`  (Swagger docs at `/docs`)

## Test the endpoints

```bash
BASE=https://<your-service>.onrender.com
```

The service has a built-in fake customer server at `/mock/receiver`, so no outside server is needed.
Its behaviour is set with query parameters: `fail_first=N` (answers 503 N times, then 200), `status=500`
(always that status), `delay_ms=N` (slow answer).

**1. Dispatch a webhook** (returns `202` immediately):
```bash
curl -i -X POST $BASE/api/webhooks/dispatch -H 'Content-Type: application/json' -d '{
  "destinationUrl": "'$BASE'/mock/receiver",
  "eventId": "evt_123456",
  "payload": { "event": "user.signup", "userId": "usr_999" }
}'
# 202  {"status":"queued","jobId":"<id>"}
```

**2. See the job and its delivery log** (status, response codes, attempt counts):
```bash
curl $BASE/api/webhooks/jobs/<jobId>
```

**3. Retries with backoff** (fails twice, succeeds on attempt 3; waits 2s then 4s):
```bash
curl -X POST $BASE/api/webhooks/dispatch -H 'Content-Type: application/json' \
  -d '{"destinationUrl":"'$BASE'/mock/receiver?fail_first=2","eventId":"evt_retry","payload":{"a":1}}'
```

**4. Non-blocking** (destination takes 10s, the POST still returns instantly):
```bash
time curl -X POST $BASE/api/webhooks/dispatch -H 'Content-Type: application/json' \
  -d '{"destinationUrl":"'$BASE'/mock/receiver?delay_ms=10000","eventId":"evt_slow","payload":{"a":1}}'
```

**5. Gives up after 3 retries** (always 500 → job `failed` with 4 attempts):
```bash
curl -X POST $BASE/api/webhooks/dispatch -H 'Content-Type: application/json' \
  -d '{"destinationUrl":"'$BASE'/mock/receiver?status=500","eventId":"evt_fail","payload":{"a":1}}'
```

**6. Validation** (returns `400`):
```bash
curl -i -X POST $BASE/api/webhooks/dispatch -H 'Content-Type: application/json' -d '{"eventId":"x"}'
```

Or run all of it automatically: `./scripts/smoke_test.sh $BASE`

## Architecture

**Background execution.** The API only validates the request and inserts one row into Postgres, then returns
`202`. A worker loop running next to the API picks up due jobs and delivers them with non-blocking HTTP calls
(20 at a time), so a slow destination can never slow down ingestion. Workers claim jobs with
`SELECT ... FOR UPDATE SKIP LOCKED`, so extra workers can be added without delivering a job twice.

**State persistence.** Postgres is both the queue (`webhook_jobs`) and the delivery log (`delivery_attempts`),
so nothing lives only in memory. A job is saved before the `202` is returned. If a worker dies mid-delivery, the
job's lease expires after 60s and it is picked up again (at-least-once delivery). Sending the same `eventId` to
the same URL twice does not create a second job. Job states: `queued → processing → succeeded`, or
`retrying` between attempts, or `failed`.

**Error handling.**
- `2xx` → succeeded.
- `5xx`, `429`, timeout (10s), connection error → retried up to 3 times, waiting 2s, 4s, 8s (a `Retry-After` header is respected). After that the job is `failed`.
- Other `4xx` → `failed` immediately, since retrying cannot help.
- Every attempt is logged with its status (`success` / `retrying` / `failed`), response code, error and attempt number.
- Destinations that point to private/internal IPs are refused (SSRF protection).

**Rate limiting.** A token bucket per destination host (5 requests/second, burst of 5) stored in Postgres, so the
limit holds across all workers. A job that hits the limit is rescheduled without losing one of its retries. At most
3 requests to the same host run at once. The ingestion endpoint also has a per-IP sliding window.

**Deploy.** `render.yaml` + `Dockerfile` (Render → New → Blueprint). Free-tier note: the service sleeps after ~15
minutes idle, so open `/health` first.
