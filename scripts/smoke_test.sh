#!/usr/bin/env bash
# Smoke test against a running instance (local or deployed):
#   ./scripts/smoke_test.sh https://your-app.onrender.com
set -euo pipefail
BASE="${1:-http://localhost:8000}"
json_field() { python3 -c "import sys, json; print(json.loads(sys.stdin.readline()).get('$1', ''))"; }
post() {  # post <destinationUrl> <eventId>
  curl -s -w '\n%{http_code}' -X POST "$BASE/api/webhooks/dispatch" -H 'Content-Type: application/json' \
    -d "{\"destinationUrl\":\"$1\",\"eventId\":\"$2\",\"payload\":{\"event\":\"user.signup\",\"userId\":\"usr_999\"}}"
}
wait_for() {  # wait_for <jobId>: poll until succeeded/failed
  for _ in $(seq 1 40); do
    status=$(curl -s "$BASE/api/webhooks/jobs/$1" | json_field status)
    [[ "$status" == "succeeded" || "$status" == "failed" ]] && { echo "$status"; return; }
    sleep 1
  done; echo "timeout"
}
check() { [[ "$2" == "$3" ]] && echo "  PASS  $1" || { echo "  FAIL  $1 (expected $3, got $2)"; exit 1; }; }
RUN=$RANDOM

echo "1. health";              check "GET /health" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/health")" 200
echo "2. validation";          check "bad body -> 400" "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/webhooks/dispatch" -H 'Content-Type: application/json' -d '{"eventId":"x"}')" 400

echo "3. instant 202 even if the destination takes 10s"
start=$(date +%s); out=$(post "$BASE/mock/receiver?delay_ms=10000" "smoke-slow-$RUN"); took=$(( $(date +%s) - start ))
check "status 202" "$(echo "$out" | tail -1)" 202
[[ $took -le 2 ]] && echo "  PASS  returned in ${took}s" || { echo "  FAIL  took ${took}s"; exit 1; }

echo "4. delivery succeeds in the background"
id=$(post "$BASE/mock/receiver" "smoke-ok-$RUN" | head -1 | json_field jobId)
check "job succeeds" "$(wait_for "$id")" succeeded

echo "5. retries with backoff (target fails twice, then succeeds; takes ~6s)"
id=$(post "$BASE/mock/receiver?fail_first=2" "smoke-retry-$RUN" | head -1 | json_field jobId)
check "job succeeds after retries" "$(wait_for "$id")" succeeded
echo "  delivery log:"; curl -s "$BASE/api/webhooks/jobs/$id" | python3 -c "
import sys, json
for d in json.load(sys.stdin)['deliveries']: print('   attempt', d['attempt'], d['status'], d['responseCode'])"

echo "6. permanent client error is not retried"
id=$(post "$BASE/mock/receiver?status=404" "smoke-404-$RUN" | head -1 | json_field jobId)
check "job fails" "$(wait_for "$id")" failed
echo; echo "All smoke checks passed."
