#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:9010}"
TOKEN="${COLLECTOR_API_TOKEN:-change-me}"
TRACE_ID="${TRACE_ID:-smoke-$(date +%s)}"

call_api() {
  local name="$1"
  local method="$2"
  local path="$3"
  local token="$4"
  local body="$5"

  echo "== ${name} =="
  local resp
  resp="$(curl -sS -X "${method}" "${BASE_URL}${path}" \
    -H "Content-Type: application/json" \
    -H "x-trace-id: ${TRACE_ID}-${name}" \
    -H "x-api-token: ${token}" \
    -d "${body}" \
    -w '\nHTTP_STATUS:%{http_code}\n')"
  echo "${resp}"
  echo
}

now_utc() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

msg_ok="smoke-ok-$(date +%s)"
msg_invalid="smoke-invalid-$(date +%s)"

call_api \
  "unauthorized" \
  "POST" \
  "/api/v1/ingest/node_metric" \
  "wrong-token" \
  "{\"schema_version\":\"monitor.v1\",\"message_id\":\"${msg_ok}\",\"node_id\":\"N-001\",\"timestamp\":\"$(now_utc)\"}"

call_api \
  "invalid_payload" \
  "POST" \
  "/api/v1/ingest/node_metric" \
  "${TOKEN}" \
  "{\"schema_version\":\"monitor.v1\",\"message_id\":\"${msg_invalid}\",\"timestamp\":\"$(now_utc)\"}"

call_api \
  "success" \
  "POST" \
  "/api/v1/ingest/node_metric" \
  "${TOKEN}" \
  "{\"schema_version\":\"monitor.v1\",\"message_id\":\"${msg_ok}\",\"node_id\":\"N-001\",\"timestamp\":\"$(now_utc)\",\"cpu_ratio\":0.2,\"mem_ratio\":0.4}"

echo "== snapshot =="
curl -sS "${BASE_URL}/api/v1/monitor/snapshot" -w '\nHTTP_STATUS:%{http_code}\n'
echo

echo "== snapshot_by_epoch =="
snap_headers="$(mktemp)"
curl -sS -D "${snap_headers}" "${BASE_URL}/api/v1/monitor/snapshot?topology_epoch=default" -w '\nHTTP_STATUS:%{http_code}\n'
etag="$(awk 'BEGIN{IGNORECASE=1} /^ETag:/{gsub(/\r/,"",$2); print $2}' "${snap_headers}")"
rm -f "${snap_headers}"
echo

if [ -n "${etag}" ]; then
  echo "== snapshot_if_none_match =="
  curl -sS "${BASE_URL}/api/v1/monitor/snapshot?topology_epoch=default" \
    -H "If-None-Match: ${etag}" \
    -w '\nHTTP_STATUS:%{http_code}\n'
  echo
fi

echo "== failed_events =="
curl -sS "${BASE_URL}/api/v1/ops/failed-events?limit=10" -w '\nHTTP_STATUS:%{http_code}\n'
echo

echo "== failed_events_replay =="
curl -sS -X POST "${BASE_URL}/api/v1/ops/failed-events/replay?limit=10" -w '\nHTTP_STATUS:%{http_code}\n'
echo

echo "提示: 若需验证 NATS_UNAVAILABLE(503)，请在 collector 启动后中断 NATS，再重复执行 success 请求。"
