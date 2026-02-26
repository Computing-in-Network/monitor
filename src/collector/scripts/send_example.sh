#!/usr/bin/env bash
set -e

curl -s -X POST "http://127.0.0.1:9010/api/v1/ingest/node_metric" \
  -H "Content-Type: application/json" \
  -H "x-api-token: ${COLLECTOR_API_TOKEN:-change-me}" \
  -d '{
    "schema_version":"monitor.v1",
    "message_id":"example-001",
    "node_id":"N-001",
    "timestamp":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'",
    "cpu_ratio": 0.32,
    "mem_ratio": 0.61,
    "conn_count": 128,
    "tx_bps": 12000.5,
    "rx_bps": 9800.0,
    "status": "UP"
  }'
