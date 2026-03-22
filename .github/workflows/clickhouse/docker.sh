#!/bin/bash

set -ev

docker run -d --name clickhouse -p 8123:8123 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  clickhouse/clickhouse-server:25.8-alpine

# Wait for ClickHouse to be ready (using HTTP interface), timeout after 30 seconds
TIMEOUT=30
ELAPSED=0
until curl -s http://localhost:8123/ping 2>/dev/null | grep -q "Ok"; do
  if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "Timeout waiting for ClickHouse to be ready"
    exit 1
  fi
  echo "Waiting for ClickHouse to be ready..."
  sleep 2
  ELAPSED=$((ELAPSED + 2))
done
echo "ClickHouse is ready"
