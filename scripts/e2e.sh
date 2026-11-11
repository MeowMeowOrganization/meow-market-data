#!/usr/bin/env bash
# CEX 23.7: real service, real Kafka, PostgreSQL, Redis, Connect, WebSocket.
# Run from anywhere: bash scripts/e2e.sh
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f deploy/local/compose.yml -f deploy/local/compose.e2e.yml)
for command in docker go curl; do
  command -v "$command" >/dev/null || { echo "Missing required executable: $command" >&2; exit 1; }
done
if [[ ! -d ../meow-contracts/gen/go/meow/exchange/matching/v1alpha1 ]]; then
  echo 'Missing ../meow-contracts generated Go contracts; this repository go.mod uses a local replace.' >&2
  echo 'Clone meow-contracts alongside meow-market-data, and run buf generate in meow-contracts.' >&2
  exit 1
fi

# Fresh Kafka topic/PG database per run. No pre-existing projection gets reset.
ID="$(date -u +%Y%m%d%H%M%S)_$RANDOM"
DB_NAME="md_e2e_${ID}"
TOPIC="matching.results.cex237.${ID//_/.}"
GROUP="meow-md-e2e-${ID}"
TMP="$(mktemp -d)"
SERVICE_PID=''
CREATED_DB=0
CREATED_TOPIC=0
KEEP_E2E="${KEEP_E2E:-0}"

stop_service() {
  if [[ -n "$SERVICE_PID" ]]; then
    kill "$SERVICE_PID" 2>/dev/null || true
    wait "$SERVICE_PID" 2>/dev/null || true
    SERVICE_PID=''
  fi
}
cleanup() {
  code=$?
  trap - EXIT
  stop_service
  if [[ $code -ne 0 ]]; then
    echo '--- market-data service log (tail) ---' >&2
    tail -n 120 "$TMP/service.log" 2>/dev/null >&2 || true
  fi
  if [[ $code -eq 0 && "$KEEP_E2E" == 0 ]]; then
    if [[ "$CREATED_TOPIC" -eq 1 ]]; then
      "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:19092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
    fi
    if [[ "$CREATED_DB" -eq 1 ]]; then
      "${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U meow -d postgres \
        -c "DROP DATABASE IF EXISTS \"$DB_NAME\" WITH (FORCE);" >/dev/null 2>&1 || true
    fi
    "${COMPOSE[@]}" exec -T redis-e2e redis-cli FLUSHDB >/dev/null 2>&1 || true
    rm -rf "$TMP"
  else
    echo "E2E artifacts retained: database=$DB_NAME topic=$TOPIC logs=$TMP" >&2
    echo "Use KEEP_E2E=1 to retain artifacts even on success." >&2
  fi
  exit "$code"
}
trap cleanup EXIT

if curl -fsS --max-time 2 http://127.0.0.1:18090/healthz >/dev/null 2>&1; then
  echo 'Port 18090 is already serving HTTP; stop the conflicting service first.' >&2
  exit 1
fi

"${COMPOSE[@]}" up -d --wait postgres kafka redis-e2e
"${COMPOSE[@]}" exec -T redis-e2e redis-cli FLUSHDB >/dev/null
"${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U meow -d postgres \
  -c "CREATE DATABASE \"$DB_NAME\";"
CREATED_DB=1
for migration in migrations/000001_recovery.sql migrations/000002_public_projections.sql migrations/000003_stream_candles.sql migrations/000004_trade_minute_rollups.sql migrations/000005_cex25_finality_provenance.sql; do
  "${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U meow -d "$DB_NAME" < "$migration" >/dev/null
  echo "Applied $migration"
done
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --create --topic "$TOPIC" \
  --partitions 1 --replication-factor 1 --config cleanup.policy=delete
CREATED_TOPIC=1

# Build once: the production binary runs unchanged in both phases.
go build -o "$TMP/market-data" ./cmd/market-data
go build -o "$TMP/e2e-check" ./cmd/e2e-check

export MARKET_DATA_DATABASE_URL="postgres://meow:meow@127.0.0.1:5435/$DB_NAME?sslmode=disable"
export MARKET_DATA_KAFKA_BROKERS='127.0.0.1:9094'
export MARKET_DATA_KAFKA_TOPIC="$TOPIC"
export MARKET_DATA_KAFKA_GROUP="$GROUP"
export MARKET_DATA_KAFKA_CLIENT_ID='meow-md-e2e'
export MARKET_DATA_MARKETS_FILE="$ROOT/config/markets.example.json"
export MARKET_DATA_REDIS_URL='redis://127.0.0.1:6382/0'
export MARKET_DATA_REDIS_REFRESH_INTERVAL='1s'
export MARKET_DATA_REDIS_BOOK_POLL_INTERVAL='100ms'
export MARKET_DATA_REDIS_PROJECTION_EPOCH='1'
export MARKET_DATA_HTTP_ADDR='127.0.0.1:18090'
export MARKET_DATA_CURSOR_SECRET='cex237-e2e-local-not-a-real-production-secret-12345678'
export MARKET_DATA_PUBLIC_ALLOWED_ORIGINS='http://localhost:5173'
export MARKET_DATA_HTTP_MAX_WS_CONNECTIONS='16'
export MARKET_DATA_STREAM_POLL_INTERVAL='100ms'
export MARKET_DATA_STREAM_SOFT_INTERVAL='2s'

start_service() {
  "$TMP/market-data" > "$TMP/service.log" 2>&1 &
  SERVICE_PID=$!
  for ((i=0;i<150;i++)); do
    if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
      echo 'market-data exited during startup' >&2
      return 1
    fi
    if curl -fsS --max-time 1 http://127.0.0.1:18090/readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo 'Timed out waiting for market-data /readyz' >&2
  return 1
}

common=(--dsn "$MARKET_DATA_DATABASE_URL" --redis "$MARKET_DATA_REDIS_URL" \
  --brokers "$MARKET_DATA_KAFKA_BROKERS" --topic "$TOPIC" \
  --redis-epoch "$MARKET_DATA_REDIS_PROJECTION_EPOCH" \
  --http 'http://127.0.0.1:18090')

echo '=== Phase 1: Kafka -> PostgreSQL -> Redis -> Connect/WebSocket ==='
start_service
"$TMP/e2e-check" --phase live "${common[@]}"

echo '=== Phase 2: Restart -> durable replay -> fresh execution -> Redis rebuild ==='
stop_service
start_service
"$TMP/e2e-check" --phase recovery "${common[@]}"

echo '=== Phase 3: Redis temporarily unavailable -> PostgreSQL API fallback ==='
"${COMPOSE[@]}" stop redis-e2e >/dev/null
"$TMP/e2e-check" --phase outage "${common[@]}"

echo '=== Phase 4: Redis restarted -> automatic cache reconstruction ==='
"${COMPOSE[@]}" up -d --wait redis-e2e
"$TMP/e2e-check" --phase restored "${common[@]}"

echo 'PASS CEX 24.3: incremental projection, rolling statistics, outage fallback and Redis recovery'
