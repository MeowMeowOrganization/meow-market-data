#!/usr/bin/env bash
# CEX24.3 reproducible microbenchmarks. Integration benchmarks are opt-in.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
command -v go >/dev/null || { echo 'Go is required' >&2; exit 1; }

echo '=== Pure Go L2 and rolling-aggregate benchmarks ==='
go test -run '^$' -bench 'BenchmarkApply20Levels|BenchmarkStatsExactMerge1440Minutes' \
  -benchmem ./internal/projection/l2 ./internal/readmodel

if [[ -n "${MARKET_DATA_TEST_DATABASE_URL:-}" && -n "${MARKET_DATA_PERF_TOPIC:-}" ]]; then
  echo '=== PostgreSQL rolling-24h benchmark on existing test market ==='
  go test -tags integration -run '^$' -bench '^BenchmarkLoadStats24hPostgres$' \
    -benchmem ./internal/readmodel
else
  echo 'SKIP PostgreSQL benchmark: set MARKET_DATA_TEST_DATABASE_URL and MARKET_DATA_PERF_TOPIC'
fi

if [[ -n "${MARKET_DATA_TEST_REDIS_URL:-}" ]]; then
  echo '=== Real isolated Redis atomic L2 read/CAS benchmark ==='
  go test -tags integration -run '^$' -bench '^BenchmarkLiveBookReadCAS$' \
    -benchtime=3s -benchmem ./internal/storage/redis
else
  echo 'SKIP Redis benchmark: set MARKET_DATA_TEST_REDIS_URL'
fi
