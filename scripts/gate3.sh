#!/usr/bin/env bash
# Gate 3: end-to-end serving.
#   1. a sample GetFeatures response (with as_of and the read mode stated),
#   2. pipeline freshness from Prometheus,
#   3. training/serving skew: online samples vs the point-in-time join,
#   4. parity: streaming history vs a backfill from Kafka offset 0,
#   5. the Redis-down fault (serving falls back to the store).
# Runs against the full stack: docker compose -f deploy/compose/docker-compose.yml up -d
set -euo pipefail
source "$(dirname "$0")/../faults/lib.sh"
G=bench/results/gate3; mkdir -p "$G"
PG=postgres://streamforge:streamforge@postgres:5432/streamforge
T="$COMPOSE run --rm -T tools"

echo "== 1. sample response"
docker exec "${PROJECT}-server-1" wget -qO- "localhost:8081/v1/features?zones=161,237,132" | tee "$G/sample-response.json"; echo
docker exec "${PROJECT}-server-1" wget -qO- "localhost:8081/v1/features?zones=161&mode=linearizable" | tee "$G/sample-response-linearizable.json"; echo

echo "== 2. freshness (event published -> features committed), last 10 min"
q() { docker exec "${PROJECT}-prometheus-1" wget -qO- "localhost:9090/api/v1/query?query=$1" | grep -o '"value":\[[^]]*\]' | sed 's/.*,"//; s/"]//'; }
p50=$(q 'histogram_quantile(0.5,sum(rate(streamforge_freshness_seconds_bucket[10m]))by(le))')
p99=$(q 'histogram_quantile(0.99,sum(rate(streamforge_freshness_seconds_bucket[10m]))by(le))')
n=$(q 'sum(increase(streamforge_freshness_seconds_count[10m]))')
printf '{"window":"10m","batches":%s,"freshness_p50_s":%s,"freshness_p99_s":%s,"note":"Prometheus histogram quantiles (interpolated within buckets; bucket bounds 0.25ms*2^k). Cache hits add at most the cache TTL on top."}\n' "${n:-0}" "${p50:-null}" "${p99:-null}" | tee "$G/freshness.json"

echo "== 3. training/serving skew (point-in-time join)"
$T verify skew -brokers kafka:9092 -store "$STORE_PEERS" -postgres "$PG" -sample-for 30s | tee "$G/skew.json"

echo "== 4. parity: streaming history vs backfill"
docker pause "${PROJECT}-replayer-1" >/dev/null
$T verify oracle -brokers kafka:9092 -store "$STORE_PEERS" -wait-idle 25s | tee "$G/oracle.json"
$T verify backfill -brokers kafka:9092 -store "$STORE_PEERS" -postgres "$PG" | tee "$G/backfill.json"
$T verify parity -brokers kafka:9092 -store "$STORE_PEERS" -postgres "$PG" | tee "$G/parity.json"
docker unpause "${PROJECT}-replayer-1" >/dev/null

echo "== 5. Redis down"
faults/redis-down.sh > /dev/null
python3 - <<'PY' 2>/dev/null || grep -o '"codes": {[^}]*}' bench/results/faults/redis-down/loadgen.json
import json; r=json.load(open("bench/results/faults/redis-down/loadgen.json"))
print("redis-down:", r["codes"], "p99 ms:", r["overall"]["p99_ms"])
PY
echo "gate 3 done; results in $G"
