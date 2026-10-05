#!/usr/bin/env bash
# Redis down: stop the cache for 15 s under open-loop load. Expect: serving
# falls back to the Raft leader with higher latency; no errors other than
# deadline misses; the cache refills after Redis returns.
source "$(dirname "$0")/lib.sh"
setup_scenario redis-down
$COMPOSE run --rm -T tools loadgen -target server:50051 -rate 500 -duration 40s -warmup 2s -batch 10 \
  -deadline 200ms -label redis-down -out /results/faults/redis-down/loadgen.json > /dev/null 2> "$OUT/loadgen.stderr" &
LG=$!
sleep 12
note "FAULT stop redis"
docker stop "${PROJECT}-redis-1" >/dev/null
sleep 15
note "start redis"
docker start "${PROJECT}-redis-1" >/dev/null
wait $LG || true
cat "$OUT/loadgen.json"
codes=$(tr -d ' \n' < "$OUT/loadgen.json" | grep -o '"codes":{[^}]*}')
note "request outcomes during the run: $codes"
