#!/usr/bin/env bash
# Gate 1: single node, correct.
#   1. replay N trips into Kafka, run one worker into a single-node store,
#      and diff the store against the batch oracle;
#   2. crash the worker repeatedly mid-stream and diff again;
#   3. wipe the store, process the same log again, and require an identical
#      fingerprint (replay gives identical results) and zero conflicting
#      rewrites in the offline history.
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-1000000}
OUT=${OUT:-bench/results/gate1}
mkdir -p "$OUT"
C="docker compose -f deploy/compose/single-node.yml"
T="$C run --rm -T tools"
STORE=1=storenode1:7000

log() { echo "[gate1 $(date +%H:%M:%S)] $*" | tee -a "$OUT/gate1.log"; }

$C down -v --remove-orphans >/dev/null 2>&1 || true
log "starting kafka, storenode1, postgres"
$C up -d kafka storenode1 postgres
log "replaying $N trips (TLC 2026-03, as fast as possible, 0.2% published late)"
$T replayer -brokers kafka:9092 -topic trips -partitions 6 -tlc-file /data/yellow_tripdata_2026-03.parquet \
  -tlc-month 2026-03 -speedup 0 -max-events "$N" -late-fraction 0.002 -http :0 2>&1 | grep -E '"done"|loaded' | tee -a "$OUT/gate1.log"

log "run 1: worker with crashes (docker kill every 7 s, 4 times)"
$C up -d worker1
for i in 1 2 3 4; do sleep 7; docker kill sf-gate1-worker1-1 >/dev/null; log "killed worker1 ($i)"; $C up -d worker1 >/dev/null 2>&1; done
$T verify oracle -brokers kafka:9092 -store $STORE -wait-idle 25s | tee "$OUT/oracle-run1.json"
$T verify dump -brokers kafka:9092 -store $STORE | tee "$OUT/dump-run1.json"

log "run 2: wipe the store and process the same log again (no crashes)"
$C stop worker1 storenode1 >/dev/null
$C rm -f storenode1 >/dev/null
docker volume rm sf-gate1_raft1 >/dev/null
$C up -d storenode1 worker1
$T verify oracle -brokers kafka:9092 -store $STORE -wait-idle 25s | tee "$OUT/oracle-run2.json"
$T verify dump -brokers kafka:9092 -store $STORE | tee "$OUT/dump-run2.json"
$C exec -T postgres psql -U streamforge -tAc "select count(*), sum(conflicting_rewrites) from feature_history" | tee "$OUT/history-rewrites.txt"

a=$(grep sha256 "$OUT/dump-run1.json"); b=$(grep sha256 "$OUT/dump-run2.json")
if [ "$a" = "$b" ]; then log "PASS: identical fingerprint across runs: $a"; else log "FAIL: fingerprints differ"; exit 1; fi
$C logs worker1 > "$OUT/worker1.log" 2>&1 || true
$C down -v >/dev/null 2>&1
log "gate 1 done"
