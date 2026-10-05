#!/usr/bin/env bash
# Sustained-throughput run: replay at a fixed rate and measure.
#   bench/throughput.sh <rate ev/s> <measure duration> <label> [tlc|synthetic] [warmup]
# Pauses the demo replayer for the duration of the run.
set -euo pipefail
source "$(dirname "$0")/../faults/lib.sh"
RATE=$1; DUR=$2; LABEL=$3; SRC=${4:-tlc}; WARM=${5:-60s}
OUTD=bench/results/gate4; mkdir -p "$OUTD"
docker rm -f sf-bench-replayer >/dev/null 2>&1 || true
if [ "${RESET:-1}" = 1 ]; then
  # Fresh Kafka topic, Raft logs and history, so earlier runs' watermarks
  # and state cannot affect this one.
  echo "resetting the stack (RESET=0 to skip)"
  $COMPOSE down -v --remove-orphans >/dev/null 2>&1
  $COMPOSE up -d ${UP_FLAGS:-} ${UP_SERVICES:-kafka storenode1 storenode2 storenode3 storenode4 storenode5 postgres redis worker1 worker2 server prometheus} >/dev/null 2>&1
  sleep 20
fi
docker stop "${PROJECT}-replayer-1" >/dev/null 2>&1 || true
# synthetic: event time advances at wall-clock speed (rate*60 events per
# minute of event time), like a real stream of that volume. tlc: the real
# month replayed at a fixed rate, i.e. heavily time-compressed.
$COMPOSE run -d --name sf-bench-replayer tools replayer -brokers kafka:9092 -topic trips -source "$SRC" \
  -tlc-file /data/yellow_tripdata_2026-03.parquet -tlc-month 2026-03 -rate "$RATE" -loop -late-fraction 0.002 \
  -synthetic-per-minute "$((RATE * 60))" -zipf 1.1 -seed "$RANDOM" -http :0 >/dev/null
echo "replaying $SRC at $RATE ev/s; warm-up $WARM"
sleep "${WARM%s}"
$COMPOSE run --rm -T tools bench measure -duration "$DUR" -rate "$RATE" -label "$LABEL" -source "$SRC" \
  -project "$PROJECT" -out "/results/gate4/throughput-$LABEL.json" | tee "$OUTD/throughput-$LABEL.stdout"
docker rm -f sf-bench-replayer >/dev/null 2>&1 || true
