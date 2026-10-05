#!/usr/bin/env bash
# Gate 4: the measured run. Every number in bench/results/gate4/REPORT.md
# comes from this script; nothing is tuned between its steps.
#   bench/gate4.sh            (about 70 minutes on a 2-vCPU machine)
set -uo pipefail
cd "$(dirname "$0")/.."
source faults/lib.sh
G=bench/results/gate4; mkdir -p "$G"
T="$COMPOSE run --rm -T tools"
# SECTIONS="3 6" re-runs only those sections (default: all).
SECTIONS=${SECTIONS:-"1 2 3 4 5 6 7"}
want() { [[ " $SECTIONS " == *" $1 "* ]]; }
FIVE="kafka storenode1 storenode2 storenode3 storenode4 storenode5 postgres redis worker1 worker2 server prometheus"

fresh_five() {
  $COMPOSE down -v --remove-orphans >/dev/null 2>&1
  $COMPOSE up -d ${UP_FLAGS:-} $FIVE >/dev/null 2>&1; sleep 20
}

if want 1; then
echo "== 1. ingest throughput, synthetic stream with real-time event pacing"
for R in 25000 100000 250000 400000; do bench/throughput.sh $R 90s "ingest-ramp-$R" synthetic 100s >/dev/null 2>&1; done
FINAL=${FINAL_RATE:-250000}
bench/throughput.sh "$FINAL" 600s "ingest-final-$FINAL" synthetic 120s >/dev/null 2>&1
fi

if want 2; then
echo "== 2. time-compressed TLC replay (stresses the Raft + history commit path)"
for R in 3000 8000 15000; do bench/throughput.sh $R 90s "tlc-compressed-$R" tlc 60s >/dev/null 2>&1; done
fi

if want 3; then
echo "== 3. serving latency (open loop) while the demo replay runs (TLC at 60x)"
fresh_five
$COMPOSE up -d ${UP_FLAGS:-} replayer >/dev/null 2>&1; sleep 90
for R in 1000 2000 4000; do
  $T loadgen -target server:50051 -rate $R -duration 60s -warmup 10s -batch 1 -mode bounded -deadline 100ms \
    -label "serve-bounded-b1-$R" -out "/results/gate4/serve-bounded-b1-$R.json" >/dev/null 2>&1
done
for R in 500 1000 2000; do
  $T loadgen -target server:50051 -rate $R -duration 60s -warmup 10s -batch 1 -mode linearizable -deadline 100ms \
    -label "serve-linearizable-b1-$R" -out "/results/gate4/serve-linearizable-b1-$R.json" >/dev/null 2>&1
done
$T loadgen -target server:50051 -rate 1000 -duration 60s -warmup 10s -batch 10 -mode bounded -deadline 100ms \
  -label "serve-bounded-b10-1000" -out "/results/gate4/serve-bounded-b10-1000.json" >/dev/null 2>&1
fi

if want 4; then
echo "== 4. Raft write throughput (closed loop, 32 clients) on 5 nodes"
$T probe -store "$STORE_PEERS" -duration 60s -clients 32 -keys 64 -think 0s -out /results/gate4/raft-writes-5node >/dev/null 2>&1
fi

if want 5; then
echo "== 5. leader crash x5 (write unavailability distribution)"
for i in 1 2 3 4 5; do NAME="leader-crash-$i" faults/leader-crash.sh >/dev/null 2>&1; sleep 5; done
mkdir -p "$G/leader-crash-repeats"
for i in 1 2 3 4 5; do cp "bench/results/faults/leader-crash-$i/unavailability.json" "$G/leader-crash-repeats/run-$i.json" 2>/dev/null; rm -rf "bench/results/faults/leader-crash-$i"; done
fi

if want 6; then
echo "== 6. replication cost: the same tests on a single-node store"
export RAFT_PEERS=1=storenode1:7000 STORE_PEERS=1=storenode1:7000
export UP_SERVICES="--no-deps kafka storenode1 postgres redis worker1 worker2 server prometheus"
bench/throughput.sh 3000 90s "tlc-compressed-3000-1node" tlc 60s >/dev/null 2>&1
bench/throughput.sh 8000 90s "tlc-compressed-8000-1node" tlc 60s >/dev/null 2>&1
$COMPOSE down -v --remove-orphans >/dev/null 2>&1
$COMPOSE up -d ${UP_FLAGS:-} $UP_SERVICES >/dev/null 2>&1; sleep 15
$COMPOSE up -d ${UP_FLAGS:-} --no-deps replayer >/dev/null 2>&1; sleep 90
$T probe -store "$STORE_PEERS" -duration 60s -clients 32 -keys 64 -think 0s -out /results/gate4/raft-writes-1node >/dev/null 2>&1
$T loadgen -target server:50051 -rate 1000 -duration 60s -warmup 10s -batch 1 -mode linearizable -deadline 100ms \
  -label "serve-linearizable-b1-1000-1node" -out "/results/gate4/serve-linearizable-b1-1000-1node.json" >/dev/null 2>&1
unset RAFT_PEERS STORE_PEERS UP_SERVICES
$COMPOSE down -v --remove-orphans >/dev/null 2>&1
fi

if want 7; then
echo "== 7. report"
$T bench report -dir /results/gate4 > /dev/null
$COMPOSE up -d ${UP_FLAGS:-} >/dev/null 2>&1
cat "$G/REPORT.md"
fi
