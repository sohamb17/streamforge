#!/usr/bin/env bash
# Stream worker crashes and duplicate delivery, end to end:
#   - SIGKILL worker1 three times while it is committing batches,
#   - make worker2 re-send its last committed batch (a lost ack) and rewind
#     its Kafka position by 2000 offsets (redelivery),
#   - then stop the replayer, let the workers catch up, and diff the online
#     store against a batch recomputation from the Kafka log.
# Expect: duplicate batches are ignored, and 0 oracle mismatches.
source "$(dirname "$0")/lib.sh"
setup_scenario worker-crash
for i in 1 2 3; do
  sleep 6; note "FAULT kill worker1 ($i)"; docker kill "${PROJECT}-worker1-1" >/dev/null; sleep 1; docker start "${PROJECT}-worker1-1" >/dev/null
done
note "FAULT redeliver on worker2 (re-send last batch, rewind 2000 offsets)"
docker exec "${PROJECT}-worker2-1" wget -qO- --post-data= "localhost:9103/admin/redeliver?n=2000" | tee -a "$OUT/events.txt"; echo
sleep 5
dups=$(docker exec "${PROJECT}-worker2-1" wget -qO- localhost:9103/metrics | awk '/^streamforge_worker_batches_total.*duplicate/ {s+=$2} END {print s+0}')
skipped=$(docker exec "${PROJECT}-worker2-1" wget -qO- localhost:9103/metrics | awk '/^streamforge_worker_records_skipped_total.*duplicate/ {s+=$2} END {print s+0}')
note "worker2: $dups duplicate batch(es) ignored by the store, $skipped redelivered record(s) skipped by the offset guard"
note "pausing replayer; waiting for workers to go idle"
docker pause "${PROJECT}-replayer-1" >/dev/null
$COMPOSE run --rm -T tools verify oracle -brokers kafka:9092 -store "$STORE_PEERS" -wait-idle 25s | tee "$OUT/oracle.json"
docker unpause "${PROJECT}-replayer-1" >/dev/null
