#!/usr/bin/env bash
# Runs every fault scenario against the running stack and writes a summary.
#   docker compose -f deploy/compose/docker-compose.yml up -d
#   faults/run-all.sh
set -uo pipefail
cd "$(dirname "$0")"
./leader-crash.sh
./follower-restart.sh
MODE=leader-minority ./partition.sh
MODE=leader-majority ./partition.sh
./pause-leader.sh
./worker-crash.sh
./negative-control.sh
[ -x ./redis-down.sh ] && ./redis-down.sh
./summarize.sh
