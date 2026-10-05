#!/usr/bin/env bash
# Leader crash: SIGKILL the Raft leader under load, bring it back 20 s later.
# Expect: a new leader is elected; writes resume; the history stays
# linearizable; the old leader rejoins as a follower and converges.
source "$(dirname "$0")/lib.sh"
NAME=${NAME:-leader-crash}
setup_scenario "$NAME"
start_probe 50
sleep 7
old=$(leader); snapshot_cluster before
note "FAULT kill leader node $old"
docker kill "$(ctr "$old")" >/dev/null
new=$(wait_leader "$old"); note "new leader is node $new"
sleep 20
note "restart node $old"; docker start "$(ctr "$old")" >/dev/null
finish_probe
$COMPOSE run --rm -T tools probe -analyze "/results/faults/$NAME"
check_converged
