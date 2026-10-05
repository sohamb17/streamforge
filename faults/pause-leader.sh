#!/usr/bin/env bash
# Leader stall (stands in for a long GC pause or a stuck disk): docker pause
# freezes the leader process for 12 s. Expect: followers time out and elect
# a new leader; when the old leader resumes it sees a higher term and steps
# down; nothing it had in flight is lost or double-applied.
source "$(dirname "$0")/lib.sh"
setup_scenario pause-leader
start_probe 45
sleep 7
old=$(leader)
note "FAULT pause leader node $old"
docker pause "$(ctr "$old")" >/dev/null
new=$(wait_leader "$old"); note "new leader is node $new"
sleep 12
note "unpause node $old"; docker unpause "$(ctr "$old")" >/dev/null
sleep 2; snapshot_cluster "2 s after unpause"
finish_probe
$COMPOSE run --rm -T tools probe -analyze "/results/faults/pause-leader"
check_converged
