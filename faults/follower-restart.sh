#!/usr/bin/env bash
# Follower crash and restart: the follower misses enough entries that the
# leader has compacted them, so it must catch up by InstallSnapshot.
# Expect: no write unavailability; the follower converges to the same
# applied index and state fingerprint as everyone else.
source "$(dirname "$0")/lib.sh"
setup_scenario follower-restart
start_probe 55
sleep 5
l=$(leader); f=$(followers_of "$l" | awk '{print $1}')
note "FAULT kill follower node $f (leader is $l)"
docker kill "$(ctr "$f")" >/dev/null
sleep 30
note "restart follower node $f"; docker start "$(ctr "$f")" >/dev/null
finish_probe
$COMPOSE run --rm -T tools probe -analyze "/results/faults/follower-restart"
check_converged
s=$(node_status "$f"); note "follower $f installed $(jget "$s" SnapshotsInstalled) snapshot(s) from the leader"
