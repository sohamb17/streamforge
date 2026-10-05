#!/usr/bin/env bash
# Network partition 3/2 with iptables. MODE=leader-minority (default) puts
# the leader on the 2-node side; MODE=leader-majority keeps it with 3.
# Expect: only the majority side commits; the minority's commit index does
# not move (no split-brain writes); a minority leader steps down
# (CheckQuorum); the history stays linearizable; everything converges after
# healing.
source "$(dirname "$0")/lib.sh"
MODE=${MODE:-leader-minority}
setup_scenario "partition-$MODE"
start_probe 50
sleep 7
l=$(leader); fs=($(followers_of "$l"))
if [ "$MODE" = leader-minority ]; then minority="$l ${fs[0]}"; majority="${fs[1]} ${fs[2]} ${fs[3]}"
else minority="${fs[0]} ${fs[1]}"; majority="$l ${fs[2]} ${fs[3]}"; fi
note "FAULT partition minority=[$minority] majority=[$majority] (leader was $l)"
partition "$minority" "$majority"
sleep 2
declare -A c0; for n in $minority; do c0[$n]=$(commit_of "$n"); done
snapshot_cluster "2 s into partition"
sleep 18
snapshot_cluster "20 s into partition"
for n in $minority; do
  c1=$(commit_of "$n")
  if [ "$c1" = "${c0[$n]}" ]; then note "minority node $n commit index unchanged at $c1 (cannot commit)"
  else note "VIOLATION: minority node $n commit moved ${c0[$n]} -> $c1"; fi
done
note "majority leader is $(leader)"
note "heal partition"; heal
finish_probe
$COMPOSE run --rm -T tools probe -analyze "/results/faults/partition-$MODE"
check_converged
