#!/usr/bin/env bash
# Negative control: the same leader-minority partition, but the probe reads
# from ANY node's local state instead of using ReadIndex on the leader. The
# isolated old leader and its partner keep serving their stale values, so
# the checker must report the history as NOT linearizable. If it did not,
# the passing results of the other scenarios would mean nothing.
source "$(dirname "$0")/lib.sh"
setup_scenario negative-control-local-reads
PROBE_ARGS="-local-reads -clients 4 -keys 4 -think 20ms" start_probe 30
sleep 7
l=$(leader); fs=($(followers_of "$l"))
note "FAULT partition minority=[$l ${fs[0]}] majority=[${fs[1]} ${fs[2]} ${fs[3]}], reads served from local state"
partition "$l ${fs[0]}" "${fs[1]} ${fs[2]} ${fs[3]}"
sleep 12
note "heal partition"; heal
finish_probe
r=$(grep -o '"linearizable": "[a-z]*"' "$OUT/probe/result.json")
if echo "$r" | grep -q illegal; then note "EXPECTED: checker rejected the history ($r)"; else note "UNEXPECTED: checker did not catch stale local reads ($r)"; exit 1; fi
