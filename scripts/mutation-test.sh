#!/usr/bin/env bash
# Mutation testing for the Raft core: re-introduce classic Raft bugs one at a
# time and check that the simulation tests catch each of them. A test suite
# that passes on a buggy implementation proves nothing; this shows ours fails.
set -uo pipefail
cd "$(dirname "$0")/.."
F=internal/raft/raft.go
cp "$F" /tmp/raft.go.orig
trap 'cp /tmp/raft.go.orig "$F"' EXIT
SEEDS=${SEEDS:-300}

mutate() { # mutate <name> <python replace from> <to>
  python3 - "$F" "$2" "$3" <<'PY'
import sys
p, a, b = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(p).read()
assert a in s, "mutation anchor not found"
open(p, "w").write(s.replace(a, b, 1))
PY
}

run() { # run <name> <test regex>
  out=$(RAFTSIM_SEEDS=$SEEDS go test ./internal/raftsim/ -run "$2" -count=1 2>&1)
  if echo "$out" | grep -q "^ok"; then verdict="NOT CAUGHT"; else verdict="caught"; fi
  why=$(echo "$out" | grep -oE "safety violations|not linearizable|served a read|differ from oracle|did not converge|did not finish" | sort | uniq -c | tr '\n' ' ')
  printf '| %s | %s | %s |\n' "$1" "$verdict" "$why"
  cp /tmp/raft.go.orig "$F"
}

echo "| Injected bug | Result | Detected by |"
echo "|---|---|---|"
mutate m1 $'if t, _ := c.log.term(n); t != c.term {\n\t\treturn false\n\t}' ''
run "Commit entries from older terms by counting replicas (Raft paper Figure 8)" 'TestRandomizedHeavyChurn'
mutate m2 'if canVote && c.log.isUpToDate(m.Index, m.LogTerm) {' 'if canVote {'
run "Grant votes without the log up-to-date check" 'TestRandomizedFaults'
mutate m3 $'\tc.startRead(ctx)\n\treturn nil' $'\tc.readStates = append(c.readStates, ReadState{Index: c.log.committed, Context: ctx})\n\treturn nil'
run "Serve ReadIndex without confirming leadership with a heartbeat round" 'TestDeposedLeaderCannotServeStaleReads'
mutate m4 'canVote := c.vote == m.From ||' 'canVote := true ||'
run "Vote for more than one candidate per term" 'TestRandomizedFaults'
