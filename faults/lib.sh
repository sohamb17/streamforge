# Shared helpers for the fault-injection scripts. Source it, don't run it.
#
# Every scenario records a client history with the probe while the fault is
# injected, then checks it for linearizability, and writes everything into
# bench/results/faults/<scenario>/.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
COMPOSE="docker compose -f deploy/compose/docker-compose.yml ${COMPOSE_EXTRA:-}"
PROJECT=streamforge
STORE_PEERS=${STORE_PEERS:-1=storenode1:7000,2=storenode2:7000,3=storenode3:7000,4=storenode4:7000,5=storenode5:7000}
NODES="1 2 3 4 5"

ctr() { echo "${PROJECT}-storenode$1-1"; }

# node_status N -> JSON from the node's admin endpoint (empty if down).
node_status() { docker exec "$(ctr "$1")" wget -qO- -T 2 "localhost:9100/admin/status?fingerprint=1" 2>/dev/null || true; }

jget() { # jget '<json>' key  (flat numeric/string fields only)
  echo "$1" | tr ',{}' '\n\n\n' | grep -m1 "\"$2\":" | sed 's/.*://; s/"//g'
}

# leader -> id of the node that reports itself leader with the highest term.
leader() {
  local best=0 bt=-1 s r t
  for n in $NODES; do
    s=$(node_status "$n"); [ -z "$s" ] && continue
    r=$(jget "$s" role); t=$(jget "$s" Term)
    if [ "$r" = "leader" ] && [ "${t:-0}" -gt "$bt" ]; then best=$n; bt=$t; fi
  done
  echo "$best"
}

wait_leader() { # wait_leader [exclude-id]
  for _ in $(seq 1 100); do
    l=$(leader); if [ "$l" != 0 ] && [ "$l" != "${1:-}" ]; then echo "$l"; return; fi; sleep 0.2
  done
  echo 0
}

followers_of() { for n in $NODES; do if [ "$n" != "$1" ]; then printf "%s " "$n"; fi; done; }

ip_of() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(ctr "$1")"; }

# partition "1 2" "3 4 5": drop all packets between the two groups with
# iptables inside each store node's network namespace.
partition() {
  local a="$1" b="$2" x y
  for x in $a; do for y in $b; do
    docker exec "$(ctr "$x")" iptables -A INPUT -s "$(ip_of "$y")" -j DROP
    docker exec "$(ctr "$x")" iptables -A OUTPUT -d "$(ip_of "$y")" -j DROP
    docker exec "$(ctr "$y")" iptables -A INPUT -s "$(ip_of "$x")" -j DROP
    docker exec "$(ctr "$y")" iptables -A OUTPUT -d "$(ip_of "$x")" -j DROP
  done; done
}

heal() { for n in $NODES; do docker exec "$(ctr "$n")" iptables -F 2>/dev/null || true; done; }

commit_of() { jget "$(node_status "$1")" Commit; }

OUT=""
setup_scenario() { # setup_scenario <name>
  OUT="bench/results/faults/$1"
  rm -rf "$OUT"; mkdir -p "$OUT"
  : > "$OUT/events.txt"
  heal
  for n in $NODES; do docker start "$(ctr "$n")" >/dev/null 2>&1 || true; docker unpause "$(ctr "$n")" >/dev/null 2>&1 || true; done
  sleep 2
  note "scenario $1 starting; leader is $(wait_leader)"
}

note() { echo "$(date +%s%3N) $*" | tee -a "$OUT/events.txt"; }

# start_probe <seconds>: records a history in the background.
start_probe() {
  $COMPOSE run --rm -T tools probe -store "$STORE_PEERS" -duration "${1}s" -clients 8 -keys 16 ${PROBE_ARGS:-} \
    -out "/results/faults/$(basename "$OUT")/probe" > "$OUT/probe.stdout" 2> "$OUT/probe.stderr" &
  PROBE_PID=$!
  sleep 3   # container start-up; the probe writes its start time to start.txt
}

finish_probe() {
  wait "$PROBE_PID" || true
  cat "$OUT/probe.stdout"
}

# snapshot_cluster <label>: role/term/commit/applied/fingerprint per node.
snapshot_cluster() {
  {
    echo "## $1"
    for n in $NODES; do
      s=$(node_status "$n")
      if [ -z "$s" ]; then echo "node $n: down"; continue; fi
      echo "node $n: role=$(jget "$s" role) term=$(jget "$s" Term) commit=$(jget "$s" Commit) applied=$(jget "$s" Applied) snap_installed=$(jget "$s" SnapshotsInstalled) fingerprint=$(jget "$s" Fingerprint)"
    done
  } | tee -a "$OUT/cluster.txt"
}

# converged: wait until all live nodes report the same applied index and
# fingerprint (pauses the replayer so the log stops growing).
check_converged() {
  docker pause "${PROJECT}-replayer-1" >/dev/null 2>&1 || true
  local ok=0
  for _ in $(seq 1 60); do
    sleep 1
    local fps="" apps=""
    for n in $NODES; do
      s=$(node_status "$n"); [ -z "$s" ] && continue
      fps="$fps $(jget "$s" Fingerprint)"; apps="$apps $(jget "$s" Applied)"
    done
    if [ "$(echo $fps | tr ' ' '\n' | sort -u | wc -l)" = 1 ] && [ "$(echo $apps | tr ' ' '\n' | sort -u | wc -l)" = 1 ]; then ok=1; break; fi
  done
  snapshot_cluster "after convergence (replayer paused)"
  docker unpause "${PROJECT}-replayer-1" >/dev/null 2>&1 || true
  if [ $ok = 1 ]; then note "CONVERGED: all nodes report the same applied index and state fingerprint"; else note "DIVERGED or did not converge"; return 1; fi
}
