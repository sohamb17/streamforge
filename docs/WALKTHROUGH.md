# StreamForge walkthrough

This is the guide to owning this codebase: where everything is, why it is
built the way it is, and how to answer the questions an interviewer will
ask, each answer pointing at a test, a log or a measurement in the repo.
Read it with the code open. The exercises at the end are the fastest way
to make it yours.

## 1. The 60-second version

StreamForge is a small real-time feature store. NYC taxi trips stream into
Kafka (partitioned by pickup zone). Go stream workers compute windowed
features per zone in event time (trips in the last 5/30/60 minutes, mean
fare and distance over 15 minutes, a demand-spike ratio). Each batch of
feature updates is committed, **together with the Kafka offset that
produced it and the window state**, as one entry in a 5-node Raft log. The
Raft implementation is written from scratch in Go (no Raft library). A gRPC
FeatureService serves the features, either linearizably (ReadIndex on the
leader) or from a Redis cache with a stated staleness bound. Every feature
row is also written to an append-only Postgres history, so training data
can be built with point-in-time joins.

The deliverable is not "a feature store". It is evidence for three
questions: do values stay correct when processes crash, replay or race?
Do reads stay fast while writes replicate? Can the measurements be
defended?

## 2. Repository map

| Path | What | Read first |
|---|---|---|
| `proto/streamforge/v1/` | gRPC contracts: public `FeatureService`, internal `StoreService`, `RaftTransportService`, and the replicated `Command` | `store.proto` (the `FeatureBatch` message is the heart of the design) |
| `internal/window/` | Event-time windowing: 1-minute buckets, watermark, lateness, feature derivation, the replicated window state (`Replica`) | `window.go: Add, advance, close, evict, Flush` |
| `internal/raft/` | The Raft core: a pure, tick-driven state machine with no I/O | `raft.go: Step, stepLeader, handleAppend, maybeCommit, ReadIndex`; `log.go` |
| `internal/raftnode/` | Runs the core as a server: timers, WAL + snapshots on disk, gRPC transport, apply loop | `node.go: run, processReady`; `storage.go` |
| `internal/store/` | The replicated state machine applied on every node (features, offsets, window state, registers, client dedup) | `sm.go: Apply, applyBatch` |
| `internal/storeserver/`, `internal/storeclient/` | StoreService server and the client library (leader discovery, retries, dedup sequence numbers) | `storeclient/client.go: call` |
| `internal/worker/` | Stream worker: Kafka consumer group, per-partition processors, commit pipeline, restore | `worker.go: onAssigned, adjustOffsets, proc.run, commit, restore` |
| `internal/serving/` | FeatureService: read modes, deadlines, cache, circuit breaker | `serving.go: GetFeatures` |
| `internal/history/`, `deploy/sql/` | Postgres history: idempotent upsert, point-in-time join, parity query | `history.go` |
| `internal/oracle/`, `cmd/verify/` | Batch recomputation from the log; oracle, backfill, parity, skew checks | `verify/main.go` |
| `internal/raftsim/` | Deterministic simulator for the whole Raft group + randomized fault tests + Porcupine | `sim.go`, `sim_test.go` |
| `internal/probe/`, `cmd/probe/`, `faults/` | Linearizability probe and the Docker fault-injection scenarios | `faults/partition.sh` |
| `cmd/loadgen/`, `cmd/bench/`, `bench/` | Open-loop load generator, measurement driver, Gate 4 script | `loadgen/main.go` |
| `internal/console/`, `cmd/console/`, `web/` | Live dashboard backend (snapshot stream, chaos API, continuous checks) and the React UI | `console/chaos.go` |
| `internal/simdemo/`, `cmd/simwasm/` | The same code running in the browser (WebAssembly) | `engine.go` |
| `ml/` | Small scikit-learn consumer: trains on point-in-time-joined history, scores online through the API | `features.py` |

## 3. One trip, end to end

1. **Replayer** (`cmd/replayer`) reads the March 2026 TLC file
   (`internal/tlc`), cleans and sorts it, and publishes each trip as a
   37-byte record (`internal/event`) keyed by pickup zone. The Kafka record
   timestamp is the *publish* time; the event time is in the payload (see
   the retention bug in section 9).
2. Kafka hashes the key, so **all trips of one zone land in one
   partition**. That is why per-zone windows can be computed per partition
   with no coordination between workers.
3. A **worker** owns some partitions (`kgo` consumer group). On assignment
   it reads the partition's state from the store with a linearizable read
   (`onAssigned`) and starts fetching right after the stored offset
   (`adjustOffsets`). Kafka's own committed offsets are only for lag
   dashboards.
4. `proc.consume` checks offsets are contiguous, decodes the event and
   calls `window.Partition.Add`. `Add` either drops the event as late
   (counted) or adds it to a 1-minute bucket, then advances the watermark
   (max event time minus 30 s) and closes every window whose end has
   passed, producing a feature row for every zone active in the last hour.
5. When rows are ready and no proposal is in flight for the partition,
   `Flush` returns a `Batch`: the new rows, the buckets that changed (the
   operator state), and the offset of the last event reflected. `commit`
   writes the rows to Postgres (idempotent), then proposes the batch to the
   Raft leader. One proposal in flight per partition; whatever closes
   meanwhile goes into the next batch (adaptive batching).
6. The **leader** appends the command, replicates it, and once a majority
   has it fsynced, commits it. Every node applies it in `store.SM.Apply`:
   if the batch's offset is not greater than the stored offset for that
   partition, it is ignored; otherwise buckets, offset, watermark and
   latest feature rows are updated in one step.
7. The worker invalidates the zones in Redis (off the commit path) and
   records freshness: now minus the publish time of the event that closed
   the window.
8. A **model** calls `FeatureService.GetFeatures(["161"])`. In the default
   mode the server checks Redis; on a miss it asks the leader for a
   ReadIndex read, gets the exact sums, derives the named values
   (`window.Derive`), caches them for 2 s and answers with `as_of_event_time`
   and the staleness bound.

## 4. Delivery semantics (the core of the interview story)

The rule, from the Kafka design docs: when a consumer writes to an external
system, store the consumer position *with* the output. StreamForge does it
inside the replicated state machine:

- **At least once in**: Kafka redelivers after crashes, rebalances and
  seeks.
- **Idempotent apply**: a `FeatureBatch` carries `to_offset`; `Replica.Apply`
  rejects anything at or below the stored offset. The window itself also
  ignores events at or below its offset (`Partition.Add`).
- **Resume from the store**: after a crash the worker restores the window
  state and offset from Raft, not from Kafka. Lost in-flight work is
  recomputed.
- **Determinism**: given the same events in the same order, the window
  produces identical rows. No wall clock, no map-order dependence (zones
  are sorted when windows close), integer sums only (means are derived at
  read time, so there is no floating point to compare). That is what makes
  a zombie worker harmless: it computes the same batch as its replacement,
  and whichever commits second is ignored.

Result: **effectively-once effects on the online store**. Do not call it
end-to-end exactly-once, because two things sit outside the Raft entry:

- **Postgres history** is written before the proposal. If the worker dies in
  between, the rows exist without a commit; the restarted worker recomputes
  the *same* rows and the upsert is a no-op. The key is `(feature_view,
  zone, window_end)`, not including `source_offset` as the blueprint
  suggested, because batch boundaries move with the adaptive batching: the
  same window can be produced by a batch ending at a different offset.
  Including the offset in the key would allow duplicate rows. Instead, any
  rewrite with *different* values is counted in `conflicting_rewrites`
  (always 0 in every recorded run).
- **Redis** is a cache, never a source of truth: it can be stale until the
  TTL or the invalidation, and responses say so.

Evidence: `bench/results/gate1/` (4 crashes, 0 oracle mismatches, identical
fingerprint across runs), `faults/worker-crash.sh` results (duplicate
batches ignored, redelivered records skipped, 0 mismatches),
`TestRestoreIsTransparent` in `internal/window`, and the raftsim worker that
crashes and re-proposes blindly in every one of 1,500 seeds.

## 5. Event time, windows and late data

- Buckets are 1 minute of **event time** (pickup time). Features at window
  end W use buckets in `[W-k min, W)`.
- **Watermark** = max event time seen in the partition minus 30 s. A window
  closes (rows are emitted) when the watermark passes its end.
- An event whose bucket's window already closed is **late**: counted in
  `late_dropped`, never merged. Events up to 30 s late are fine.
- A zone that goes quiet gets exactly one all-zero row, so the store does
  not keep serving its last busy values.
- Buckets older than 60 minutes before the newest closed window are
  evicted; the state machine applies the same eviction rule to its copy, so
  evictions are never shipped in batches.
- Answer to "how do late events change a window that was already served?":
  they do not. Within 30 s they land in a window that has not closed yet;
  after that they are dropped and counted. Serving never sees a row change
  after the fact. The trade-off (completeness vs. freshness) is the
  lateness allowance. In Gate 1 (1M TLC trips, 0.2 % of events delayed by
  up to 90 s, so about 2,000 delayed events), 566 arrived after their window
  closed and were dropped: roughly 28 % of the delayed events, 0.06 % of all.

`TestMatchesNaive` checks the optimized window against a brute-force
recomputation on 20 random streams with late events and idle gaps.

## 6. The Raft implementation

Design: `internal/raft` is a pure state machine in the style of etcd's
raft library: implemented from the paper, with the well-known `Step` +
`Ready` structure, but independent code. It never does I/O. `Tick()` advances time,
`Step(msg)` handles a message, `Propose`/`ReadIndex` are client calls, and
`Ready()` returns everything the driver must do: persist hard state and
entries, then send messages, then apply committed entries. That separation
is what makes deterministic simulation possible.

- **Elections**: randomized timeouts in `[E, 2E)` ticks. **PreVote**: a
  node first asks whether it *could* win before bumping its term, so a node
  coming back from a partition does not disrupt a healthy leader (the
  partition runs show the term unchanged after healing). **CheckQuorum**:
  a leader that has not heard from a majority for an election timeout steps
  down; followers that heard from a leader recently ignore vote requests
  (leader stickiness).
- **Replication**: optimistic pipelining; on rejection the follower returns
  a hint that skips the whole conflicting term (`handleAppend`), so the
  leader backs up a term at a time, not an entry at a time.
- **Commit rule**: an index commits when a majority's match index reaches
  it *and* the entry is from the current term (Figure 8 of the paper). Every
  new leader appends a no-op to commit something of its own term.
- **Linearizable reads (ReadIndex)**: the leader records its commit index,
  confirms with a heartbeat round that a majority still follows it, waits
  until it has applied that index, then reads. Reads arriving together
  share one heartbeat round (`flushReads`). If acknowledgements are lost,
  the next regular heartbeat carries the newest pending read (a liveness
  bug found by the simulator, see section 9).
- **Snapshots**: every 2,000 applied entries the state machine is
  serialized, the log is compacted, and the WAL is rewritten atomically. A
  follower that needs compacted entries gets `InstallSnapshot` (whole
  snapshot in one message; the state is small).
- **Persistence**: `raftnode/storage.go`. One WAL file of CRC-framed
  records (hard state, entries, truncations), fsynced before any message
  for those entries is sent; a torn tail is detected and cut on recovery
  (`TestRecoverFromTornWAL`). Snapshot files are written to a temp file,
  fsynced, renamed, and the directory fsynced.
- **Client retries**: puts carry `(client id, sequence)`; the state
  machine stores the last sequence per client, so a retried write is
  applied at most once.
- **Not implemented**: membership changes (fixed at 5 nodes), leadership
  transfer, chunked snapshots, leader leases for reads (ReadIndex always
  costs a heartbeat round).

## 7. How correctness is tested

1. **Property tests** for windowing (`internal/window`): optimized vs
   naive, crash/restore at random points, duplicates.
2. **Deterministic simulation** (`internal/raftsim`): the real core and
   state machine, a simulated network, random crashes, restarts, partitions
   and message loss, register clients and a stream worker. Checked per
   seed: election safety, state-machine safety, convergence, Porcupine
   linearizability of the full client history, and features equal to the
   oracle. 1,000 + 500 seeds pass (`bench/results/raft-sim/`).
3. **Mutation testing** (`scripts/mutation-test.sh`): four classic Raft bugs
   are put back in one at a time; each is caught.
4. **Real faults on the Docker cluster** (`faults/`): SIGKILL of the leader,
   `docker pause` of the leader, follower kill long enough to need a
   snapshot, 3/2 iptables partitions with the leader on either side, worker
   kills and redelivery, Redis down. Each records a probe history checked by
   Porcupine and verifies all five nodes converge to the same state
   fingerprint (`bench/results/faults/SUMMARY.md`).
5. **Negative control**: the same partition, but reading followers' local
   state; Porcupine must say "illegal", and does. Without this, the passing
   checks could be vacuous.
6. **Oracle and parity** (`cmd/verify`): recompute from offset 0 and diff
   the store; backfill the history and diff it row by row; sample online
   reads and compare with the point-in-time join (`bench/results/gate3/`).

## 8. Measurement

- **Open-loop load**: `loadgen` schedules request *i* at `start + i/rate`
  and measures from that scheduled time. A closed-loop client stops sending
  while the server stalls and so never records the stall (coordinated
  omission). The report states rate, batch size, deadline and the maximum
  number of requests in flight.
- **Histograms, not summaries**: every latency metric is a Prometheus
  histogram (exponential buckets), so quantiles can be aggregated across
  nodes; the cost is bucket-resolution estimates, which the report says.
- **Sustained throughput** means consumer lag did not grow over the
  window (least-squares slope under 1 % of the rate).
- **Two throughput regimes**, reported separately (`bench/results/gate4/REPORT.md`):
  - a synthetic stream whose event time advances at wall-clock speed
    (like a real stream of that volume): the ingest path (Kafka, decode,
    windowing) is what is measured; Raft carries a few batches per minute;
  - the real TLC month replayed at a fixed rate, i.e. time-compressed by
    thousands of times: every compressed minute closes windows, so the Raft
    and Postgres commit path is stressed and freshness degrades as the
    batches grow. This is where the bottleneck shows.
- Everything ran on one 2-vCPU machine, all services sharing it
  (`bench/results/ENVIRONMENT.md`). Re-run `bench/gate4.sh` on your own
  machine before quoting a number for it.

## 9. Bugs found along the way (good interview stories)

1. **Kafka retention ate the replay.** Records carried the March 2026
   event time as their Kafka timestamp; time-based retention (72 h) deleted
   them minutes after they were written, and consumers saw empty fetches.
   Fix: the record timestamp is the publish time; event time lives in the
   payload (`cmd/replayer`).
2. **ReadIndex could hang forever.** The simulator found it at seeds 25 and
   294 of 300: if the heartbeat acknowledgements for a read were lost, no
   later heartbeat carried the read's context. Fix: every regular heartbeat
   re-attaches the newest pending read (`tickLeader`).
3. **go-redis ignored request deadlines.** With Redis killed, 1.4 % of
   requests missed their deadline: the client's default dial retries (5)
   and `ContextTimeoutEnabled=false` meant the request context was not
   honored. Fix: one dial attempt, context deadlines on, a 1 s circuit
   breaker, and the cache write-back off the request path. Result: 0 errors
   with Redis down (`bench/results/faults/redis-down/`).
4. **Consumer-group callback order.** franz-go calls the offset-adjust hook
   *after* `OnPartitionsAssigned`, and blocking rebalances on poll stalled
   the first fetch. Restoring state in `onAssigned` and only positioning
   fetches in `adjustOffsets` fixed it.
5. **The load generator was the noisiest process on the box.** The first
   open-loop `loadgen` busy-waited (spinning on `runtime.Gosched`) between
   sends at sub-millisecond intervals. On a 2-vCPU host that burned a whole
   core and starved the system under test, inflating p99 several times.
   Fix: sleep until each scheduled send; latency is still measured from the
   scheduled time, so timer slack counts against the server rather than
   being hidden. The serving results in REPORT.md are from the fixed tool.
6. **The cache made the tail worse (a stampede).** With the fixed load
   generator, cached reads had a worse p99.9 than going to the Raft leader
   every time: all hot keys were written together, expired together, and
   were invalidated in bursts at each window close, so concurrent misses hit
   the leader at once. Fix: request coalescing for misses
   (`singleflight`, one store read per missed zone set) and TTL jitter.
   REPORT.md section 3 has the before/after table.
7. **A partition script bug that would have hidden a result**: a `set -e`
   shell function returned non-zero whenever the leader was node 5. Small,
   but it is why each scenario writes its own `events.txt` and why the
   summary is generated from files, not from console output.

## 10. Interview questions, with pointers

**Why not Redis replication or etcd?** A production team would use etcd,
DynamoDB or Cassandra for the online store. Raft is implemented here to
own replication end to end, then tested with randomized simulation,
mutation testing and real partitions, and measured against a single node
to show what replication costs (REPORT.md section 4). It is a
learning and evidence choice, not a claim that custom consensus is the
right production decision.

**What happens to a write the leader accepted but had not replicated when
it crashed?** It is not committed, and the client never got an OK. Two
cases: a new leader that has the entry commits it (by committing a no-op of
its own term); a new leader without it overwrites it. The client sees a
timeout or "outcome unknown" and retries with the same `(client id,
sequence)` (or the same Kafka offset for batches), so whichever happened,
the effect is applied once. `ErrUnknownOutcome` in `raftnode/node.go`,
`storeclient.call`, `TestRandomizedFaults` (workers re-propose blindly).

**How do you avoid double counting after a worker restart?** The offset is
stored in the same Raft entry as the output and the window state; restart
resumes from it, and anything at or below it is ignored. Gate 1: 4 kills,
0 mismatches; `faults/worker-crash.sh`: duplicates ignored, 0 mismatches.

**What does a read return during a partition, and from which side?**
Linearizable reads only succeed on the majority side's leader. The minority
side's old leader steps down within an election timeout (CheckQuorum), and
even before that its ReadIndex cannot get a majority of heartbeat acks, so
it cannot answer. Cached reads may be served stale from Redis, labelled
with the TTL bound. Evidence: `faults/partition.sh` (minority commit index
unchanged, history linearizable), `TestDeposedLeaderCannotServeStaleReads`,
and the negative control.

**How do you measure p99 without coordinated omission?** Open-loop
`loadgen`, latency from the scheduled send time, plus in-flight counts.

**How do you know training features match serving features?** One
transformation (`window`) feeds both; means are derived from integer sums by
one function on each side (`window.Derive`, `ml/features.py`). Parity:
294,543 history rows equal a backfill from offset 0; skew: 300 of 300
online reads equal the point-in-time join (`bench/results/gate3/`).

**What is the bottleneck at peak throughput, and how did you find it?**
With real-time event pacing, the ingest path scaled past the rates tested
on 2 vCPUs (see REPORT.md); the replayer and the Kafka broker used the most
CPU. With the time-compressed real data, the commit path is the limit:
every closed window becomes rows in Raft and Postgres; Postgres CPU climbs
with rows per second and freshness rises as the worker's adaptive batches
grow, while lag stays flat. Found with per-container CPU accounting in
`bench measure` and the pprof endpoints (`/debug/pprof/`).

**How do late events change a window that was already served?** Section 5.

## 11. What you can claim, and what not

Claim only what the recorded runs show, with their conditions:

- Built a Raft-replicated online feature store in Go (5 nodes, Raft written
  from scratch); verified linearizable reads and writes with Porcupine on
  recorded histories under SIGKILLed and paused leaders and iptables
  partitions, plus 1,500 randomized simulated fault schedules.
- Made Kafka consumption effectively-once by committing source offsets and
  window state atomically with feature updates in the replicated state
  machine; replays and crash recovery reproduced identical features.
- Numbers from `bench/results/gate4/REPORT.md` with hardware, concurrency and
  duration attached.

Do not claim: end-to-end exactly-once; production uptime or SLOs;
linearizable serving through the cache; a novel consensus algorithm; ML in
production; Kubernetes; any number without its conditions.

## 12. Known limitations and next steps

- Fixed membership (no joint consensus); no leadership transfer.
- Snapshots are sent whole; fine for a small state, not for gigabytes.
- ReadIndex costs a heartbeat round per batch of reads; leader leases
  would make reads cheaper at the cost of clock assumptions.
- One Kafka broker and one Postgres in the demo; the Raft group is the only
  replicated component.
- Cold start on a small machine: when all 15 containers boot together on 2
  vCPUs, store nodes can be starved past the 300-600 ms election timeout
  and hold a few extra elections before settling, so the term after a fresh
  `up` is often well above 1. It is stable once running. The compose file
  gives the store nodes `cpu_shares: 2048` to soften this; a real deployment
  would give each node its own core.
- The worker writes history rows before proposing; a crashed worker can
  leave rows for windows that commit later (identical values, verified by
  `conflicting_rewrites`), so the history can briefly be ahead of the store.

## 13. Exercises (do these yourself)

1. Change the lateness allowance to 10 s in `cmd/worker` flags, rerun
   `scripts/gate1.sh`, and predict `late_dropped` before looking.
2. Delete the `t != c.term` check in `maybeCommit` and run
   `RAFTSIM_SEEDS=300 go test ./internal/raftsim -run HeavyChurn`. Then write
   a *targeted* test that reproduces Figure 8 deterministically.
3. Turn off fsync (`storenode -fsync=false`) and compare Raft commit latency
   in Grafana. What guarantee did you just give up?
4. Add a `trips_15m` feature end to end: window, proto, Derive, UI.
5. Implement leadership transfer (`TimeoutNow`) and use it before a planned
   restart.
