# Deterministic simulation results

`internal/raftsim` runs the real `raft.Core` and the real `store.SM` for a
5-node (or 3-node) group on a simulated clock and network. Every run is
reproducible from its seed. Each seed runs ~2,500 ticks of random faults
(crashes, restarts, 2-way partitions, message loss up to 5 %, reordering
through random delays) with 4 register clients and a stream worker that
crashes and retries, then heals everything and checks:

- **election safety**: never two leaders in one term,
- **state machine safety**: no two nodes apply different entries at one index,
- **convergence**: all nodes end with the same commit index and state,
- **linearizability** of the full client history (Porcupine),
- **feature correctness**: every node's features equal a sequential batch
  recomputation of the same events (the oracle).

Recorded on 2026-10-05 (see `../ENVIRONMENT.md`):

| Suite | Seeds | Failures | Client ops checked | Duplicate batches ignored | Worker restores |
|---|---|---|---|---|---|
| `TestRandomizedFaults` (5 nodes, normal churn) | 1,000 | 0 | 1,441,785 | 941 | 11,245 |
| `TestRandomizedHeavyChurn` (3 nodes, ~5x crash rate) | 500 | 0 | 387,558 | 1,856 | 6,612 |

Reproduce:

```sh
RAFTSIM_SEEDS=1000 go test ./internal/raftsim/ -run 'TestRandomizedFaults$' -v
RAFTSIM_SEEDS=500  go test ./internal/raftsim/ -run 'TestRandomizedHeavyChurn$' -v
scripts/mutation-test.sh   # see mutation-testing.md
```

`mutation-testing.md` shows that the suite fails when classic Raft bugs are
put back in. The Figure 8 bug is caught by the linearizability check in
only a small fraction of heavy-churn seeds (1 of 300 in the recorded run):
it needs a specific sequence of three leader changes, and the random
schedule rarely produces it. A targeted test for it would be stronger.
