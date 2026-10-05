# StreamForge

A small real-time feature store: taxi-trip events go into Kafka, stream
workers compute per-zone windowed features, and a 5-node Raft-replicated
store (Raft implemented from scratch in Go) serves them, with an offline
Postgres history for point-in-time-correct training data.

Work in progress. Status by gate:

- [x] Gate 1: single node, correct (see `bench/results/gate1/`)
- [ ] Gate 2: Raft passes fault tests
- [ ] Gate 3: end-to-end serving
- [ ] Gate 4: measured

Quick checks:

```sh
go test ./...                     # unit, property and Raft simulation tests
RAFTSIM_SEEDS=1000 go test ./internal/raftsim/ -run TestRandomizedFaults
data/fetch.sh && scripts/gate1.sh # needs Docker
```
