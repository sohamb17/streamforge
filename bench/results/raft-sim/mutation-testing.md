| Injected bug | Result | Detected by |
|---|---|---|
| Commit entries from older terms by counting replicas (Raft paper Figure 8) | caught |       1 not linearizable  |
| Grant votes without the log up-to-date check | caught |     163 safety violations  |
| Serve ReadIndex without confirming leadership with a heartbeat round | caught |       1 served a read  |
| Vote for more than one candidate per term | caught |     274 safety violations  |
