# Environment for the recorded runs

Recorded 2026-10-05 in a cloud sandbox VM, Docker Compose, all containers on one host.

```
CPU: 2 vCPU (Intel(R) Xeon(R) Processor @ 2.10GHz)
               total        used        free      shared  buff/cache   available
Mem:               7           1           3           0           3           6
Linux 6.18.44-fc-v70
Docker 29.8.2
```

No per-container CPU or memory limits were set; all 15 containers share the 2 vCPUs. Store nodes fsync their WAL. Raft tick 20 ms, election timeout 15-30 ticks (300-600 ms), heartbeat every 3 ticks (60 ms).
