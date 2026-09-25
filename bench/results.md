# Benchmark results

Node v24.21.0, Redis 7.4.11, Apple M4 Pro (12 cores).

20,000 no-op jobs, one worker with concurrency 50; latency is add() to completed for 300 jobs on an idle worker.

| Library | Enqueue (jobs/s) | Process (jobs/s) | Latency p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|
| Meridian | 101,832 | 34,687 | 0.45 | 0.92 | 1.71 |
| BullMQ | 59,956 | 35,067 | 0.48 | 0.94 | 1.72 |
