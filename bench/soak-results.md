# Soak test results

Node v24.21.0, Apple M4 Pro; Redis at 127.0.0.1:6379.
1 minutes at 200 jobs/s into one queue, 4 worker processes x concurrency 25,
recycling off. Jobs keep the newest 1,000 completed and failed, carry one of 100 tags, fail
once in 50 attempts (retried) and permanently once in 997. A scheduler adds a job every second.

**Growth per hour** after warm-up (least-squares slope): worker RSS 5658.6 MB/h,
Redis memory 248.3 MB/h, keys 1180851.7/h. Worker crashes: 0.

| Minute | Jobs/s | Waiting | Worker RSS (MB, 4 processes) | Redis (MB) | Keys | Events stream | Tag index (customer:1) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.3 | 198 | 0 | 431 | 12 | 15,887 | 9,063 | 1 |
| 0.5 | 200 | 0 | 482 | 13 | 21,929 | 10,013 | 1 |
| 0.8 | 200 | 0 | 501 | 14 | 27,473 | 10,045 | 2 |
| 1 | 200 | 0 | 501 | 15 | 29,818 | 10,051 | 1 |
