# 7. Surviving connection drops, Redis crashes and Redis Cluster

- Status: accepted
- Date: 2026-09-25

## Context

The guarantees in ADR 0003 assume every script call runs exactly once. Real networks drop
connections. With `maxRetriesPerRequest: null`, ioredis reconnects and **resends** any
command whose reply it did not receive. The first execution may already have been applied,
so the same script can run twice for one call. Redis itself can also crash, and larger
deployments run Redis Cluster.

Chaos tests turned up three concrete problems:

1. A resent `moveToActive` claimed a **second** job for one fetch. The worker only knew
   about the second one, so the first sat locked until the stalled checker recovered it.
   That counted as a stall toward `maxStalledCount`, so it could eventually fail a healthy
   job.
2. A resent `moveToFinished`, `retryJob` or `releaseJob` found the lock already released by
   its own first run and reported a **false** lost lock.
3. `close()` could **hang forever**. When the blocking connection was dropped during a
   `BLPOP`, ioredis kept the command queued between reconnect attempts, and `disconnect()`
   cleared the reconnect timer without rejecting it.

## Decision

**Scripts are safe to run twice.** Every claim and settle call already carries a unique
token. The scripts leave short-lived receipts keyed by it (60 seconds, or the lock
duration):

- `moveToActive` records `claim:<token> → jobId`. A second run with the same token returns
  the job it claimed the first time.
- The scripts that settle a job record `<job>:settled → token`. A second run that finds the
  lock gone checks the receipt, and reports success when the earlier run was its own.

The receipts cost two small keys per job, alive for a minute. At 200 jobs/s that is about
24,000 keys, which the soak test shows as a flat plateau.

**Waiting for work never blocks shutdown.** The worker races `BLPOP` against its stop
signal, and does not rely on the connection rejecting the command.

**Redis crashes need persistence.** The code keeps no state outside Redis, so after a crash
the queue is exactly as durable as Redis is configured to be. The chaos suite SIGKILLs a
Redis running with `appendonly yes` and `appendfsync always` in the middle of processing,
keeps it down for two seconds, then restarts it. Every job completes exactly once.
`appendfsync everysec` (the Redis default when AOF is on) can lose up to a second of writes
in a crash, and running without persistence loses everything.

**Redis Cluster** works because every key of a queue shares one hash slot (ADR 0002):
scripts and pipelines never cross slots. Only two things change:

- SCAN is node-local, so `discover()` and `obliterate()` scan every master.
- The dashboard cannot read the events streams of several queues with one `XREAD`, because
  they live in different slots. On a cluster it reads each stream separately and polls
  every 250 ms instead of blocking.

Worker processes started by the supervisor receive the cluster settings as a plain
`{ cluster: nodes }` object, because a `Cluster` instance cannot cross a process boundary.

## Consequences

- CI runs three suites: the regular tests (including connection chaos), a Redis Cluster
  suite, and a suite that crashes Redis.
- Delivery stays at-least-once (ADR 0003). The receipts remove the duplicates and false
  alarms caused by resends. They do not change what happens when a worker genuinely loses
  a job.
- Deployments must turn on Redis persistence to survive Redis crashes. The docs say so
  directly.
