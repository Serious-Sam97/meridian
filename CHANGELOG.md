# Changelog

All packages are versioned together. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.4.0] - 2026-09-25

### @meridian/core

- Redis Cluster: queues and workers accept a `Cluster` instance or a plain
  `{ cluster: nodes }` config; `discover()` and `obliterate()` scan every master
- Claiming and settling jobs is safe when ioredis resends a command after a dropped
  connection. A resent claim no longer strands a second job, and a resent settle no longer
  reports a false lost lock (ADR 0007)

### @meridian/supervisor

- Worker process recycling on `maxMemory`, `maxJobs` and `maxTime`, for the whole
  supervisor or per queue, reported as a recycle rather than a crash
- Cluster connections are passed to worker processes

### @meridian/dashboard

- Runs on Redis Cluster, reading each events stream separately there
- `meridian-dashboard --cluster host:port,...`

### Fixed

- `worker.close()` could hang forever after the blocking connection had been dropped

### Testing

- Connection chaos test (connections killed every 50 ms), Redis crash suite
  (`npm run test:chaos`), Redis Cluster suite (`npm run test:cluster`), both in CI
- Soak test (`npm run soak`) with results in `bench/soak-results.md`

## [0.3.0] - 2026-09-25

0.2.0 (rename and publish to npm) was postponed; the packages keep their names.

### @meridian/core

- Repeatable jobs: `upsertScheduler` with `{ every }` or `{ pattern, tz }` (cron with
  seconds and time zones), plus `removeScheduler`, `getScheduler` and `getSchedulers`.
  Each run schedules the next through a compare-and-set, so there are no duplicates with
  any number of workers (ADR 0006)
- Queue rate limits: `setRateLimit({ max, duration })`, shared by all workers and
  applied to running workers immediately
- Tags: `tags` job option, `getJobsByTag` and `countJobsByTag`. Every deletion path keeps
  the tag indexes in sync
- Lua scripts share helpers through `--@include` directives

### @meridian/dashboard

- Schedulers panel with removal, rate limit and scheduler badges on queues, tag search in
  the job browser, and tags on jobs
- API: `/api/schedulers`, scheduler deletion, `?tag=` job search, and
  `PUT`/`DELETE /api/queues/:name/rate-limit`

## [0.1.0] - 2026-09-25

First release.

### @meridian/core

- Queues with priorities, delays, custom idempotent job ids and bulk adds
- Workers with concurrency, token locks renewed while jobs run, and an `AbortSignal` for
  handlers whose lock is lost
- Retries with fixed or exponential backoff and jitter; `UnrecoverableError` to skip
  them
- Recovery of jobs held by crashed workers, with a limit that fails jobs which keep
  crashing their worker
- `close({ timeout })` hands unfinished jobs back to the queue without counting an
  attempt
- Pause and resume; job listing, retry and removal; per-minute metrics; an events stream

### @meridian/supervisor

- Process pools per queue with crash restarts (exponential backoff) and forced kills of
  hung processes
- `simple` and `auto` balancing with `minProcesses`, `maxProcesses` and `maxShift`
- Heartbeats for the dashboard, and the `meridian-supervisor` CLI

### @meridian/dashboard

- Overview, throughput chart, queues, job browser and job details with retry/delete,
  supervisors, and a live event feed over SSE
- An `authorize` hook, CSRF header check, strict CSP, and the `meridian-dashboard` CLI
  with Basic auth

[0.4.0]: https://github.com/Serious-Sam97/meridian/releases/tag/v0.4.0
[0.3.0]: https://github.com/Serious-Sam97/meridian/releases/tag/v0.3.0
[0.1.0]: https://github.com/Serious-Sam97/meridian/releases/tag/v0.1.0
