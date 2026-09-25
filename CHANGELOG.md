# Changelog

All packages are versioned together. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

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

[0.3.0]: https://github.com/Serious-Sam97/meridian/releases/tag/v0.3.0
[0.1.0]: https://github.com/Serious-Sam97/meridian/releases/tag/v0.1.0
