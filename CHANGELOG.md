# Changelog

All packages are versioned together. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

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

[0.1.0]: https://github.com/Serious-Sam97/meridian/releases/tag/v0.1.0
