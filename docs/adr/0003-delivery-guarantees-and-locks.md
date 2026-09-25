# 3. At-least-once delivery with token-based locks

- Status: accepted
- Date: 2026-09-24

## Context

Workers crash, get `SIGKILL`ed during deploys, or stall on a blocked event loop.
The queue must decide what happens to a job whose worker disappeared. Exactly-once
execution is impossible when the handler has side effects outside Redis, so the
real choice is between at-most-once (a job can be lost) and at-least-once (a job
can run twice).

## Decision

Meridian guarantees **at-least-once** delivery.

- When a worker takes a job, it writes a random token to `job:<id>:lock` with a
  TTL (`lockDuration`, 30s by default).
- While the job runs, the worker renews the lock every `lockDuration / 2`. The
  renewal script checks the token first.
- Finishing a job (complete, fail or retry) also checks the token. If the token no
  longer matches, the worker has lost ownership: the job was recovered and
  possibly picked up by another worker. The finish is rejected, and the stale
  worker's result is discarded instead of overwriting the newer attempt.
- A **stalled-job checker** runs periodically, throttled across all workers by a
  `SET NX PX` key. It scans `active` for jobs with no lock. Each such job is moved
  back to `wait`, or to `failed` once it has stalled more than `maxStalledCount`
  times. The limit stops a job that always crashes its worker (a "poison pill")
  from looping forever.

## Consequences

- Handlers must be **idempotent**, or deduplicate on their side, for example with
  `job.id` as an idempotency key. The README states this in bold.
- A worker that is alive but blocks its event loop for longer than `lockDuration`
  loses its lock, and its job runs twice. `lockDuration` is configurable, and CPU
  heavy handlers should run in worker threads.
- Recovery latency after a crash is at most `lockDuration + stalledInterval`.
