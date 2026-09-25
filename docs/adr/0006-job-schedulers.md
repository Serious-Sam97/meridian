# 6. Job schedulers: each run schedules the next

- Status: accepted
- Date: 2026-09-25

## Context

Applications need jobs that repeat: every 5 minutes, or on a cron pattern such as "at 9:00
on weekdays in São Paulo". Any number of workers may be running. A repeat must never be
created twice, never be lost, and not flood the queue after downtime.

## Decision

A **scheduler** is a hash `scheduler:<id>` holding the schedule (`every` in ms, or a
`pattern` plus an optional `tz`), a job template and `next`, the time of the next run. A
`schedulers` sorted set indexes schedulers by `next` for listing.

**Each run schedules the following one**, using the queue's normal delayed jobs:

1. `upsertScheduler` computes the first run and adds a delayed job with id
   `repeat:<id>:<runAt>`.
2. When a worker takes a job that carries `opts.repeat = { scheduler, runAt }`, it computes
   the next run after `max(runAt, now)` and calls `advanceScheduler` before running the job.
3. `advanceScheduler` is a compare-and-set in one script. It only proceeds if the
   scheduler's `next` is still the `runAt` of the job being run. It then writes the new
   `next` and creates the next delayed job (reusing the shared `createJob` include).

Why this is safe:

- **No duplicates.** The compare-and-set lets exactly one worker advance a given run. The
  job id `repeat:<id>:<runAt>` makes creation idempotent even if a script is retried after a
  lost reply.
- **No lost repeats.** If a worker crashes after taking a run but before advancing, the
  stalled-job checker gives that run to another worker, which advances it.
- **No backlog after downtime.** The next run is computed from `max(runAt, now)`, so runs
  missed while no worker was up are skipped rather than replayed. This matches cron.
- **No extra polling.** Timing comes from the delayed-job machinery (ADR 0002), so a worker
  sleeps until exactly the next run.

`every` schedules are aligned to multiples of the interval, so calling `upsertScheduler` again
with the same settings (on every deploy, for example) keeps the same next run instead of
moving it. Cron patterns are evaluated in JavaScript with `cron-parser`, which handles time
zones and daylight saving time. Doing that in Lua would be impractical.

## Alternatives considered

- **A polling loop in every worker** reading due schedulers each second: simpler to explain,
  but it adds constant Redis traffic and up to a second of jitter, and it still needs a
  compare-and-set to avoid duplicates.
- **A single leader** elected to fire schedules: this needs leader election and failover, a
  new source of bugs that a compare-and-set avoids entirely.

## Consequences

- A scheduler only moves forward while at least one worker consumes its queue. A paused
  queue does not pile up runs, and each run happens only after the previous one has started.
- The next run is scheduled when a run *starts*, not when it finishes. A job that takes
  longer than the interval can therefore overlap with the next run. Handlers of frequent
  schedules should tolerate that, or use a longer interval.
