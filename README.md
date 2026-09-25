# Meridian

A Redis-backed job queue for Node.js, with a Horizon-style supervisor and dashboard on the way.

Meridian is built around one rule: **a job is never lost and never in two states at once**,
even when workers are `SIGKILL`ed in the middle of a deploy. Every state transition is a
single atomic Lua script, and the test suite kills real worker processes to prove it.

## Status

| Package | Status | Description |
|---|---|---|
| [`@meridian/core`](packages/core) | ✅ working | Queues, workers, retries, delayed jobs, stalled-job recovery |
| `@meridian/supervisor` | 🚧 planned | Worker pools that scale with queue depth (like Laravel Horizon's balancing) |
| `@meridian/dashboard` | 🚧 planned | Throughput, wait times, failed-job inspection and retry |

## Quick look

```ts
import { Queue, Worker, UnrecoverableError } from '@meridian/core';

const emails = new Queue<{ to: string }>('emails', { connection: 'redis://localhost:6379' });

await emails.add(
  'welcome',
  { to: 'ada@example.com' },
  {
    jobId: 'welcome:ada', // idempotent: adding the same id twice is a no-op
    attempts: 5,
    backoff: { type: 'exponential', delay: 1_000, jitter: 0.2 },
    priority: 1, // lower runs first
    removeOnComplete: 1_000, // keep the last 1,000 completed jobs
  },
);

const worker = new Worker<{ to: string }>(
  'emails',
  async (job, signal) => {
    if (!job.data.to.includes('@')) throw new UnrecoverableError('invalid address');
    await sendEmail(job.data.to, { signal }); // aborted if this worker loses the job
    return { sentAt: Date.now() };
  },
  { connection: 'redis://localhost:6379', concurrency: 10 },
);

worker.on('retrying', (job, err, delay) => console.warn(`${job.id} retry in ${delay}ms`, err));
worker.on('failed', (job, err) => console.error(`${job.id} failed for good`, err));

process.on('SIGTERM', () => worker.close({ timeout: 10_000 }));
```

## Features

- **Atomic state transitions**: each move between states is one Lua script ([ADR 0002](docs/adr/0002-redis-data-model.md))
- **At-least-once delivery** with token locks that are renewed while the job runs ([ADR 0003](docs/adr/0003-delivery-guarantees-and-locks.md))
- **Stalled-job recovery**: jobs held by a crashed worker go back to the queue, and a job that keeps crashing its worker is failed instead of looping forever
- **Priorities, delays and retries** with fixed or exponential backoff plus jitter
- **Instant wake-up**: idle workers block on Redis and pick up new jobs with no polling delay, and they sleep only until the next delayed job is due
- **Graceful shutdown**: `close({ timeout })` drains running jobs, then hands unfinished ones back to the queue without counting a failed attempt
- **Pause and resume** per queue
- **Event stream**: every transition is appended to a capped Redis stream, for the upcoming dashboard
- A single Redis clock for delays and lock expiry, so clock skew between worker machines does not matter

> **Handlers must be idempotent.** At-least-once means a job can run twice, for example
> when a worker freezes for longer than `lockDuration`. Use `job.id` as an idempotency key
> for side effects.

## How it works

```
             add()                       moveToActive (atomic)
   ┌──────────────────────┐        ┌──────────────────────────────┐
   │                      ▼        │                              ▼
 client ──► delayed ──► wait ──────┘                  active + lock:<token> (TTL)
           (zset by    (zset by                         │   │   │
            due time)   priority,seq)                   │   │   └─ lock expired ─► moveStalledJobs ─► wait
                ▲                                       │   └─ failed, attempts left ─► retryJob ─► delayed / wait
                └──────────── backoff ──────────────────┘
                                                        └─ done ─► moveToFinished ─► completed / failed
```

Every arrow is one Lua script. A job can only enter `active` together with its lock, so an
active job without a lock can only mean its worker died. That is how the stalled checker can
recover jobs safely.

## Development

Requires Node 22+ and Docker.

```bash
npm install
npm run redis:up   # Redis 7 on localhost:6379
npm run check      # lint + typecheck + tests
```

The tests run against a real Redis, with no mocks. The Lua scripts are the core of the
system, so tests that mocked Redis would only test the mocks.

## Architecture decisions

- [0001: Record architecture decisions](docs/adr/0001-record-architecture-decisions.md)
- [0002: Redis data model and atomic state transitions](docs/adr/0002-redis-data-model.md)
- [0003: At-least-once delivery with token-based locks](docs/adr/0003-delivery-guarantees-and-locks.md)

## License

MIT
