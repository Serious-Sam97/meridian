# Meridian

[![CI](https://github.com/Serious-Sam97/meridian/actions/workflows/ci.yml/badge.svg)](https://github.com/Serious-Sam97/meridian/actions/workflows/ci.yml)

A Redis-backed job queue for Node.js, with a Laravel Horizon-style **supervisor** that
scales worker processes by workload and a live **dashboard**.

Meridian is built around one rule: **a job is never lost and never in two states at once**,
even when workers are `SIGKILL`ed in the middle of a deploy. Every state transition is one
atomic Lua script, and the test suite kills real worker processes to prove it.

| Package | Description |
|---|---|
| [`@meridian/core`](packages/core) | Queues and workers: priorities, delays, retries with backoff, locks, stalled-job recovery, graceful shutdown, metrics |
| [`@meridian/supervisor`](packages/supervisor) | Pools of worker processes per queue, balanced by workload (`simple` / `auto`), with a CLI |
| [`@meridian/dashboard`](packages/dashboard) | Web UI and JSON API: throughput, queues, failed jobs, supervisors, live events |

## Try it

```bash
npm install
npm run redis:up
npm run demo        # supervisor + dashboard + a producer sending bursts of work
```

Open http://127.0.0.1:3000 and watch the supervisor move processes to the queue that
receives a burst ([demo walkthrough](examples/demo)).

## Quick look

```ts
// producer.ts
import { Queue } from '@meridian/core';

const emails = new Queue<{ to: string }>('emails', { connection: process.env.REDIS_URL });
await emails.add('welcome', { to: 'ada@example.com' }, {
  jobId: 'welcome:ada', // idempotent
  attempts: 5,
  backoff: { type: 'exponential', delay: 1_000, jitter: 0.2 },
});
```

```ts
// jobs/send-email.ts: one module per queue
import { UnrecoverableError, type Job } from '@meridian/core';

export default async function (job: Job<{ to: string }>, signal: AbortSignal) {
  if (!job.data.to.includes('@')) throw new UnrecoverableError('invalid address');
  return sendEmail(job.data.to, { signal }); // aborted if this worker loses the job
}
```

```js
// meridian.config.js, then: npx meridian-supervisor
export default {
  queues: {
    emails: { processor: './jobs/send-email.js', concurrency: 10 },
    images: { processor: './jobs/resize-image.js', concurrency: 2 },
  },
  balance: 'auto',
  maxProcesses: 10,
};
```

```bash
npx meridian-dashboard   # or mount createDashboard().handler in Express
```

You can also run a `Worker` directly in your own process, without the supervisor. See
[`@meridian/core`](packages/core).

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

- **Every arrow is one Lua script.** A job enters `active` together with its lock, so an
  active job without a lock can only mean its worker died. That is how the stalled checker
  recovers jobs safely.
- **At-least-once delivery.** Workers renew their locks while a job runs. A worker that loses
  one (it froze, or its lock expired) has its handler aborted and its result discarded.
  **Handlers must be idempotent.**
- **One clock.** Delays, lock expiry and metrics use Redis' `TIME`, so clock skew between
  machines does not matter.
- **No polling.** Idle workers block on Redis and wake the moment a job is added, or exactly
  when the next delayed job is due.
- **The supervisor** runs one pool of child processes per queue. Every few seconds it gives
  each queue a share of `maxProcesses` proportional to its time to clear
  (`waiting × avg runtime`), capped at what the queue can keep busy, moving by at most
  `maxShift` per round.

## Benchmarks

Same Redis, same payload, one worker at concurrency 50, no-op jobs
([method and caveats](bench)):

| Library | Enqueue (jobs/s) | Process (jobs/s) | Latency p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|
| Meridian | 101,832 | 34,687 | 0.45 | 0.92 | 1.71 |
| BullMQ 6.3 | 59,956 | 35,067 | 0.48 | 0.94 | 1.72 |

<sub>Node 24, Redis 7.4 in Docker, Apple M4 Pro. Run it yourself: `npm run bench`.</sub>

The first run of this benchmark measured Meridian at 6,400 jobs/s. That exposed a worker
loop in which all concurrency slots shared one fetch round trip. The fix is in
[`0d28e0e`](../../commit/0d28e0e).

## Testing

```bash
npm run check   # lint + typecheck + 115 tests against a real Redis
```

There are no Redis mocks: the Lua scripts are the core of the system, so mocking Redis would
only test the mocks. Some tests worth reading:

- [SIGKILL a worker process holding jobs](packages/core/test/stalled.test.ts) and assert
  that another worker completes every one of them
- [a randomised invariant test](packages/supervisor/test/balancer.test.ts) over 2,000
  inputs for the balancer. It found a case where a new queue pushed the total over
  `maxProcesses`.
- [process pool tests](packages/supervisor/test/pool.test.ts): crash loops with backoff,
  and a `SIGSTOP`ped child that must be killed
- [dashboard security](packages/dashboard/test/api.test.ts): CSRF header, `authorize`
  hook, malformed URLs

## Architecture decisions

- [0001: Record architecture decisions](docs/adr/0001-record-architecture-decisions.md)
- [0002: Redis data model and atomic state transitions](docs/adr/0002-redis-data-model.md)
- [0003: At-least-once delivery with token-based locks](docs/adr/0003-delivery-guarantees-and-locks.md)
- [0004: Supervisor: process pools balanced by workload](docs/adr/0004-supervisor-and-balancing.md)
- [0005: Dashboard: a framework-free handler with SSE and no build step](docs/adr/0005-dashboard.md)

## Development

Requires Node 22+ and Docker.

```bash
npm install
npm run redis:up   # Redis 7 on localhost:6379
npm run check      # lint, typecheck, tests
npm run build      # compile all packages to dist/
```

Packages resolve each other's TypeScript sources through a `@meridian/source` export
condition, so there is no build step between editing `core` and testing `supervisor`.

## License

MIT
