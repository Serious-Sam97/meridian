# @meridian/core

The queue engine: queues, workers, retries, delayed jobs and recovery of jobs held by
crashed workers. It is Redis-backed, with every state change done in one atomic Lua script.

```bash
npm install @meridian/core
```

## Queue

```ts
import { Queue } from '@meridian/core';

const queue = new Queue<{ to: string }>('emails', {
  connection: 'redis://localhost:6379', // URL, ioredis options, or an ioredis instance
  defaultJobOptions: { attempts: 3 },
});

await queue.add('welcome', { to: 'ada@example.com' }, {
  jobId: 'welcome:ada',        // idempotent: a second add with this id is a no-op
  delay: 60_000,               // run in a minute
  priority: 1,                 // lower runs first (default 0)
  attempts: 5,
  backoff: { type: 'exponential', delay: 1_000, jitter: 0.2 },
  removeOnComplete: 1_000,     // keep the newest 1,000 completed jobs
  removeOnFail: false,         // keep failed jobs for inspection
});

await queue.addBulk([{ name: 'welcome', data: { to: 'grace@example.com' } }]);
```

| Method | Description |
|---|---|
| `add(name, data, options?)` / `addBulk(jobs)` | Add jobs |
| `getJob(id)` / `getJobState(id)` | Read a job and where it is |
| `getJobs(state, start?, end?)` | List jobs by state with paging |
| `getJobCounts()` | Waiting, delayed, active, completed and failed counts |
| `getMetrics(minutes?)` | Per-minute completed/failed counts, average runtime and wait |
| `retryJob(id)` / `retryAllFailed()` | Move failed jobs back to the queue with fresh attempts |
| `removeJob(id)` | Delete a job that is not running |
| `pause()` / `resume()` / `isPaused()` | Stop and restart handing out jobs |
| `Queue.discover(client, prefix?)` | Names of all queues |
| `obliterate()` | Delete every key of the queue (tests, local development) |

## Worker

```ts
import { UnrecoverableError, Worker } from '@meridian/core';

const worker = new Worker<{ to: string }, { id: string }>(
  'emails',
  async (job, signal) => {
    if (!job.data.to.includes('@')) throw new UnrecoverableError('invalid address');
    return sendEmail(job.data.to, { signal });
  },
  { connection: 'redis://localhost:6379', concurrency: 10 },
);

worker.on('completed', (job, result) => {});
worker.on('retrying', (job, error, delay) => {});
worker.on('failed', (job, error) => {});      // final failure only
worker.on('stalled', (jobIds) => {});         // recovered from a dead worker
worker.on('error', (error) => {});            // Redis or lock problems, not job failures

process.on('SIGTERM', () => worker.close({ timeout: 10_000 }));
```

| Option | Default | Description |
|---|---|---|
| `concurrency` | `1` | Jobs processed in parallel |
| `lockDuration` | `30000` | Lock TTL in ms; renewed every `lockDuration / 2` |
| `stalledInterval` | `30000` | How often to look for jobs left behind by dead workers (use the same value on all workers) |
| `maxStalledCount` | `1` | Stalls allowed before a job is failed instead of recovered |
| `blockTimeout` | `5000` | Longest idle sleep before polling again |

### Delivery guarantees

Meridian is **at-least-once** ([ADR 0003](../../docs/adr/0003-delivery-guarantees-and-locks.md)).
Jobs are never lost, but a job can run twice. That happens, for example, when a worker's
event loop is blocked for longer than `lockDuration`. **Make handlers idempotent.** When
a worker loses a job, the `signal` passed to your handler is aborted and the result of
that attempt is discarded.

`close({ timeout })` stops taking jobs, waits for running ones, and after the timeout hands
unfinished jobs back to the queue. That does not count as an attempt or a stall.
