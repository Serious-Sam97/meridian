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
  tags: ['customer:42'],       // find it later with getJobsByTag
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
| `upsertScheduler(id, schedule, template)` | Add a job on a schedule (see below) |
| `removeScheduler(id)` / `getSchedulers()` | Manage schedulers |
| `setRateLimit({ max, duration })` / `getRateLimit()` | Limit how many jobs start per window, across all workers |
| `getJobsByTag(tag)` / `countJobsByTag(tag)` | Find jobs by tag |
| `Queue.discover(client, prefix?)` | Names of all queues |
| `obliterate()` | Delete every key of the queue (tests, local development) |

## Repeatable jobs

```ts
// Every 5 minutes (aligned to the clock)
await queue.upsertScheduler('sync-inventory', { every: 5 * 60_000 }, { name: 'sync' });

// Weekdays at 9:00 in São Paulo, with a job template
await queue.upsertScheduler(
  'morning-report',
  { pattern: '0 9 * * 1-5', tz: 'America/Sao_Paulo' },
  { name: 'report', data: { team: 'growth' }, options: { attempts: 3, tags: ['reports'] } },
);
```

Upserting is idempotent, so it can run on every deploy. Each run schedules the next, and a
compare-and-set makes sure exactly one worker does it, however many are running. Runs missed
while no worker was consuming the queue are skipped, like cron
([ADR 0006](../../docs/adr/0006-job-schedulers.md)).

## Rate limiting

```ts
await queue.setRateLimit({ max: 600, duration: 60_000 }); // the email provider's limit
```

The limit is stored in Redis, so it is shared by every worker and applies to running workers
immediately. It is a fixed window: at most `max` jobs *start* per window.

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
