import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker, WorkerClosingError } from '../src/index.js';
import { createRedis, uniqueQueueName, waitFor } from './helpers.js';

describe('graceful shutdown', () => {
  let redis: Redis;
  let queue: Queue<{ n: number }>;
  const workers: Worker<{ n: number }>[] = [];

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName('shutdown'), { connection: redis });
  });

  afterEach(async () => {
    await Promise.all(workers.splice(0).map((w) => w.close({ timeout: 100 })));
    await queue.obliterate();
  });

  function hangingWorker(onAbort: (reason: unknown) => void): Worker<{ n: number }> {
    const worker = new Worker<{ n: number }>(
      queue.name,
      (_job, signal) =>
        new Promise((resolve) => {
          signal.addEventListener('abort', () => {
            onAbort(signal.reason);
            resolve(undefined);
          });
        }),
      { connection: redis, blockTimeout: 1_000 },
    );
    workers.push(worker);
    return worker;
  }

  it('releases unfinished jobs back to the queue when the timeout expires', async () => {
    const job = await queue.add('stuck', { n: 1 });
    let reason: unknown;
    const worker = hangingWorker((r) => {
      reason = r;
    });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'active');

    const startedAt = Date.now();
    await worker.close({ timeout: 200 });

    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(reason).toBeInstanceOf(WorkerClosingError);
    expect(await queue.getJobState(job.id)).toBe('waiting');
    expect(await redis.exists(queue.keys.lock(job.id))).toBe(0);
    // Releasing is not a failed attempt and not a stall.
    const stored = await queue.getJob(job.id);
    expect(stored?.attemptsMade).toBe(0);
    expect(await redis.hget(queue.keys.job(job.id), 'stalledCount')).toBeNull();
  });

  it('lets another worker pick up a released job immediately', async () => {
    const job = await queue.add('stuck', { n: 1 });
    const first = hangingWorker(() => {});
    await waitFor(async () => (await queue.getJobState(job.id)) === 'active');

    const done: string[] = [];
    const second = new Worker<{ n: number }>(queue.name, async () => 'ok', {
      connection: redis,
      blockTimeout: 10_000,
    });
    workers.push(second);
    second.on('completed', (j) => done.push(j.id));

    await first.close({ timeout: 100 });
    await waitFor(() => done.length === 1, { timeout: 1_000 });
    expect(done).toEqual([job.id]);
  });

  it('does not release jobs that finish within the timeout', async () => {
    const job = await queue.add('quick', { n: 1 });
    let started = false;
    const worker = new Worker<{ n: number }>(
      queue.name,
      async () => {
        started = true;
        await new Promise((r) => setTimeout(r, 100));
        return 'done';
      },
      { connection: redis, blockTimeout: 1_000 },
    );
    workers.push(worker);
    await waitFor(() => started);

    await worker.close({ timeout: 2_000 });
    expect(await queue.getJobState(job.id)).toBe('completed');
  });

  it('returns the same promise when close() is called twice', async () => {
    const worker = new Worker<{ n: number }>(queue.name, async () => 'ok', { connection: redis });
    workers.push(worker);
    expect(worker.close()).toBe(worker.close());
    await worker.close();
    expect(worker.isRunning).toBe(false);
  });
});
