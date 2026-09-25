import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { LockLostError, type Processor, Queue, Worker, type WorkerOptions } from '../src/index.js';
import { createRedis, uniqueQueueName, waitFor } from './helpers.js';

describe('Worker', () => {
  let redis: Redis;
  let queue: Queue<{ n: number }>;
  const workers: Worker<{ n: number }, unknown>[] = [];

  function startWorker<R>(
    processor: Processor<{ n: number }, R>,
    options: WorkerOptions = {},
  ): Worker<{ n: number }, R> {
    const worker = new Worker<{ n: number }, R>(queue.name, processor, {
      connection: redis,
      blockTimeout: 1_000,
      ...options,
    });
    workers.push(worker as Worker<{ n: number }, unknown>);
    return worker;
  }

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName(), { connection: redis });
  });

  afterEach(async () => {
    await Promise.all(workers.splice(0).map((w) => w.close()));
    await queue.obliterate();
  });

  it('processes a job and stores its return value', async () => {
    const job = await queue.add('double', { n: 21 });
    const completed: unknown[] = [];

    const worker = startWorker(async (j) => j.data.n * 2);
    worker.on('completed', (_job, result) => completed.push(result));

    await waitFor(() => completed.length === 1);
    expect(completed).toEqual([42]);

    const stored = await queue.getJob(job.id);
    expect(stored?.returnValue).toBe(42);
    expect(stored?.attemptsMade).toBe(1);
    expect(stored?.finishedOn).toBeGreaterThanOrEqual(stored?.processedOn ?? 0);
    expect(await queue.getJobState(job.id)).toBe('completed');
    expect(await redis.exists(queue.keys.lock(job.id))).toBe(0);
  });

  it('marks a job as failed when the processor throws', async () => {
    const job = await queue.add('explode', { n: 1 });
    const failed: string[] = [];

    const worker = startWorker(async () => {
      throw new Error('boom');
    });
    worker.on('failed', (_job, err) => failed.push(err.message));

    await waitFor(() => failed.length === 1);

    const stored = await queue.getJob(job.id);
    expect(stored?.failedReason).toBe('boom');
    expect(stored?.stacktrace).toContain('Error: boom');
    expect(await queue.getJobState(job.id)).toBe('failed');
  });

  it('processes jobs in priority order', async () => {
    await queue.add('job', { n: 3 }, { priority: 5 });
    await queue.add('job', { n: 1 });
    await queue.add('job', { n: 2 });
    const seen: number[] = [];

    startWorker(async (job) => {
      seen.push(job.data.n);
    });

    await waitFor(() => seen.length === 3);
    expect(seen).toEqual([1, 2, 3]);
  });

  it('never runs more jobs at once than its concurrency', async () => {
    for (let n = 0; n < 12; n++) await queue.add('job', { n });
    let running = 0;
    let peak = 0;
    let done = 0;

    startWorker(
      async () => {
        running++;
        peak = Math.max(peak, running);
        await new Promise((r) => setTimeout(r, 30));
        running--;
        done++;
      },
      { concurrency: 4 },
    );

    await waitFor(() => done === 12);
    expect(peak).toBe(4);
  });

  it('wakes up as soon as a job is added instead of waiting for the poll timeout', async () => {
    const done: number[] = [];
    startWorker(
      async (job) => {
        done.push(job.data.n);
      },
      { blockTimeout: 10_000 },
    );
    // Let the worker find the queue empty and go to sleep on the marker.
    await new Promise((r) => setTimeout(r, 100));

    const addedAt = Date.now();
    await queue.add('job', { n: 1 });
    await waitFor(() => done.length === 1, { timeout: 1_000 });
    expect(Date.now() - addedAt).toBeLessThan(500);
  });

  it('shares the work between several workers without duplicates', async () => {
    for (let n = 0; n < 50; n++) await queue.add('job', { n });
    const seen: number[] = [];

    for (let i = 0; i < 3; i++) {
      startWorker(
        async (job) => {
          seen.push(job.data.n);
        },
        { concurrency: 5 },
      );
    }

    await waitFor(() => seen.length === 50);
    expect(new Set(seen).size).toBe(50);
  });

  it('keeps only the newest completed jobs when removeOnComplete is a number', async () => {
    for (let n = 0; n < 5; n++) await queue.add('job', { n }, { removeOnComplete: 2 });
    let done = 0;
    startWorker(async () => {
      done++;
    });

    await waitFor(() => done === 5);
    await waitFor(async () => (await queue.getJobCounts()).completed === 2);
    expect(await redis.exists(queue.keys.job('1'))).toBe(0);
    expect(await redis.exists(queue.keys.job('5'))).toBe(1);
  });

  it('deletes the job when removeOnComplete is true', async () => {
    const job = await queue.add('job', { n: 1 }, { removeOnComplete: true });
    let done = false;
    startWorker(async () => {
      done = true;
    });

    await waitFor(() => done);
    await waitFor(async () => (await queue.getJob(job.id)) === undefined);
    expect((await queue.getJobCounts()).completed).toBe(0);
  });

  it('renews the lock of a job that outlives lockDuration', async () => {
    const job = await queue.add('slow', { n: 1 });
    let done = false;
    const errors: Error[] = [];

    const worker = startWorker(
      async () => {
        await new Promise((r) => setTimeout(r, 700));
        done = true;
      },
      { lockDuration: 200 },
    );
    worker.on('error', (err) => errors.push(err));

    await waitFor(() => done);
    await waitFor(async () => (await queue.getJobState(job.id)) === 'completed');
    expect(errors).toEqual([]);
  });

  it('aborts the processor and discards the result when the lock is lost', async () => {
    const job = await queue.add('slow', { n: 1 });
    let aborted: unknown;
    const errors: Error[] = [];
    const completed: string[] = [];

    const worker = startWorker(
      (_job, signal) =>
        new Promise((resolve) => {
          signal.addEventListener('abort', () => {
            aborted = signal.reason;
            resolve('late result');
          });
        }),
      { lockDuration: 200 },
    );
    worker.on('error', (err) => errors.push(err));
    worker.on('completed', (j) => completed.push(j.id));

    await waitFor(async () => (await queue.getJobState(job.id)) === 'active');
    // Simulate the stalled checker handing the job to another worker.
    await redis.set(queue.keys.lock(job.id), 'someone-else');

    await waitFor(() => aborted !== undefined);
    expect(aborted).toBeInstanceOf(LockLostError);
    expect(errors.map((e) => e.name)).toEqual(['LockLostError']);
    expect(completed).toEqual([]);
    expect(await queue.getJobState(job.id)).toBe('active');
  });

  it('close() waits for in-flight jobs to finish', async () => {
    await queue.add('slow', { n: 1 });
    let finished = false;
    let started = false;

    const worker = startWorker(async () => {
      started = true;
      await new Promise((r) => setTimeout(r, 200));
      finished = true;
    });

    await waitFor(() => started);
    await worker.close();
    expect(finished).toBe(true);
    expect(await queue.getJobState('1')).toBe('completed');
  });
});
