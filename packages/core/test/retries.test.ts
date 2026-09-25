import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue, UnrecoverableError, Worker } from '../src/index.js';
import { createRedis, uniqueQueueName, waitFor } from './helpers.js';

describe('retries', () => {
  let redis: Redis;
  let queue: Queue<{ failUntil: number }>;
  let worker: Worker<{ failUntil: number }, string> | undefined;

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
    await worker?.close();
    worker = undefined;
    await queue.obliterate();
  });

  /** Fails until the job has been attempted `failUntil` times. */
  function flakyWorker(): Worker<{ failUntil: number }, string> {
    worker = new Worker<{ failUntil: number }, string>(
      queue.name,
      async (job) => {
        if (job.attemptsMade + 1 < job.data.failUntil) {
          throw new Error(`attempt ${job.attemptsMade + 1} failed`);
        }
        return `ok after ${job.attemptsMade + 1}`;
      },
      { connection: redis, blockTimeout: 1_000 },
    );
    return worker;
  }

  it('retries a failing job until it succeeds', async () => {
    const job = await queue.add('flaky', { failUntil: 3 }, { attempts: 5 });
    const retries: number[] = [];

    const w = flakyWorker();
    w.on('retrying', (j) => retries.push(j.attemptsMade));

    await waitFor(async () => (await queue.getJobState(job.id)) === 'completed');
    const stored = await queue.getJob(job.id);
    expect(stored?.returnValue).toBe('ok after 3');
    expect(stored?.attemptsMade).toBe(3);
    expect(retries).toEqual([1, 2]);
  });

  it('fails the job for good once attempts are exhausted', async () => {
    const job = await queue.add('flaky', { failUntil: 10 }, { attempts: 3 });
    const failed: string[] = [];
    let retries = 0;

    const w = flakyWorker();
    w.on('retrying', () => retries++);
    w.on('failed', (_j, err) => failed.push(err.message));

    await waitFor(() => failed.length === 1);
    expect(retries).toBe(2);
    expect(failed).toEqual(['attempt 3 failed']);

    const stored = await queue.getJob(job.id);
    expect(stored?.attemptsMade).toBe(3);
    expect(await queue.getJobState(job.id)).toBe('failed');
  });

  it('waits for the backoff delay before the next attempt', async () => {
    const job = await queue.add(
      'flaky',
      { failUntil: 2 },
      { attempts: 2, backoff: { type: 'fixed', delay: 300 } },
    );
    const delays: number[] = [];

    const w = flakyWorker();
    w.on('retrying', (_j, _err, delay) => delays.push(delay));

    await waitFor(() => delays.length === 1);
    expect(delays).toEqual([300]);
    expect(await queue.getJobState(job.id)).toBe('delayed');

    await waitFor(async () => (await queue.getJobState(job.id)) === 'completed');
    const stored = await queue.getJob(job.id);
    expect((stored?.processedOn ?? 0) - (stored?.timestamp ?? 0)).toBeGreaterThanOrEqual(290);
  });

  it('skips remaining attempts on an UnrecoverableError', async () => {
    const job = await queue.add('bad-input', { failUntil: 0 }, { attempts: 5 });
    const failed: string[] = [];

    worker = new Worker<{ failUntil: number }, string>(
      queue.name,
      async () => {
        throw new UnrecoverableError('invalid payload');
      },
      { connection: redis, blockTimeout: 1_000 },
    );
    worker.on('failed', (_j, err) => failed.push(err.name));

    await waitFor(() => failed.length === 1);
    expect(failed).toEqual(['UnrecoverableError']);
    expect((await queue.getJob(job.id))?.attemptsMade).toBe(1);
  });

  it('records a retrying event with the reason', async () => {
    await queue.add('flaky', { failUntil: 2 }, { attempts: 2 });
    flakyWorker();

    await waitFor(async () => (await queue.getJobCounts()).completed === 1);
    const events = await redis.xrange(queue.keys.events, '-', '+');
    const retrying = events.find(([, fields]) => fields[1] === 'retrying');
    expect(retrying?.[1]).toEqual([
      'event',
      'retrying',
      'jobId',
      '1',
      'delay',
      '0',
      'reason',
      'attempt 1 failed',
    ]);
  });
});
