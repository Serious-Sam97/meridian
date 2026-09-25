import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src/index.js';
import { createRedis, uniqueQueueName, waitFor } from './helpers.js';

describe('queue management', () => {
  let redis: Redis;
  let queue: Queue<{ n: number; fail?: boolean }>;
  let worker: Worker<{ n: number; fail?: boolean }> | undefined;

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName('mgmt'), { connection: redis });
  });

  afterEach(async () => {
    await worker?.close();
    worker = undefined;
    await queue.obliterate();
  });

  function startWorker(): Worker<{ n: number; fail?: boolean }> {
    worker = new Worker<{ n: number; fail?: boolean }>(
      queue.name,
      async (job) => {
        await new Promise((r) => setTimeout(r, 20));
        if (job.data.fail) throw new Error(`job ${job.data.n} failed`);
        return job.data.n;
      },
      { connection: redis, blockTimeout: 1_000 },
    );
    return worker;
  }

  async function processAll(count: number): Promise<void> {
    startWorker();
    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed + counts.failed === count;
    });
  }

  describe('getJobs', () => {
    it('lists waiting jobs in the order they will run', async () => {
      await queue.add('job', { n: 1 }, { priority: 2 });
      await queue.add('job', { n: 2 });
      await queue.add('job', { n: 3 });

      const jobs = await queue.getJobs('waiting');
      expect(jobs.map((j) => j.data.n)).toEqual([2, 3, 1]);
    });

    it('lists finished jobs newest first and supports paging', async () => {
      for (let n = 1; n <= 5; n++) await queue.add('job', { n });
      await processAll(5);

      const firstPage = await queue.getJobs('completed', 0, 1);
      const secondPage = await queue.getJobs('completed', 2, 3);
      expect(firstPage.map((j) => j.data.n)).toEqual([5, 4]);
      expect(secondPage.map((j) => j.data.n)).toEqual([3, 2]);
    });

    it('returns an empty list for an empty state', async () => {
      expect(await queue.getJobs('failed')).toEqual([]);
    });
  });

  describe('retryJob', () => {
    it('moves a failed job back to the queue with fresh attempts', async () => {
      const job = await queue.add('job', { n: 1, fail: true });
      await processAll(1);
      expect(await queue.getJobState(job.id)).toBe('failed');
      await worker?.close();

      expect(await queue.retryJob(job.id)).toBe(true);
      expect(await queue.getJobState(job.id)).toBe('waiting');

      const stored = await queue.getJob(job.id);
      expect(stored?.attemptsMade).toBe(0);
      expect(stored?.failedReason).toBeUndefined();
    });

    it('returns false for a job that is not failed', async () => {
      const job = await queue.add('job', { n: 1 });
      expect(await queue.retryJob(job.id)).toBe(false);
      expect(await queue.getJobState(job.id)).toBe('waiting');
    });

    it('retries every failed job', async () => {
      for (let n = 1; n <= 3; n++) await queue.add('job', { n, fail: true });
      await processAll(3);
      await worker?.close();

      expect(await queue.retryAllFailed()).toBe(3);
      expect(await queue.getJobCounts()).toMatchObject({ failed: 0, waiting: 3 });
    });
  });

  describe('removeJob', () => {
    it('deletes a waiting job', async () => {
      const job = await queue.add('job', { n: 1 });
      expect(await queue.removeJob(job.id)).toBe(true);
      expect(await queue.getJob(job.id)).toBeUndefined();
      expect((await queue.getJobCounts()).waiting).toBe(0);
    });

    it('returns false for an unknown job', async () => {
      expect(await queue.removeJob('nope')).toBe(false);
    });

    it('refuses to delete an active job', async () => {
      const job = await queue.add('job', { n: 1 });
      await redis.set(queue.keys.lock(job.id), 'owner');
      await expect(queue.removeJob(job.id)).rejects.toThrow(/active/);
      expect(await queue.getJob(job.id)).toBeDefined();
    });
  });

  describe('getMetrics', () => {
    it('reports completed and failed jobs in the current minute', async () => {
      await queue.add('job', { n: 1 });
      await queue.add('job', { n: 2 });
      await queue.add('job', { n: 3, fail: true });
      await processAll(3);

      const metrics = await queue.getMetrics(5);
      expect(metrics).toHaveLength(5);

      // The last bucket is the current minute; allow for a minute boundary mid-test.
      const recent = metrics.slice(-2);
      const completed = recent.reduce((sum, b) => sum + b.completed, 0);
      const failed = recent.reduce((sum, b) => sum + b.failed, 0);
      expect({ completed, failed }).toEqual({ completed: 2, failed: 1 });

      const busy = recent.find((b) => b.completed + b.failed > 0);
      expect(busy?.avgRuntime).toBeGreaterThanOrEqual(15);
      expect(busy?.avgWait).toBeGreaterThanOrEqual(0);
    });

    it('returns buckets one minute apart, oldest first', async () => {
      const timestamps = (await queue.getMetrics(3)).map((b) => b.timestamp);
      const gaps = timestamps.slice(1).map((ts, i) => ts - (timestamps[i] ?? 0));
      expect(gaps).toEqual([60_000, 60_000]);
    });
  });

  it('discovers queues by name', async () => {
    await queue.add('job', { n: 1 });
    const other = new Queue(uniqueQueueName('mgmt'), { connection: redis });
    await other.add('job', { n: 1 });

    const names = await Queue.discover(redis);
    expect(names).toContain(queue.name);
    expect(names).toContain(other.name);
    await other.obliterate();
  });
});
