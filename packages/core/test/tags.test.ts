import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src/index.js';
import { createRedis, uniqueQueueName, waitFor } from './helpers.js';

describe('tags', () => {
  let redis: Redis;
  let queue: Queue<{ n: number }>;

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName('tags'), { connection: redis });
  });

  afterEach(async () => {
    await queue.obliterate();
  });

  async function processAll(): Promise<void> {
    const worker = new Worker<{ n: number }>(queue.name, async () => 'ok', {
      connection: redis,
      blockTimeout: 500,
    });
    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.waiting + counts.active === 0;
    });
    await worker.close();
  }

  it('finds jobs by tag, newest first', async () => {
    await queue.add('invoice', { n: 1 }, { tags: ['customer:42', 'billing'] });
    await queue.add('invoice', { n: 2 }, { tags: ['customer:7'] });
    await queue.add('receipt', { n: 3 }, { tags: ['customer:42'] });

    const jobs = await queue.getJobsByTag('customer:42');
    expect(jobs.map((j) => j.data.n)).toEqual([3, 1]);
    expect(await queue.countJobsByTag('billing')).toBe(1);
    expect(await queue.getJobsByTag('nobody')).toEqual([]);
  });

  it('keeps tagged jobs findable after they finish', async () => {
    await queue.add('invoice', { n: 1 }, { tags: ['customer:42'] });
    await processAll();

    const [job] = await queue.getJobsByTag('customer:42');
    expect(job?.returnValue).toBe('ok');
  });

  it('removes jobs from the index on every path that deletes them', async () => {
    // removeJob
    const removed = await queue.add('a', { n: 1 }, { tags: ['t'] });
    await queue.removeJob(removed.id);
    // removeOnComplete: true
    await queue.add('b', { n: 2 }, { tags: ['t'], removeOnComplete: true });
    // retention trimming
    for (let n = 3; n <= 5; n++) await queue.add('c', { n }, { tags: ['t'], removeOnComplete: 1 });
    await processAll();

    // Only the newest trimmed job is left, and the index agrees.
    const remaining = await queue.getJobsByTag('t');
    expect(remaining.map((j) => j.data.n)).toEqual([5]);
    expect(await queue.countJobsByTag('t')).toBe(1);
  });

  it('tags the jobs a scheduler creates', async () => {
    await queue.upsertScheduler(
      'nightly',
      { every: 60_000 },
      {
        name: 'report',
        options: { tags: ['reports'] },
      },
    );
    expect(await queue.countJobsByTag('reports')).toBe(1);

    // Removing the scheduler deletes its pending job and its index entry.
    await queue.removeScheduler('nightly');
    expect(await queue.countJobsByTag('reports')).toBe(0);
  });

  it('rejects invalid tags', async () => {
    await expect(queue.add('a', { n: 1 }, { tags: [''] })).rejects.toThrow(RangeError);
    await expect(
      queue.add('a', { n: 1 }, { tags: Array.from({ length: 21 }, (_, i) => `t${i}`) }),
    ).rejects.toThrow(RangeError);
  });
});
