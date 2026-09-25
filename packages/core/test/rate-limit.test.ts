import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src/index.js';
import { createRedis, uniqueQueueName, waitFor } from './helpers.js';

describe('rate limiting', () => {
  let redis: Redis;
  let queue: Queue<{ n: number }>;
  const workers: Worker<{ n: number }>[] = [];

  function startWorker(onJob: () => void): void {
    workers.push(
      new Worker<{ n: number }>(
        queue.name,
        async () => {
          onJob();
        },
        { connection: redis, concurrency: 10, blockTimeout: 1_000 },
      ),
    );
  }

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName('rate'), { connection: redis });
  });

  afterEach(async () => {
    await Promise.all(workers.splice(0).map((w) => w.close()));
    await queue.obliterate();
  });

  it('starts at most max jobs per window, shared by all workers', async () => {
    await queue.setRateLimit({ max: 5, duration: 400 });
    await queue.addBulk(Array.from({ length: 15 }, (_, n) => ({ name: 'job', data: { n } })));
    const startedAt: number[] = [];
    // Three workers with concurrency 10 each would finish instantly without a limit.
    for (let i = 0; i < 3; i++) startWorker(() => startedAt.push(Date.now()));

    await waitFor(() => startedAt.length === 15, { timeout: 5_000 });
    // 15 jobs at 5 per window need three windows: at least two full windows pass.
    expect(Math.max(...startedAt) - Math.min(...startedAt)).toBeGreaterThanOrEqual(750);

    const limiterCount = Number((await redis.get(queue.keys.limiter)) ?? 0);
    expect(limiterCount).toBeLessThanOrEqual(5);
  });

  it('applies a limit set while workers are running, and lifting it takes effect at once', async () => {
    let done = 0;
    startWorker(() => done++);
    await queue.setRateLimit({ max: 2, duration: 60_000 });
    expect(await queue.getRateLimit()).toEqual({ max: 2, duration: 60_000 });

    await queue.addBulk(Array.from({ length: 6 }, (_, n) => ({ name: 'job', data: { n } })));
    await waitFor(() => done === 2);
    await new Promise((r) => setTimeout(r, 300));
    expect(done).toBe(2);

    // Lifting the limit wakes the idle worker; the old window's counter is ignored.
    await queue.setRateLimit(null);
    await waitFor(() => done === 6, { timeout: 3_000 });
    expect(await queue.getRateLimit()).toBeNull();
  });

  it('rejects invalid limits', async () => {
    await expect(queue.setRateLimit({ max: 0, duration: 1_000 })).rejects.toThrow(RangeError);
    await expect(queue.setRateLimit({ max: 1, duration: 0.5 })).rejects.toThrow(RangeError);
  });
});
