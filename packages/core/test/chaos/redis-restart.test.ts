import { execFileSync } from 'node:child_process';
import { Redis } from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../../src/index.js';
import { uniqueQueueName, waitFor } from '../helpers.js';

// Runs only with `npm run test:chaos`, which starts a durable Redis
// (AOF, appendfsync always) that this test is allowed to kill.
const REDIS_URL = process.env.CHAOS_REDIS_URL;
const CONTAINER = process.env.CHAOS_REDIS_CONTAINER ?? 'meridian-redis-durable';

function docker(...args: string[]): void {
  execFileSync('docker', args, { stdio: 'ignore' });
}

describe.skipIf(!REDIS_URL)('Redis server crash', { timeout: 90_000 }, () => {
  let probe: Redis;

  beforeAll(() => {
    probe = new Redis(REDIS_URL ?? '', { maxRetriesPerRequest: null });
  });

  afterAll(async () => {
    probe.disconnect();
  });

  it('completes every job after Redis is SIGKILLed and restarted mid-run', async () => {
    const connection = REDIS_URL ?? '';
    const queue = new Queue<{ n: number }>(uniqueQueueName('crash'), { connection });
    const total = 400;
    await queue.addBulk(Array.from({ length: total }, (_, n) => ({ name: 'job', data: { n } })));

    const runs = new Map<number, number>();
    const worker = new Worker<{ n: number }>(
      queue.name,
      async (job) => {
        runs.set(job.data.n, (runs.get(job.data.n) ?? 0) + 1);
        await new Promise((r) => setTimeout(r, 20));
      },
      { connection, concurrency: 10, lockDuration: 5_000, stalledInterval: 1_000 },
    );
    const errors: string[] = [];
    worker.on('error', (err) => errors.push(err.message));

    // Crash Redis once a quarter of the jobs are done.
    await waitFor(() => runs.size >= total / 4, { timeout: 30_000 });
    const crashedAt = Date.now();
    docker('kill', CONTAINER);
    // Stay down long enough for every worker call to hit a dead connection.
    await new Promise((r) => setTimeout(r, 2_000));
    docker('start', CONTAINER);
    await waitFor(
      async () => {
        try {
          return (await probe.ping()) === 'PONG';
        } catch {
          return false;
        }
      },
      { timeout: 30_000, interval: 200 },
    );
    const downtime = Date.now() - crashedAt;

    await waitFor(async () => (await queue.getJobCounts()).completed === total, {
      timeout: 60_000,
      interval: 200,
    });
    const counts = await queue.getJobCounts();
    const duplicates = [...runs.values()].filter((n) => n > 1).length;
    console.log(
      `redis crash: down ${downtime}ms, ${duplicates} jobs ran more than once, ${errors.length} worker errors`,
    );

    expect(counts).toMatchObject({ completed: total, failed: 0, active: 0, waiting: 0 });
    expect(runs.size).toBe(total);

    await worker.close();
    await queue.obliterate();
    await queue.close();
  });
});
