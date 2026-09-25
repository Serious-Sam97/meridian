import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker } from '../src/index.js';
import { createRedis, REDIS_URL, uniqueQueueName, waitFor } from './helpers.js';

/** Kills only the connections named `name`, so parallel test files are unaffected. */
async function killConnections(admin: Redis, name: string): Promise<number> {
  const list = (await admin.call('CLIENT', 'LIST', 'TYPE', 'normal')) as string;
  const ids = list
    .split('\n')
    .filter((line) => line.includes(` name=${name} `))
    .map((line) => /\bid=(\d+)/.exec(line)?.[1])
    .filter((id): id is string => id !== undefined);
  await Promise.all(ids.map((id) => admin.call('CLIENT', 'KILL', 'ID', id).catch(() => {})));
  return ids.length;
}

/**
 * Kills every client connection over and over while jobs are processed, as a
 * flaky network would. ioredis reconnects and resends commands whose reply
 * was lost, so scripts can run twice: no job may be lost or fail because of it.
 */
describe('connection chaos', { timeout: 60_000 }, () => {
  let admin: Redis;
  let queue: Queue<{ n: number }>;
  let worker: Worker<{ n: number }> | undefined;
  let connection: { host: string; port: number; connectionName: string };

  beforeAll(() => {
    admin = createRedis();
  });

  afterAll(async () => {
    await admin.quit();
  });

  beforeEach(() => {
    const { hostname, port } = new URL(REDIS_URL);
    const name = uniqueQueueName('chaos');
    // Named connections, duplicated by the worker with the same name.
    connection = { host: hostname, port: Number(port || 6379), connectionName: name };
    queue = new Queue(name, { connection });
  });

  afterEach(async () => {
    // Regression: close() used to hang when the blocking connection was
    // between reconnect attempts. The hook timeout would catch it.
    await worker?.close();
    await queue.obliterate();
    await queue.close();
  });

  it('completes every job while connections keep dropping', async () => {
    const total = 300;
    await queue.addBulk(Array.from({ length: total }, (_, n) => ({ name: 'job', data: { n } })));

    const runs = new Map<number, number>();
    const errors: Error[] = [];
    worker = new Worker<{ n: number }>(
      queue.name,
      async (job) => {
        runs.set(job.data.n, (runs.get(job.data.n) ?? 0) + 1);
        await new Promise((r) => setTimeout(r, 20 + Math.random() * 20));
      },
      {
        connection,
        concurrency: 10,
        lockDuration: 1_000,
        stalledInterval: 300,
        blockTimeout: 500,
      },
    );
    worker.on('error', (err) => errors.push(err));

    let kills = 0;
    const chaos = setInterval(() => {
      void killConnections(admin, connection.connectionName).then((n) => {
        kills += n;
      });
    }, 50);

    try {
      await waitFor(async () => (await queue.getJobCounts()).completed === total, {
        timeout: 45_000,
        interval: 100,
      });
    } finally {
      clearInterval(chaos);
    }

    const counts = await queue.getJobCounts();
    const duplicates = [...runs.values()].filter((n) => n > 1).length;
    console.log(
      `connection chaos: ${kills} kills, ${duplicates} jobs ran more than once, worker errors: ${
        errors.map((e) => e.message).join('; ') || 'none'
      }`,
    );

    expect(counts).toMatchObject({ completed: total, failed: 0, active: 0, waiting: 0 });
    expect(runs.size).toBe(total);
    expect(kills).toBeGreaterThan(5);
  });
});
