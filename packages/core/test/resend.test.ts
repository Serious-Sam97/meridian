import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue } from '../src/index.js';
import { runScript } from '../src/scripts.js';
import { createRedis, uniqueQueueName } from './helpers.js';

/**
 * ioredis resends a command whose reply was lost in a dropped connection, so
 * the same script call can run twice. These tests run each call twice with
 * the same token, as a resend would.
 */
describe('scripts under resends', () => {
  let redis: Redis;
  let queue: Queue<{ n: number }>;

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName('resend'), { connection: redis });
  });

  afterEach(async () => {
    await queue.obliterate();
  });

  const k = () => queue.keys;
  const claim = (token: string) =>
    runScript<[string, string[]] | [number]>(
      redis,
      'moveToActive',
      [k().wait, k().active, k().delayed, k().events, k().jobPrefix, k().meta, k().limiter],
      [token, 30_000, 1_000],
    );
  const finish = (jobId: string, token: string) =>
    runScript<number>(
      redis,
      'moveToFinished',
      [k().active, k().completed, k().events, k().jobPrefix, k().metricsPrefix, k().tagPrefix],
      [jobId, token, 'completed', 'returnValue', '"ok"', '', -1, 1_000],
    );

  it('returns the same job when a claim is resent', async () => {
    await queue.add('a', { n: 1 });
    await queue.add('b', { n: 2 });

    const first = await claim('token-1');
    const resent = await claim('token-1');

    expect(resent[0]).toBe(first[0]);
    // The second job was not claimed by the resend.
    expect(await queue.getJobCounts()).toMatchObject({ active: 1, waiting: 1 });
  });

  it('treats a resent finish as a success, not a lost lock', async () => {
    const job = await queue.add('a', { n: 1 });
    await claim('token-1');

    expect(await finish(job.id, 'token-1')).toBe(0);
    expect(await finish(job.id, 'token-1')).toBe(0);
    expect(await queue.getJobState(job.id)).toBe('completed');
  });

  it('still reports a lost lock to a different token', async () => {
    const job = await queue.add('a', { n: 1 });
    await claim('token-1');
    await finish(job.id, 'token-1');

    expect(await finish(job.id, 'someone-else')).toBe(-1);
  });
});
