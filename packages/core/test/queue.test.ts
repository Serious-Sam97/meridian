import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue } from '../src/index.js';
import { createRedis, uniqueQueueName } from './helpers.js';

describe('Queue', () => {
  let redis: Redis;
  let queue: Queue<{ to: string }>;

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
    await queue.obliterate();
  });

  it('adds a job and reads it back', async () => {
    const added = await queue.add('send', { to: 'a@example.com' }, { attempts: 3 });

    const job = await queue.getJob(added.id);
    expect(job?.name).toBe('send');
    expect(job?.data).toEqual({ to: 'a@example.com' });
    expect(job?.opts.attempts).toBe(3);
    expect(job?.attemptsMade).toBe(0);
    expect(await queue.getJobState(added.id)).toBe('waiting');
  });

  it('generates sequential ids', async () => {
    const a = await queue.add('send', { to: 'a' });
    const b = await queue.add('send', { to: 'b' });
    expect(Number(b.id)).toBe(Number(a.id) + 1);
  });

  it('puts jobs with a delay in the delayed set', async () => {
    const job = await queue.add('send', { to: 'a' }, { delay: 60_000 });
    expect(await queue.getJobState(job.id)).toBe('delayed');
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 1 });
  });

  it('is idempotent on a custom job id', async () => {
    await queue.add('send', { to: 'first' }, { jobId: 'welcome-42' });
    const dup = await queue.add('send', { to: 'second' }, { jobId: 'welcome-42' });

    expect(dup.data).toEqual({ to: 'first' });
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 1 });
  });

  it('orders the wait set by priority, then insertion order', async () => {
    const low = await queue.add('send', { to: 'low' }, { priority: 10 });
    const first = await queue.add('send', { to: 'first' });
    const second = await queue.add('send', { to: 'second' });

    const order = await redis.zrange(queue.keys.wait, '0', '-1');
    expect(order).toEqual([first.id, second.id, low.id]);
  });

  it('adds jobs in bulk, keeping their order', async () => {
    const jobs = await queue.addBulk(
      Array.from({ length: 2_500 }, (_, i) => ({ name: 'send', data: { to: `user${i}` } })),
    );
    expect(jobs).toHaveLength(2_500);
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 2_500 });

    const firstThree = await queue.getJobs('waiting', 0, 2);
    expect(firstThree.map((j) => j.data.to)).toEqual(['user0', 'user1', 'user2']);
  });

  it('applies default job options', async () => {
    const withDefaults = new Queue(queue.name, {
      connection: redis,
      defaultJobOptions: { attempts: 5 },
    });
    const job = await withDefaults.add('send', { to: 'a' }, { priority: 1 });
    expect(job.opts).toMatchObject({ attempts: 5, priority: 1 });
  });

  it('rejects invalid options', async () => {
    await expect(queue.add('send', { to: 'a' }, { priority: -1 })).rejects.toThrow(RangeError);
    await expect(queue.add('send', { to: 'a' }, { attempts: 0 })).rejects.toThrow(RangeError);
    await expect(queue.add('send', { to: 'a' }, { jobId: 'a:b' })).rejects.toThrow(RangeError);
  });

  it('records an event for each added job', async () => {
    await queue.add('send', { to: 'a' });
    const events = await redis.xrange(queue.keys.events, '-', '+');
    expect(events).toHaveLength(1);
    expect(events[0]?.[1]).toEqual(['event', 'waiting', 'jobId', '1', 'name', 'send']);
  });
});
