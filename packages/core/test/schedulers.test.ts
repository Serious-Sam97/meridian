import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { type Job, Queue, Worker } from '../src/index.js';
import { createRedis, uniqueQueueName, waitFor } from './helpers.js';

describe('job schedulers', () => {
  let redis: Redis;
  let queue: Queue<{ report: string }>;
  const workers: Worker<{ report: string }>[] = [];

  function startWorker(onJob: (job: Job<{ report: string }>) => void): Worker<{ report: string }> {
    const worker = new Worker<{ report: string }>(
      queue.name,
      async (job) => {
        onJob(job);
      },
      { connection: redis, blockTimeout: 1_000 },
    );
    workers.push(worker);
    return worker;
  }

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName('sched'), { connection: redis });
  });

  afterEach(async () => {
    await Promise.all(workers.splice(0).map((w) => w.close()));
    await queue.obliterate();
  });

  it('adds a job on every run of an every-schedule', async () => {
    await queue.upsertScheduler(
      'digest',
      { every: 200 },
      { name: 'digest', data: { report: 'daily' } },
    );
    const runs: number[] = [];
    startWorker((job) => runs.push(job.opts.repeat?.runAt ?? 0));

    await waitFor(() => runs.length >= 4, { timeout: 5_000 });
    expect(runs.every((runAt) => runAt % 200 === 0)).toBe(true);
    // Runs move forward by whole intervals: one apart normally, more only if a
    // run was picked up so late (under load) that the next slot had passed.
    const gaps = runs.slice(1).map((runAt, i) => runAt - (runs[i] ?? 0));
    expect(gaps.every((gap) => gap > 0 && gap % 200 === 0)).toBe(true);
    expect(gaps).toContain(200);
  });

  it('never runs the same occurrence twice with several workers', async () => {
    await queue.upsertScheduler(
      'digest',
      { every: 100 },
      { name: 'digest', data: { report: 'x' } },
    );
    const runs: number[] = [];
    for (let i = 0; i < 4; i++) startWorker((job) => runs.push(job.opts.repeat?.runAt ?? 0));

    await waitFor(() => runs.length >= 10, { timeout: 5_000 });
    expect(new Set(runs).size).toBe(runs.length);
  });

  it('is idempotent: upserting the same scheduler twice keeps one pending run', async () => {
    await queue.upsertScheduler(
      'digest',
      { every: 60_000 },
      { name: 'digest', data: { report: 'a' } },
    );
    await queue.upsertScheduler(
      'digest',
      { every: 60_000 },
      { name: 'digest', data: { report: 'a' } },
    );

    expect((await queue.getJobCounts()).delayed).toBe(1);
    expect(await queue.getSchedulers()).toHaveLength(1);
  });

  it('replaces the pending run when the schedule changes', async () => {
    // Two patterns whose next runs can never coincide, whatever the current time.
    const first = await queue.upsertScheduler(
      'digest',
      { pattern: '0 0 1 1 *', tz: 'UTC' },
      { name: 'digest' },
    );
    const second = await queue.upsertScheduler(
      'digest',
      { pattern: '0 0 1 7 *', tz: 'UTC' },
      { name: 'digest' },
    );

    expect(second.next).not.toBe(first.next);
    const delayed = await queue.getJobs('delayed');
    expect(delayed.map((j) => j.id)).toEqual([`repeat:digest:${second.next}`]);
  });

  it('applies the job template, including options', async () => {
    await queue.upsertScheduler(
      'digest',
      { every: 100 },
      { name: 'weekly-digest', data: { report: 'growth' }, options: { attempts: 3, priority: 2 } },
    );
    const seen: Job<{ report: string }>[] = [];
    startWorker((job) => seen.push(job));

    await waitFor(() => seen.length >= 1, { timeout: 3_000 });
    expect(seen[0]).toMatchObject({ name: 'weekly-digest', data: { report: 'growth' } });
    expect(seen[0]?.opts).toMatchObject({
      attempts: 3,
      priority: 2,
      repeat: { scheduler: 'digest' },
    });
  });

  it('stops adding jobs once removed', async () => {
    await queue.upsertScheduler('digest', { every: 100 }, { name: 'digest' });
    const runs: number[] = [];
    startWorker((job) => runs.push(job.opts.repeat?.runAt ?? 0));
    await waitFor(() => runs.length >= 2, { timeout: 3_000 });

    expect(await queue.removeScheduler('digest')).toBe(true);
    const countAfterRemove = runs.length;
    await new Promise((r) => setTimeout(r, 400));

    // At most the run that was already in progress.
    expect(runs.length - countAfterRemove).toBeLessThanOrEqual(1);
    expect(await queue.getSchedulers()).toEqual([]);
    expect(await queue.removeScheduler('digest')).toBe(false);
  });

  it('skips runs missed while no worker was running', async () => {
    await queue.upsertScheduler('digest', { every: 100 }, { name: 'digest' });
    // Nothing consumes the queue for ten intervals.
    await new Promise((r) => setTimeout(r, 1_000));

    const runs: number[] = [];
    const workerStartedAt = Date.now();
    startWorker((job) => runs.push(job.opts.repeat?.runAt ?? 0));
    await waitFor(() => runs.length >= 3, { timeout: 3_000 });

    // The overdue run is processed once...
    expect(runs[0]).toBeLessThan(workerStartedAt - 500);
    // ...then the schedule continues from now instead of replaying the nine
    // missed runs. (A run picked up late under load may skip a slot too.)
    expect(runs[1]).toBeGreaterThanOrEqual(workerStartedAt - 100);
    expect(runs.every((runAt) => runAt % 100 === 0)).toBe(true);
  });

  it('lists schedulers with their next run', async () => {
    await queue.upsertScheduler(
      'b',
      { pattern: '0 9 * * 1-5', tz: 'Europe/Lisbon' },
      { name: 'b' },
    );
    await queue.upsertScheduler('a', { every: 60_000 }, { name: 'a' });

    const schedulers = await queue.getSchedulers();
    expect(schedulers.map((s) => s.id)).toEqual(['a', 'b']);
    expect(schedulers[1]).toMatchObject({
      schedule: { pattern: '0 9 * * 1-5', tz: 'Europe/Lisbon' },
      iterations: 0,
      template: { name: 'b' },
    });
  });

  it('rejects invalid schedules and ids', async () => {
    await expect(
      queue.upsertScheduler('x', { pattern: 'not cron' }, { name: 'x' }),
    ).rejects.toThrow(RangeError);
    await expect(queue.upsertScheduler('x', { every: 0 }, { name: 'x' })).rejects.toThrow(
      RangeError,
    );
    await expect(queue.upsertScheduler('a:b', { every: 10 }, { name: 'x' })).rejects.toThrow(
      RangeError,
    );
  });
});
