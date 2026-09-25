import { type ChildProcess, fork } from 'node:child_process';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Queue, Worker, type WorkerOptions } from '../src/index.js';
import { createRedis, REDIS_URL, uniqueQueueName, waitFor } from './helpers.js';

describe('stalled job recovery', () => {
  let redis: Redis;
  let queue: Queue<{ n: number }>;
  const workers: Worker<{ n: number }>[] = [];
  const children: ChildProcess[] = [];

  function startWorker(options: WorkerOptions = {}): Worker<{ n: number }> {
    const worker = new Worker<{ n: number }>(queue.name, async () => 'done', {
      connection: redis,
      blockTimeout: 1_000,
      stalledInterval: 200,
      ...options,
    });
    workers.push(worker);
    return worker;
  }

  /** Puts a job in active without a lock, as if its worker had died. */
  async function abandon(jobId: string): Promise<void> {
    await redis.zrem(queue.keys.wait, jobId);
    await redis.zadd(queue.keys.active, Date.now(), jobId);
  }

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName('stalled'), { connection: redis });
  });

  afterEach(async () => {
    for (const child of children.splice(0)) child.kill('SIGKILL');
    await Promise.all(workers.splice(0).map((w) => w.close()));
    await queue.obliterate();
  });

  it('puts an abandoned active job back in the queue and processes it', async () => {
    const job = await queue.add('job', { n: 1 });
    await abandon(job.id);
    const stalled: string[][] = [];

    const worker = startWorker();
    worker.on('stalled', (ids) => stalled.push(ids));

    await waitFor(async () => (await queue.getJobState(job.id)) === 'completed');
    expect(stalled).toEqual([[job.id]]);
    expect(await redis.hget(queue.keys.job(job.id), 'stalledCount')).toBe('1');
  });

  it('fails a job that stalls more than maxStalledCount times', async () => {
    const job = await queue.add('job', { n: 1 });
    await abandon(job.id);
    await redis.hset(queue.keys.job(job.id), 'stalledCount', 1);

    startWorker({ maxStalledCount: 1 });

    await waitFor(async () => (await queue.getJobState(job.id)) === 'failed');
    expect((await queue.getJob(job.id))?.failedReason).toMatch(/stalled more than the allowed 1/);
  });

  it('leaves jobs with a live lock alone', async () => {
    const job = await queue.add('job', { n: 1 });
    await abandon(job.id);
    await redis.set(queue.keys.lock(job.id), 'owner', 'PX', 5_000);

    startWorker();
    await new Promise((r) => setTimeout(r, 500));

    expect(await queue.getJobState(job.id)).toBe('active');
    expect(await redis.hget(queue.keys.job(job.id), 'stalledCount')).toBeNull();
  });

  it('runs the check at most once per interval across workers', async () => {
    const first = startWorker({ stalledInterval: 5_000 });
    await waitFor(async () => (await redis.exists(queue.keys.stalledCheck)) === 1);

    const job = await queue.add('job', { n: 1 });
    await abandon(job.id);
    const stalled: string[][] = [];
    const second = startWorker({ stalledInterval: 5_000 });
    for (const w of [first, second]) w.on('stalled', (ids) => stalled.push(ids));

    await new Promise((r) => setTimeout(r, 300));
    expect(stalled).toEqual([]);
    expect(await queue.getJobState(job.id)).toBe('active');
  });

  it('recovers every job after a worker process is killed with SIGKILL', async () => {
    const total = 10;
    for (let n = 0; n < total; n++) await queue.add('job', { n });

    // A worker in another process takes 5 jobs and hangs on them.
    const child = fork(
      new URL('./fixtures/slow-worker.ts', import.meta.url),
      [queue.name, REDIS_URL],
      {
        execArgv: ['--import', 'tsx'],
        stdio: 'ignore',
      },
    );
    children.push(child);
    const taken = new Set<string>();
    child.on('message', (msg: { jobId: string }) => taken.add(msg.jobId));
    await waitFor(() => taken.size === 5, { timeout: 10_000 });

    child.kill('SIGKILL');
    expect((await queue.getJobCounts()).active).toBe(5);

    // A healthy worker must finish all 10, including the 5 the dead one held.
    const completed = new Set<string>();
    const worker = startWorker({ concurrency: 5, lockDuration: 500 });
    worker.on('completed', (job) => completed.add(job.id));

    await waitFor(() => completed.size === total, { timeout: 10_000 });
    for (const id of taken) {
      expect(await redis.hget(queue.keys.job(id), 'stalledCount')).toBe('1');
    }
    expect(await queue.getJobCounts()).toMatchObject({
      waiting: 0,
      active: 0,
      completed: total,
      failed: 0,
    });
  });
});
