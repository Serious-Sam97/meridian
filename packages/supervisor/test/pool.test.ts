import { Queue } from '@meridian/core';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { ChildConfig } from '../src/child-config.js';
import { ProcessPool } from '../src/pool.js';
import type { FixtureData } from './fixtures/processor.js';
import {
  CHILD_EXEC_ARGV,
  createRedis,
  isAlive,
  PROCESSOR,
  REDIS_URL,
  uniqueQueueName,
  waitFor,
} from './helpers.js';

describe('ProcessPool', { timeout: 30_000 }, () => {
  let redis: Redis;
  let queue: Queue<FixtureData>;
  let pool: ProcessPool;

  function createPool(overrides: Partial<ChildConfig> = {}, restartDelay = 200): ProcessPool {
    pool = new ProcessPool({
      child: {
        queue: queue.name,
        processor: PROCESSOR,
        connection: REDIS_URL,
        concurrency: 2,
        shutdownTimeout: 2_000,
        workerOptions: { blockTimeout: 1_000, stalledInterval: 300, lockDuration: 1_000 },
        ...overrides,
      },
      execArgv: CHILD_EXEC_ARGV,
      restartDelay,
    });
    return pool;
  }

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(() => {
    queue = new Queue(uniqueQueueName('pool'), { connection: redis });
  });

  afterEach(async () => {
    await pool?.stop();
    await queue.obliterate();
  });

  it('starts processes that process jobs', async () => {
    createPool();
    await pool.scale(2);
    expect(pool.size).toBe(2);

    const jobs = await Promise.all([1, 2, 3, 4].map((n) => queue.add('double', { n })));
    await waitFor(async () => (await queue.getJobCounts()).completed === 4);

    const results = await Promise.all(
      jobs.map((j) => queue.getJob<{ pid: number; n: number }>(j.id)),
    );
    expect(results.map((r) => r?.returnValue?.n)).toEqual([2, 4, 6, 8]);
    for (const result of results) expect(pool.pids).toContain(result?.returnValue?.pid);
  });

  it('scales down by letting processes finish their jobs first', async () => {
    createPool();
    await pool.scale(1);
    const [pid] = pool.pids;
    const job = await queue.add('slow', { n: 1, sleep: 500 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'active');

    await pool.scale(0);

    expect(pool.size).toBe(0);
    expect(isAlive(pid ?? -1)).toBe(false);
    expect(await queue.getJobState(job.id)).toBe('completed');
  });

  it('replaces a process that crashes and recovers its job', async () => {
    createPool({ concurrency: 1 });
    const crashes: number[] = [];
    pool.on('crash', (pid) => crashes.push(pid));
    await pool.scale(1);
    const [firstPid] = pool.pids;

    // maxStalledCount 1: the crashing job is retried once, then failed.
    const crashing = await queue.add('crash', { n: 0, crash: true });
    await waitFor(() => crashes.length >= 1);
    expect(crashes[0]).toBe(firstPid);

    await waitFor(() => pool.size === 1 && pool.pids[0] !== firstPid);
    const healthy = await queue.add('double', { n: 5 });
    await waitFor(async () => (await queue.getJobState(healthy.id)) === 'completed');
    await waitFor(async () => (await queue.getJobState(crashing.id)) === 'failed', {
      timeout: 15_000,
    });
  });

  it('backs off exponentially when processes keep crashing', async () => {
    createPool({ processor: '/does/not/exist.js' }, 100);
    const delays: number[] = [];
    pool.on('crash', (_pid, _code, _signal, restartIn) => delays.push(restartIn));

    await pool.scale(1);
    await waitFor(() => delays.length >= 3);
    expect(delays.slice(0, 3)).toEqual([100, 200, 400]);
  });

  it('kills a process that does not exit after the shutdown timeout', async () => {
    pool = new ProcessPool({
      child: {
        queue: queue.name,
        processor: PROCESSOR,
        connection: REDIS_URL,
        concurrency: 1,
        shutdownTimeout: 100,
        workerOptions: { blockTimeout: 1_000 },
      },
      execArgv: CHILD_EXEC_ARGV,
      killGrace: 200,
    });
    await pool.scale(1);
    const [pid] = pool.pids;
    await new Promise((r) => setTimeout(r, 300));
    // Freeze the process so it cannot react to the shutdown message.
    process.kill(pid ?? -1, 'SIGSTOP');

    const startedAt = Date.now();
    await pool.scale(0);
    expect(Date.now() - startedAt).toBeLessThan(2_000);
    expect(isAlive(pid ?? -1)).toBe(false);
  });

  it('stop() ends every process and does not restart them', async () => {
    createPool();
    await pool.scale(3);
    const pids = pool.pids;
    expect(pids).toHaveLength(3);

    await pool.stop();
    await pool.scale(2);
    expect(pool.size).toBe(0);
    for (const pid of pids) expect(isAlive(pid)).toBe(false);
  });
});
