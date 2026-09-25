import { Queue } from '@meridian/core';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { Supervisor, type SupervisorOptions, supervisorsKey } from '../src/index.js';
import type { FixtureData } from './fixtures/processor.js';
import {
  CHILD_EXEC_ARGV,
  createRedis,
  PROCESSOR,
  REDIS_URL,
  uniqueQueueName,
  waitFor,
} from './helpers.js';

describe('Supervisor', { timeout: 60_000 }, () => {
  let redis: Redis;
  const supervisors: Supervisor[] = [];
  const queues: Queue<FixtureData>[] = [];

  function setup(overrides: Partial<SupervisorOptions> = {}): {
    supervisor: Supervisor;
    busy: Queue<FixtureData>;
    idle: Queue<FixtureData>;
  } {
    const busy = new Queue<FixtureData>(uniqueQueueName('busy'), { connection: redis });
    const idle = new Queue<FixtureData>(uniqueQueueName('idle'), { connection: redis });
    queues.push(busy, idle);

    const supervisor = new Supervisor({
      name: uniqueQueueName('supervisor'),
      connection: REDIS_URL,
      queues: {
        [busy.name]: { processor: PROCESSOR },
        [idle.name]: { processor: PROCESSOR },
      },
      minProcesses: 1,
      maxProcesses: 4,
      maxShift: 1,
      balanceInterval: 200,
      shutdownTimeout: 1_000,
      workerOptions: { blockTimeout: 500 },
      execArgv: CHILD_EXEC_ARGV,
      ...overrides,
    });
    supervisors.push(supervisor);
    return { supervisor, busy, idle };
  }

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  afterEach(async () => {
    await Promise.all(supervisors.splice(0).map((s) => s.stop()));
    await Promise.all(queues.splice(0).map((q) => q.obliterate()));
  });

  it('starts minProcesses per queue when the queues are idle', async () => {
    const { supervisor, busy, idle } = setup();
    await supervisor.start();
    expect(supervisor.allocation).toEqual({ [busy.name]: 1, [idle.name]: 1 });
  });

  it('gives a busy queue more processes and shrinks it back when drained', async () => {
    const { supervisor, busy, idle } = setup();
    const history: number[] = [];
    supervisor.on('scaled', (allocation) => history.push(allocation[busy.name] ?? 0));
    await supervisor.start();

    for (let n = 0; n < 60; n++) await busy.add('work', { n, sleep: 100 });

    // One process at a time (maxShift 1) up to the 3 the idle queue leaves free.
    await waitFor(() => supervisor.allocation[busy.name] === 3, { timeout: 15_000 });
    expect(supervisor.allocation[idle.name]).toBe(1);
    // The first event is the initial allocation made by start().
    expect(history.slice(0, 3)).toEqual([1, 2, 3]);

    await waitFor(async () => (await busy.getJobCounts()).completed === 60, { timeout: 30_000 });
    await waitFor(() => supervisor.allocation[busy.name] === 1, { timeout: 15_000 });
  });

  it('splits processes evenly with the simple strategy', async () => {
    const { supervisor, busy, idle } = setup({ balance: 'simple' });
    await supervisor.start();
    expect(supervisor.allocation).toEqual({ [busy.name]: 2, [idle.name]: 2 });
  });

  it('publishes a heartbeat and removes it on stop', async () => {
    const { supervisor, busy } = setup();
    await supervisor.start();

    const [status] = (await Supervisor.list(redis)).filter((s) => s.name === supervisor.name);
    expect(status?.strategy).toBe('auto');
    expect(status?.queues[busy.name]).toEqual({ processes: 1, target: 1 });

    await supervisor.stop();
    expect(await redis.hexists(supervisorsKey(), supervisor.name)).toBe(0);
  });

  it('ignores heartbeats older than maxAge', async () => {
    await redis.hset(
      supervisorsKey(),
      'ghost',
      JSON.stringify({ name: 'ghost', updatedAt: Date.now() - 60_000, queues: {} }),
    );
    const names = (await Supervisor.list(redis)).map((s) => s.name);
    expect(names).not.toContain('ghost');
    await redis.hdel(supervisorsKey(), 'ghost');
  });

  it('rejects a configuration that cannot give every queue minProcesses', () => {
    expect(() => setup({ minProcesses: 3, maxProcesses: 4 })).toThrow(/maxProcesses/);
  });
});
