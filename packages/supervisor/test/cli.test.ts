import { type ChildProcess, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { Queue } from '@meridian/core';
import type { Redis } from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { loadConfig, Supervisor, supervisorsKey } from '../src/index.js';
import type { FixtureData } from './fixtures/processor.js';
import {
  CHILD_EXEC_ARGV,
  createRedis,
  PROCESSOR,
  REDIS_URL,
  uniqueQueueName,
  waitFor,
} from './helpers.js';

const CLI = fileURLToPath(new URL('../src/cli.ts', import.meta.url));
const CONFIG = fileURLToPath(new URL('./fixtures/meridian.config.ts', import.meta.url));

describe('meridian-supervisor CLI', { timeout: 30_000 }, () => {
  let redis: Redis;

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('resolves processor paths relative to the config file', async () => {
    const [options] = await loadConfig(CONFIG);
    const [queue] = Object.values(options?.queues ?? {});
    expect(queue?.processor).toBe(PROCESSOR);
  });

  it('runs the configured supervisor and stops gracefully on SIGTERM', async () => {
    const queue = new Queue<FixtureData>(uniqueQueueName('cli'), { connection: redis });
    const name = uniqueQueueName('cli-supervisor');
    let output = '';

    const cli: ChildProcess = spawn(process.execPath, [...CHILD_EXEC_ARGV, CLI, CONFIG], {
      env: { ...process.env, REDIS_URL, TEST_QUEUE: queue.name, TEST_SUPERVISOR_NAME: name },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    cli.stdout?.on('data', (chunk) => {
      output += chunk;
    });
    cli.stderr?.on('data', (chunk) => {
      output += chunk;
    });
    const exited = new Promise<number | null>((resolve) => cli.once('exit', resolve));

    try {
      await waitFor(async () => (await Supervisor.list(redis)).some((s) => s.name === name));
      const job = await queue.add('double', { n: 21 });
      await waitFor(async () => (await queue.getJobState(job.id)) === 'completed');
      expect((await queue.getJob<{ n: number }>(job.id))?.returnValue?.n).toBe(42);

      cli.kill('SIGTERM');
      expect(await exited).toBe(0);
      expect(output).toContain(`started ${name}`);
      expect(output).toContain('stopped');
      expect(await redis.hexists(supervisorsKey(), name)).toBe(0);
    } finally {
      cli.kill('SIGKILL');
      await queue.obliterate();
    }
  });
});
