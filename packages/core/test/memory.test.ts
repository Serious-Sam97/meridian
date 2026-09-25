import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { describe, expect, it } from 'vitest';
import { REDIS_URL, uniqueQueueName } from './helpers.js';

const run = promisify(execFile);

describe('worker memory', { timeout: 90_000 }, () => {
  it('does not grow with the number of jobs processed', async () => {
    // A worker used to keep ~360 bytes per job alive: every wait raced a
    // promise that stayed pending until close(), leaving a reaction behind.
    const { stdout } = await run(process.execPath, [
      '--expose-gc',
      '--import',
      'tsx',
      new URL('./fixtures/memory-probe.ts', import.meta.url).pathname,
      uniqueQueueName('memory'),
      REDIS_URL,
    ]);
    const { before, after } = JSON.parse(stdout.trim().split('\n').at(-1) ?? '{}') as {
      before: number;
      after: number;
    };
    const growthMb = (after - before) / 1024 / 1024;
    // With the leak this probe grew ~1.5 MB; without it, ~0.05 MB.
    expect(growthMb).toBeLessThan(0.5);
  });
});
