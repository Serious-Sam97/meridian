import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';

export const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

export const PROCESSOR = fileURLToPath(new URL('./fixtures/processor.ts', import.meta.url));

/** Lets child processes run TypeScript and resolve workspace packages from source. */
export const CHILD_EXEC_ARGV = ['--import', 'tsx', '--conditions=@meridian/source'];

export function uniqueQueueName(label = 'test'): string {
  return `${label}-${randomUUID().slice(0, 8)}`;
}

export function createRedis(): Redis {
  return new Redis(REDIS_URL, { maxRetriesPerRequest: null });
}

export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  { timeout = 10_000, interval = 25 } = {},
): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`waitFor timed out after ${timeout}ms`);
    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}

export function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
