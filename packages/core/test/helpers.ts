import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';

export const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

export function uniqueQueueName(label = 'test'): string {
  return `${label}-${randomUUID().slice(0, 8)}`;
}

export function createRedis(): Redis {
  return new Redis(REDIS_URL, { maxRetriesPerRequest: null });
}

/** Polls until the predicate is true, failing after the timeout. */
export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  { timeout = 5_000, interval = 20 } = {},
): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`waitFor timed out after ${timeout}ms`);
    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}
