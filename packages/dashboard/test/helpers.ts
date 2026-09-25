import { randomUUID } from 'node:crypto';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Redis } from 'ioredis';
import type { Dashboard } from '../src/index.js';

export const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

export function uniqueName(label = 'test'): string {
  return `${label}-${randomUUID().slice(0, 8)}`;
}

export function createRedis(): Redis {
  return new Redis(REDIS_URL, { maxRetriesPerRequest: null });
}

/** Serves a dashboard on a random port and returns its base URL. */
export async function serve(dashboard: Dashboard): Promise<{ url: string; server: Server }> {
  const server = createServer((req, res) => void dashboard.handler(req, res));
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return { url: `http://127.0.0.1:${port}`, server };
}

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
