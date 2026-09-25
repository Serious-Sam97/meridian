import type { Server } from 'node:http';
import { Queue } from '@meridian/core';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDashboard, type Dashboard } from '../src/index.js';
import { createRedis, serve, uniqueName, waitFor } from './helpers.js';

/** Reads Server-Sent Events from a URL into an array until aborted. */
function listen(url: string): {
  events: Record<string, string>[];
  close: () => void;
  ready: Promise<void>;
} {
  const events: Record<string, string>[] = [];
  const controller = new AbortController();
  const ready = (async () => {
    const res = await fetch(url, { signal: controller.signal });
    const reader = res.body?.getReader();
    if (!reader) throw new Error('no body');
    const decoder = new TextDecoder();
    let buffer = '';
    void (async () => {
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) return;
          buffer += decoder.decode(value, { stream: true });
          let end = buffer.indexOf('\n\n');
          while (end !== -1) {
            const message = buffer.slice(0, end);
            buffer = buffer.slice(end + 2);
            const data = message.split('\n').find((line) => line.startsWith('data: '));
            if (data) events.push(JSON.parse(data.slice(6)));
            end = buffer.indexOf('\n\n');
          }
        }
      } catch {
        // aborted
      }
    })();
  })();
  return { events, close: () => controller.abort(), ready };
}

describe('dashboard events', () => {
  let redis: Redis;
  let prefix: string;
  let dashboard: Dashboard;
  let server: Server;
  let base: string;
  const queues: Queue[] = [];

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    prefix = uniqueName('events');
    dashboard = createDashboard({ connection: redis, prefix });
    ({ server, url: base } = await serve(dashboard));
  });

  afterEach(async () => {
    await dashboard.close();
    server.close();
    await Promise.all(queues.splice(0).map((q) => q.obliterate()));
  });

  it('streams job events of every queue as they happen', async () => {
    const emails = new Queue(uniqueName('emails'), { connection: redis, prefix });
    const reports = new Queue(uniqueName('reports'), { connection: redis, prefix });
    queues.push(emails, reports);
    // Old events must not be replayed to a new client.
    await emails.add('old', {});
    await reports.add('old', {});

    const stream = listen(`${base}/api/events`);
    await stream.ready;
    // Give the hub time to discover the queues and start reading.
    await new Promise((r) => setTimeout(r, 300));

    await emails.add('welcome', {});
    await reports.add('weekly', {}, { delay: 60_000 });

    await waitFor(() => stream.events.length >= 2);
    stream.close();

    expect(stream.events.map(({ queue, event, name }) => ({ queue, event, name }))).toEqual(
      expect.arrayContaining([
        { queue: emails.name, event: 'waiting', name: 'welcome' },
        { queue: reports.name, event: 'delayed', name: 'weekly' },
      ]),
    );
    expect(stream.events.some((e) => e.name === 'old')).toBe(false);
  });

  it('keeps streaming to a client that reconnects right after the last one left', async () => {
    const emails = new Queue(uniqueName('emails'), { connection: redis, prefix });
    queues.push(emails);
    await emails.add('warmup', {});

    const first = listen(`${base}/api/events`);
    await first.ready;
    await new Promise((r) => setTimeout(r, 300));
    first.close();

    const second = listen(`${base}/api/events`);
    await second.ready;
    await new Promise((r) => setTimeout(r, 300));
    await emails.add('after-reconnect', {});

    await waitFor(() => second.events.some((e) => e.name === 'after-reconnect'));
    second.close();
  });

  it('delivers every event of a burst, in order', async () => {
    const emails = new Queue(uniqueName('emails'), { connection: redis, prefix });
    queues.push(emails);
    await emails.add('warmup', {});

    const stream = listen(`${base}/api/events`);
    await stream.ready;
    await new Promise((r) => setTimeout(r, 300));

    for (let i = 0; i < 200; i++) await emails.add(`job-${i}`, {});
    await waitFor(() => stream.events.length >= 200);
    stream.close();

    expect(stream.events.map((e) => e.name)).toEqual(
      Array.from({ length: 200 }, (_, i) => `job-${i}`),
    );
  });
});
