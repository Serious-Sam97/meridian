import type { Server } from 'node:http';
import { Queue } from '@meridian/core';
import { Cluster } from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createDashboard, type Dashboard } from '../../src/index.js';
import { serve, uniqueName, waitFor } from '../helpers.js';

const NODES = process.env.REDIS_CLUSTER_NODES?.split(',').map((node) => {
  const [host = '127.0.0.1', port = '7100'] = node.split(':');
  return { host, port: Number(port) };
});

describe.skipIf(!NODES)('dashboard on Redis Cluster', { timeout: 30_000 }, () => {
  let cluster: Cluster;
  let dashboard: Dashboard;
  let server: Server;
  let base: string;
  const prefix = uniqueName('dash-cluster');
  const queues: Queue[] = [];

  beforeAll(async () => {
    cluster = new Cluster(NODES ?? [], { redisOptions: { maxRetriesPerRequest: null } });
    dashboard = createDashboard({ connection: cluster, prefix });
    ({ server, url: base } = await serve(dashboard));
    // Enough queues that they spread over the cluster's masters.
    for (let i = 0; i < 6; i++) {
      const q = new Queue(uniqueName('q'), { connection: cluster, prefix });
      await q.add('seed', {});
      queues.push(q);
    }
  });

  afterAll(async () => {
    await dashboard.close();
    server.close();
    for (const q of queues) await q.obliterate();
    await cluster.quit();
  });

  it('lists queues from every node in the overview', async () => {
    const res = await fetch(`${base}/api/overview`);
    const body = (await res.json()) as { queues: { name: string }[] };
    expect(body.queues.map((q) => q.name).sort()).toEqual(queues.map((q) => q.name).sort());
  });

  it('streams events from queues on different nodes', async () => {
    const controller = new AbortController();
    const res = await fetch(`${base}/api/events`, { signal: controller.signal });
    const reader = res.body?.getReader();
    const seen = new Set<string>();
    const decoder = new TextDecoder();
    let buffer = '';
    void (async () => {
      try {
        for (;;) {
          const { value, done } = (await reader?.read()) ?? { done: true };
          if (done) return;
          buffer += decoder.decode(value, { stream: true });
          for (const match of buffer.matchAll(/data: (.+)\n/g)) {
            seen.add((JSON.parse(match[1] ?? '{}') as { queue: string }).queue);
          }
        }
      } catch {
        // aborted
      }
    })();

    await new Promise((r) => setTimeout(r, 500));
    for (const q of queues) await q.add('live', {});
    await waitFor(() => seen.size === queues.length, { timeout: 10_000 });
    controller.abort();
  });
});
