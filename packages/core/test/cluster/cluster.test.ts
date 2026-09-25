import { Cluster } from 'ioredis';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { type ClusterConnection, Queue, Worker } from '../../src/index.js';
import { CLUSTER_NODES, uniqueQueueName, waitFor } from '../helpers.js';

describe.skipIf(!CLUSTER_NODES)('Redis Cluster', { timeout: 30_000 }, () => {
  const connection: ClusterConnection = { cluster: CLUSTER_NODES ?? [] };
  let admin: Cluster;
  const queues: Queue<{ n: number; fail?: boolean }>[] = [];
  const workers: Worker<{ n: number; fail?: boolean }>[] = [];

  beforeAll(() => {
    admin = new Cluster(CLUSTER_NODES ?? []);
  });

  afterAll(async () => {
    await admin.quit();
  });

  afterEach(async () => {
    await Promise.all(workers.splice(0).map((w) => w.close()));
    for (const q of queues.splice(0)) {
      await q.obliterate();
      await q.close();
    }
  });

  function queue(name = uniqueQueueName('cluster')): Queue<{ n: number; fail?: boolean }> {
    const q = new Queue<{ n: number; fail?: boolean }>(name, { connection });
    queues.push(q);
    return q;
  }

  function worker(q: Queue<{ n: number; fail?: boolean }>, done: (n: number) => void) {
    const w = new Worker<{ n: number; fail?: boolean }>(
      q.name,
      async (job) => {
        if (job.data.fail && job.attemptsMade === 0) throw new Error('first attempt fails');
        done(job.data.n);
        return job.data.n;
      },
      { connection, concurrency: 5, blockTimeout: 500, stalledInterval: 300 },
    );
    workers.push(w);
    return w;
  }

  /** The master that owns a queue's keys. */
  async function ownerOf(q: Queue<{ n: number }>): Promise<string> {
    const slot = (await admin.call('CLUSTER', 'KEYSLOT', q.keys.wait)) as number;
    const slots = (await admin.call('CLUSTER', 'SLOTS')) as [number, number, [string, number]][];
    const owner = slots.find(([from, to]) => slot >= from && slot <= to);
    return `${owner?.[2][0]}:${owner?.[2][1]}`;
  }

  /** Queues whose keys live on at least two different masters. */
  async function queuesOnDifferentNodes(): Promise<Queue<{ n: number; fail?: boolean }>[]> {
    const owners = new Map<string, Queue<{ n: number; fail?: boolean }>>();
    for (let i = 0; i < 20 && owners.size < 2; i++) {
      const q = queue();
      owners.set(await ownerOf(q), q);
    }
    return [...owners.values()];
  }

  it('processes jobs with retries, delays and priorities on queues spread over nodes', async () => {
    const spread = await queuesOnDifferentNodes();
    expect(spread.length).toBeGreaterThanOrEqual(2);

    for (const q of spread) {
      const done: number[] = [];
      worker(q, (n) => done.push(n));
      await q.add('job', { n: 1 });
      await q.add('job', { n: 2, fail: true }, { attempts: 2 });
      await q.add('job', { n: 3 }, { delay: 200 });
      await q.add('job', { n: 4 }, { priority: 5 });

      await waitFor(() => done.length === 4, { timeout: 10_000 });
      expect(done.sort()).toEqual([1, 2, 3, 4]);
      expect(await q.getJobCounts()).toMatchObject({ completed: 4, failed: 0 });
    }
  });

  it('recovers a job abandoned by a dead worker', async () => {
    const q = queue();
    const job = await q.add('job', { n: 1 });
    // Active without a lock, as if its worker had died.
    await q.client.zrem(q.keys.wait, job.id);
    await q.client.zadd(q.keys.active, Date.now(), job.id);

    const done: number[] = [];
    worker(q, (n) => done.push(n));
    await waitFor(() => done.length === 1, { timeout: 10_000 });
    expect(await q.getJobState(job.id)).toBe('completed');
  });

  it('runs schedulers, tags and rate limits', async () => {
    const q = queue();
    await q.setRateLimit({ max: 100, duration: 1_000 });
    await q.upsertScheduler(
      'tick',
      { every: 150 },
      { name: 'tick', data: { n: 0 }, options: { tags: ['ticks'] } },
    );
    await q.add('job', { n: 9 }, { tags: ['customer:9'] });

    const done: number[] = [];
    worker(q, (n) => done.push(n));
    await waitFor(() => done.filter((n) => n === 0).length >= 3, { timeout: 10_000 });

    expect(done).toContain(9);
    expect(await q.countJobsByTag('ticks')).toBeGreaterThanOrEqual(3);
    expect((await q.getJobsByTag('customer:9'))[0]?.data.n).toBe(9);
    expect(await q.getRateLimit()).toEqual({ max: 100, duration: 1_000 });
  });

  it('discovers queues on every node and obliterates them completely', async () => {
    const spread = await queuesOnDifferentNodes();
    for (const q of spread) await q.add('job', { n: 1 });

    const names = await Queue.discover(admin);
    for (const q of spread) expect(names).toContain(q.name);

    for (const q of spread) await q.obliterate();
    for (const q of spread) {
      expect(await q.client.exists(q.keys.id, q.keys.wait, q.keys.job('1'))).toBe(0);
    }
    const after = await Queue.discover(admin);
    for (const q of spread) expect(after).not.toContain(q.name);
  });
});
