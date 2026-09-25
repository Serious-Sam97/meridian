import type { Server } from 'node:http';
import { Queue, Worker } from '@meridian/core';
import { supervisorsKey } from '@meridian/supervisor';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDashboard, type Dashboard } from '../src/index.js';
import { createRedis, serve, uniqueName, waitFor } from './helpers.js';

describe('dashboard api', () => {
  let redis: Redis;
  let prefix: string;
  let dashboard: Dashboard;
  let server: Server;
  let base: string;
  let emails: Queue<{ to: string; fail?: boolean }>;

  async function api<T = Record<string, unknown>>(
    path: string,
    init: RequestInit = {},
  ): Promise<{ status: number; body: T }> {
    const res = await fetch(`${base}${path}`, init);
    return { status: res.status, body: (await res.json()) as T };
  }

  const mutate = (method: string) => ({ method, headers: { 'x-meridian-request': '1' } });

  async function processAll(count: number): Promise<void> {
    const worker = new Worker<{ to: string; fail?: boolean }>(
      emails.name,
      async (job) => {
        if (job.data.fail) throw new Error('smtp timeout');
        return 'sent';
      },
      { connection: redis, prefix, blockTimeout: 500 },
    );
    await waitFor(async () => {
      const counts = await emails.getJobCounts();
      return counts.completed + counts.failed === count;
    });
    await worker.close();
  }

  beforeAll(() => {
    redis = createRedis();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    prefix = uniqueName('dash');
    emails = new Queue(uniqueName('emails'), { connection: redis, prefix });
    dashboard = createDashboard({ connection: redis, prefix });
    ({ server, url: base } = await serve(dashboard));
  });

  afterEach(async () => {
    server.close();
    await dashboard.close();
    await emails.obliterate();
    await redis.del(supervisorsKey(prefix));
  });

  it('summarizes every queue in the overview', async () => {
    await emails.add('send', { to: 'a' });
    await emails.add('send', { to: 'b', fail: true });
    await processAll(2);
    await emails.add('send', { to: 'c' });

    const { status, body } = await api<{
      queues: {
        name: string;
        counts: Record<string, number>;
        jobsPerMinute: number;
        failureRate: number;
      }[];
      totals: Record<string, number>;
    }>('/api/overview');

    expect(status).toBe(200);
    expect(body.queues).toHaveLength(1);
    expect(body.queues[0]).toMatchObject({
      name: emails.name,
      counts: { waiting: 1, completed: 1, failed: 1 },
      jobsPerMinute: 0.4,
      failureRate: 0.5,
    });
    expect(body.totals).toMatchObject({ waiting: 1, failed: 1 });
  });

  it('includes live supervisors in the overview', async () => {
    await emails.add('send', { to: 'a' });
    await redis.hset(
      supervisorsKey(prefix),
      'web-1',
      JSON.stringify({
        name: 'web-1',
        updatedAt: Date.now(),
        queues: { [emails.name]: { processes: 3, target: 3 } },
      }),
    );

    const { body } = await api<{ supervisors: { name: string }[]; totals: { processes: number } }>(
      '/api/overview',
    );
    expect(body.supervisors.map((s) => s.name)).toEqual(['web-1']);
    expect(body.totals.processes).toBe(3);
  });

  it('lists jobs by state with paging', async () => {
    for (let i = 0; i < 5; i++) await emails.add('send', { to: `user${i}`, fail: true });
    await processAll(5);

    const { body } = await api<{ total: number; jobs: { data: { to: string } }[] }>(
      `/api/queues/${emails.name}/jobs?state=failed&page=1&size=2`,
    );
    expect(body.total).toBe(5);
    expect(body.jobs.map((j) => j.data.to)).toEqual(['user2', 'user1']);
  });

  it('rejects an unknown state', async () => {
    const { status } = await api(`/api/queues/${emails.name}/jobs?state=nope`);
    expect(status).toBe(400);
  });

  it('shows a job with its state and failure details', async () => {
    const job = await emails.add('send', { to: 'a', fail: true });
    await processAll(1);

    const { body } = await api(`/api/queues/${emails.name}/jobs/${job.id}`);
    expect(body).toMatchObject({ id: job.id, state: 'failed', failedReason: 'smtp timeout' });
    expect(body.stacktrace).toContain('smtp timeout');
  });

  it('returns 404 for an unknown job', async () => {
    const { status } = await api(`/api/queues/${emails.name}/jobs/404`);
    expect(status).toBe(404);
  });

  it('retries a failed job', async () => {
    const job = await emails.add('send', { to: 'a', fail: true });
    await processAll(1);

    const { status } = await api(`/api/queues/${emails.name}/jobs/${job.id}/retry`, mutate('POST'));
    expect(status).toBe(200);
    expect(await emails.getJobState(job.id)).toBe('waiting');

    const again = await api(`/api/queues/${emails.name}/jobs/${job.id}/retry`, mutate('POST'));
    expect(again.status).toBe(409);
  });

  it('retries every failed job of a queue', async () => {
    for (let i = 0; i < 3; i++) await emails.add('send', { to: `u${i}`, fail: true });
    await processAll(3);

    const { body } = await api(`/api/queues/${emails.name}/retry-failed`, mutate('POST'));
    expect(body).toEqual({ retried: 3 });
  });

  it('deletes a job', async () => {
    const job = await emails.add('send', { to: 'a' });
    const { status } = await api(`/api/queues/${emails.name}/jobs/${job.id}`, mutate('DELETE'));
    expect(status).toBe(200);
    expect(await emails.getJob(job.id)).toBeUndefined();
  });

  it('pauses and resumes a queue', async () => {
    await emails.add('send', { to: 'a' });
    await api(`/api/queues/${emails.name}/pause`, mutate('POST'));
    expect((await api(`/api/queues/${emails.name}`)).body.paused).toBe(true);

    await api(`/api/queues/${emails.name}/resume`, mutate('POST'));
    expect((await api(`/api/queues/${emails.name}`)).body.paused).toBe(false);
  });

  it('returns combined throughput per minute', async () => {
    await emails.add('send', { to: 'a' });
    await processAll(1);

    const { body } = await api<{ completed: number }[]>('/api/metrics?minutes=10');
    expect(body).toHaveLength(10);
    expect(body.reduce((sum, b) => sum + b.completed, 0)).toBe(1);
  });

  it('refuses mutations without the CSRF header', async () => {
    const job = await emails.add('send', { to: 'a' });
    const { status } = await api(`/api/queues/${emails.name}/jobs/${job.id}`, { method: 'DELETE' });
    expect(status).toBe(403);
    expect(await emails.getJob(job.id)).toBeDefined();
  });

  it('answers 405 for a known path with the wrong method', async () => {
    const { status } = await api('/api/overview', mutate('POST'));
    expect(status).toBe(405);
  });

  it('applies the authorize hook to every request', async () => {
    const locked = createDashboard({
      connection: redis,
      prefix,
      authorize: (req) => req.headers.authorization === 'Bearer secret',
    });
    const { server: lockedServer, url } = await serve(locked);
    try {
      expect((await fetch(`${url}/api/overview`)).status).toBe(403);
      const ok = await fetch(`${url}/api/overview`, {
        headers: { authorization: 'Bearer secret' },
      });
      expect(ok.status).toBe(200);
    } finally {
      lockedServer.close();
      await locked.close();
    }
  });
});
