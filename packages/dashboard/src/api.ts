import { type Job, type ListableState, Queue } from '@meridian/core';
import { Supervisor } from '@meridian/supervisor';
import type { Redis } from 'ioredis';
import type { EventHub } from './events.js';
import { HttpError, type RequestContext, Router, readJson } from './router.js';

const STATES: ListableState[] = ['waiting', 'delayed', 'active', 'completed', 'failed'];
const MAX_PAGE_SIZE = 100;
/** Queue names come from URLs, so the cache of Queue objects must not grow unbounded. */
const MAX_CACHED_QUEUES = 1_000;

export interface ApiOptions {
  client: Redis;
  prefix: string;
  events: EventHub;
}

/** Summary of a queue over the last `SUMMARY_MINUTES` minutes. */
const SUMMARY_MINUTES = 5;

export function createApi({ client, prefix, events }: ApiOptions): Router {
  const queues = new Map<string, Queue>();

  function queue(name: string): Queue {
    let q = queues.get(name);
    if (!q) {
      try {
        q = new Queue(name, { connection: client, prefix });
      } catch (err) {
        throw new HttpError(400, (err as Error).message);
      }
      if (queues.size >= MAX_CACHED_QUEUES) queues.clear();
      queues.set(name, q);
    }
    return q;
  }

  async function summary(name: string) {
    const q = queue(name);
    const [counts, paused, metrics, rateLimit, schedulers] = await Promise.all([
      q.getJobCounts(),
      q.isPaused(),
      q.getMetrics(SUMMARY_MINUTES),
      q.getRateLimit(),
      client.zcard(q.keys.schedulers),
    ]);
    let completed = 0;
    let failed = 0;
    let runtime = 0;
    let wait = 0;
    for (const bucket of metrics) {
      const finished = bucket.completed + bucket.failed;
      completed += bucket.completed;
      failed += bucket.failed;
      runtime += bucket.avgRuntime * finished;
      wait += bucket.avgWait * finished;
    }
    const finished = completed + failed;
    return {
      name,
      counts,
      paused,
      rateLimit,
      schedulers,
      jobsPerMinute: Math.round((finished / SUMMARY_MINUTES) * 10) / 10,
      failureRate: finished ? failed / finished : 0,
      avgRuntime: finished ? Math.round(runtime / finished) : 0,
      avgWait: finished ? Math.round(wait / finished) : 0,
    };
  }

  function minutesParam(query: URLSearchParams): number {
    const minutes = Number(query.get('minutes') ?? 60);
    if (!Number.isInteger(minutes) || minutes < 1 || minutes > 1_440) {
      throw new HttpError(400, 'minutes must be an integer between 1 and 1440');
    }
    return minutes;
  }

  async function requireJob(ctx: RequestContext): Promise<{ q: Queue; job: Job }> {
    const q = queue(ctx.params.name ?? '');
    const job = await q.getJob(ctx.params.id ?? '');
    if (!job) throw new HttpError(404, 'Job not found');
    return { q, job };
  }

  return new Router()
    .on('GET', '/api/overview', async () => {
      const names = await Queue.discover(client, prefix);
      const [summaries, supervisors] = await Promise.all([
        Promise.all(names.map(summary)),
        Supervisor.list(client, prefix),
      ]);
      const totals = {
        waiting: 0,
        active: 0,
        delayed: 0,
        failed: 0,
        jobsPerMinute: 0,
        processes: 0,
      };
      for (const s of summaries) {
        totals.waiting += s.counts.waiting;
        totals.active += s.counts.active;
        totals.delayed += s.counts.delayed;
        totals.failed += s.counts.failed;
        totals.jobsPerMinute += s.jobsPerMinute;
      }
      for (const supervisor of supervisors) {
        for (const pool of Object.values(supervisor.queues)) totals.processes += pool.processes;
      }
      totals.jobsPerMinute = Math.round(totals.jobsPerMinute * 10) / 10;
      return { queues: summaries, supervisors, totals };
    })

    .on('GET', '/api/events', ({ res }) => {
      events.subscribe(res);
    })

    .on('GET', '/api/metrics', async ({ query }) => {
      // Throughput of all queues combined, for the overview chart.
      const minutes = minutesParam(query);
      const names = await Queue.discover(client, prefix);
      const series = await Promise.all(names.map((name) => queue(name).getMetrics(minutes)));
      const combined = (series[0] ?? []).map((bucket) => ({ ...bucket, completed: 0, failed: 0 }));
      for (const buckets of series) {
        buckets.forEach((bucket, i) => {
          const target = combined[i];
          if (!target) return;
          target.completed += bucket.completed;
          target.failed += bucket.failed;
        });
      }
      return combined.map(({ timestamp, completed, failed }) => ({ timestamp, completed, failed }));
    })

    .on('GET', '/api/queues/:name', ({ params }) => summary(params.name ?? ''))

    .on('GET', '/api/queues/:name/metrics', ({ params, query }) =>
      queue(params.name ?? '').getMetrics(minutesParam(query)),
    )

    .on('GET', '/api/queues/:name/jobs', async ({ params, query }) => {
      const tag = query.get('tag');
      if (tag) {
        const page = Math.max(0, Number(query.get('page') ?? 0) || 0);
        const size = Math.min(MAX_PAGE_SIZE, Math.max(1, Number(query.get('size') ?? 20) || 20));
        const q = queue(params.name ?? '');
        const [jobs, total] = await Promise.all([
          q.getJobsByTag(tag, page * size, page * size + size - 1),
          q.countJobsByTag(tag),
        ]);
        return { tag, page, size, total, jobs };
      }
      const state = (query.get('state') ?? 'failed') as ListableState;
      if (!STATES.includes(state))
        throw new HttpError(400, `state must be one of ${STATES.join(', ')}`);
      const page = Math.max(0, Number(query.get('page') ?? 0) || 0);
      const size = Math.min(MAX_PAGE_SIZE, Math.max(1, Number(query.get('size') ?? 20) || 20));

      const q = queue(params.name ?? '');
      const [jobs, counts] = await Promise.all([
        q.getJobs(state, page * size, page * size + size - 1),
        q.getJobCounts(),
      ]);
      return { state, page, size, total: counts[state], jobs };
    })

    .on('GET', '/api/queues/:name/jobs/:id', async (ctx) => {
      const { q, job } = await requireJob(ctx);
      return { ...job, state: await q.getJobState(job.id) };
    })

    .on('POST', '/api/queues/:name/jobs/:id/retry', async (ctx) => {
      const { q, job } = await requireJob(ctx);
      if (!(await q.retryJob(job.id))) throw new HttpError(409, 'Only failed jobs can be retried');
      return { ok: true };
    })

    .on('DELETE', '/api/queues/:name/jobs/:id', async (ctx) => {
      const { q, job } = await requireJob(ctx);
      try {
        await q.removeJob(job.id);
      } catch (err) {
        throw new HttpError(409, (err as Error).message);
      }
      return { ok: true };
    })

    .on('POST', '/api/queues/:name/retry-failed', async ({ params }) => ({
      retried: await queue(params.name ?? '').retryAllFailed(),
    }))

    .on('GET', '/api/schedulers', async () => {
      const names = await Queue.discover(client, prefix);
      const lists = await Promise.all(
        names.map(async (name) =>
          (await queue(name).getSchedulers()).map((s) => ({ queue: name, ...s })),
        ),
      );
      return lists.flat().sort((a, b) => a.next - b.next);
    })

    .on('DELETE', '/api/queues/:name/schedulers/:id', async ({ params }) => {
      if (!(await queue(params.name ?? '').removeScheduler(params.id ?? ''))) {
        throw new HttpError(404, 'Scheduler not found');
      }
      return { ok: true };
    })

    .on('PUT', '/api/queues/:name/rate-limit', async ({ req, params }) => {
      const body = (await readJson(req)) as { max?: unknown; duration?: unknown } | null;
      try {
        await queue(params.name ?? '').setRateLimit(
          body === null ? null : { max: Number(body.max), duration: Number(body.duration) },
        );
      } catch (err) {
        throw new HttpError(400, (err as Error).message);
      }
      return { ok: true };
    })

    .on('DELETE', '/api/queues/:name/rate-limit', async ({ params }) => {
      await queue(params.name ?? '').setRateLimit(null);
      return { ok: true };
    })

    .on('POST', '/api/queues/:name/pause', async ({ params }) => {
      await queue(params.name ?? '').pause();
      return { ok: true };
    })

    .on('POST', '/api/queues/:name/resume', async ({ params }) => {
      await queue(params.name ?? '').resume();
      return { ok: true };
    });
}
