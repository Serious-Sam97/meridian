// Throughput and latency: Meridian vs BullMQ against the same Redis.
// Usage: npm run bench [-- --jobs 20000 --concurrency 50 --samples 300]
import { writeFileSync } from 'node:fs';
import { cpus } from 'node:os';
import { parseArgs } from 'node:util';
import { Queue as MeridianQueue, Worker as MeridianWorker } from '@meridian/core';
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import { Redis } from 'ioredis';

const { values } = parseArgs({
  options: {
    jobs: { type: 'string', default: '20000' },
    concurrency: { type: 'string', default: '50' },
    samples: { type: 'string', default: '300' },
    redis: { type: 'string', default: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' },
  },
});
const JOBS = Number(values.jobs);
const CONCURRENCY = Number(values.concurrency);
const SAMPLES = Number(values.samples);
const REDIS_URL = values.redis ?? 'redis://127.0.0.1:6379';
const { hostname: host, port } = new URL(REDIS_URL);

/** The same three operations, implemented for each library. */
interface Adapter {
  name: string;
  enqueue(queue: string, count: number): Promise<void>;
  /** Starts a worker that calls onDone after each job; returns a close function. */
  work(queue: string, concurrency: number, onDone: () => void): Promise<() => Promise<void>>;
  add(queue: string): Promise<void>;
  clean(queue: string): Promise<void>;
}

const payload = { to: 'user@example.com', template: 'welcome' };

function meridian(): Adapter {
  const client = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
  const prefix = 'bench-meridian';
  const queue = (name: string) => new MeridianQueue(name, { connection: client, prefix });
  return {
    name: 'Meridian',
    async enqueue(name, count) {
      await queue(name).addBulk(
        Array.from({ length: count }, () => ({ name: 'job', data: payload })),
      );
    },
    async work(name, concurrency, onDone) {
      const worker = new MeridianWorker(name, async () => {}, {
        connection: REDIS_URL,
        prefix,
        concurrency,
      });
      worker.on('completed', onDone);
      return () => worker.close();
    },
    async add(name) {
      await queue(name).add('job', payload);
    },
    async clean(name) {
      await queue(name).obliterate();
    },
  };
}

function bullmq(): Adapter {
  const connection = { host, port: Number(port || 6379), maxRetriesPerRequest: null };
  const prefix = 'bench-bullmq';
  const queues = new Map<string, BullQueue>();
  const queue = (name: string) => {
    let q = queues.get(name);
    if (!q) {
      q = new BullQueue(name, { connection, prefix });
      queues.set(name, q);
    }
    return q;
  };
  return {
    name: 'BullMQ',
    async enqueue(name, count) {
      const q = queue(name);
      for (let i = 0; i < count; i += 1_000) {
        const size = Math.min(1_000, count - i);
        await q.addBulk(Array.from({ length: size }, () => ({ name: 'job', data: payload })));
      }
    },
    async work(name, concurrency, onDone) {
      const worker = new BullWorker(name, async () => {}, { connection, prefix, concurrency });
      worker.on('completed', onDone);
      await worker.waitUntilReady();
      return () => worker.close();
    },
    async add(name) {
      await queue(name).add('job', payload);
    },
    async clean(name) {
      await queue(name).obliterate({ force: true });
      await queue(name).close();
      queues.delete(name);
    },
  };
}

function waitUntil(predicate: () => boolean): Promise<void> {
  return new Promise((resolve) => {
    const check = () => (predicate() ? resolve() : setImmediate(check));
    check();
  });
}

function percentile(sorted: number[], p: number): number {
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0;
}

async function benchEnqueue(adapter: Adapter): Promise<number> {
  const queue = `enqueue-${Date.now()}`;
  const started = performance.now();
  await adapter.enqueue(queue, JOBS);
  const seconds = (performance.now() - started) / 1_000;
  await adapter.clean(queue);
  return JOBS / seconds;
}

async function benchProcess(adapter: Adapter): Promise<number> {
  const queue = `process-${Date.now()}`;
  await adapter.enqueue(queue, JOBS);
  let done = 0;
  const started = performance.now();
  const close = await adapter.work(queue, CONCURRENCY, () => {
    done++;
  });
  await waitUntil(() => done >= JOBS);
  const seconds = (performance.now() - started) / 1_000;
  await close();
  await adapter.clean(queue);
  return JOBS / seconds;
}

/** Time from add() to the completed event, one job at a time on an idle worker. */
async function benchLatency(adapter: Adapter): Promise<number[]> {
  const queue = `latency-${Date.now()}`;
  let done = 0;
  const close = await adapter.work(queue, 1, () => {
    done++;
  });
  // Let the worker go idle and block on Redis.
  await new Promise((r) => setTimeout(r, 200));

  const samples: number[] = [];
  for (let i = 0; i < SAMPLES; i++) {
    const target = done + 1;
    const started = performance.now();
    await adapter.add(queue);
    await waitUntil(() => done >= target);
    samples.push(performance.now() - started);
  }
  await close();
  await adapter.clean(queue);
  return samples.sort((a, b) => a - b);
}

async function main(): Promise<void> {
  const info = await new Redis(REDIS_URL).info('server');
  const redisVersion = /redis_version:(\S+)/.exec(info)?.[1] ?? 'unknown';
  const env = `Node ${process.version}, Redis ${redisVersion}, ${cpus()[0]?.model ?? 'unknown CPU'} (${cpus().length} cores)`;
  console.log(`${env}\n${JOBS} jobs, concurrency ${CONCURRENCY}, ${SAMPLES} latency samples\n`);

  const rows: string[] = [];
  for (const adapter of [meridian(), bullmq()]) {
    // Warm up script caches and JIT.
    await benchEnqueue({ ...adapter, enqueue: (q) => adapter.enqueue(q, 500) });

    const enqueue = await benchEnqueue(adapter);
    const processed = await benchProcess(adapter);
    const latency = await benchLatency(adapter);
    const row = `| ${adapter.name} | ${Math.round(enqueue).toLocaleString('en-US')} | ${Math.round(processed).toLocaleString('en-US')} | ${percentile(latency, 50).toFixed(2)} | ${percentile(latency, 95).toFixed(2)} | ${percentile(latency, 99).toFixed(2)} |`;
    rows.push(row);
    console.log(`${adapter.name}: done`);
  }

  const table = [
    '| Library | Enqueue (jobs/s) | Process (jobs/s) | Latency p50 (ms) | p95 (ms) | p99 (ms) |',
    '|---|---:|---:|---:|---:|---:|',
    ...rows,
  ].join('\n');

  const report = `# Benchmark results\n\n${env}.\n\n${JOBS.toLocaleString('en-US')} no-op jobs, one worker with concurrency ${CONCURRENCY}; latency is add() to completed for ${SAMPLES} jobs on an idle worker.\n\n${table}\n`;
  writeFileSync(new URL('./results.md', import.meta.url), report);
  console.log(`\n${table}\n\nWritten to bench/results.md`);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
