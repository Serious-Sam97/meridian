// Long-running test: the whole system at a steady load, sampling memory and
// Redis to catch leaks and unbounded growth. Run it for hours:
//   npm run soak -- --minutes 1440
import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { cpus } from 'node:os';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { Queue } from '@meridian/core';
import { ProcessPool } from '@meridian/supervisor';
import { Redis } from 'ioredis';

const { values } = parseArgs({
  options: {
    minutes: { type: 'string', default: '10' },
    rate: { type: 'string', default: '200' },
    sample: { type: 'string', default: '30' },
    redis: { type: 'string', default: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' },
  },
});
const MINUTES = Number(values.minutes);
const RATE = Number(values.rate);
const SAMPLE_MS = Number(values.sample) * 1000;
const REDIS_URL = values.redis ?? 'redis://127.0.0.1:6379';
const PREFIX = 'soak';
const PROCESSOR = fileURLToPath(new URL('./soak/processor.ts', import.meta.url));

interface Sample {
  minute: number;
  jobsPerSecond: number;
  waiting: number;
  workerRssMb: number;
  producerHeapMb: number;
  redisMb: number;
  keys: number;
  eventsLength: number;
  tagIndexSize: number;
  completedKept: number;
}

const admin = new Redis(REDIS_URL);
const queue = new Queue<{ payload: string; n: number }>('work', {
  connection: REDIS_URL,
  prefix: PREFIX,
});

function rssMb(pids: number[]): number {
  if (pids.length === 0) return 0;
  const out = execFileSync('ps', ['-o', 'rss=', '-p', pids.join(',')], { encoding: 'utf8' });
  return out.split('\n').reduce((sum, line) => sum + (Number(line.trim()) || 0), 0) / 1024;
}

/** Least-squares slope of y over x. */
function slope(points: [number, number][]): number {
  const n = points.length;
  const mx = points.reduce((s, [x]) => s + x, 0) / n;
  const my = points.reduce((s, [, y]) => s + y, 0) / n;
  const num = points.reduce((s, [x, y]) => s + (x - mx) * (y - my), 0);
  const den = points.reduce((s, [x]) => s + (x - mx) ** 2, 0);
  return den === 0 ? 0 : num / den;
}

async function main(): Promise<void> {
  await queue.obliterate();
  const pool = new ProcessPool({
    child: {
      queue: queue.name,
      processor: PROCESSOR,
      connection: REDIS_URL,
      prefix: PREFIX,
      concurrency: 25,
      shutdownTimeout: 10_000,
    },
    execArgv: ['--import', 'tsx', '--conditions=@meridian/source'],
  });
  let crashes = 0;
  pool.on('crash', () => crashes++);
  await pool.scale(4);

  await queue.upsertScheduler(
    'tick',
    { every: 1_000 },
    {
      name: 'tick',
      data: { payload: '{"items":[1]}', n: 1 },
      // Same retention as the other jobs: counts apply to the queue's whole completed set.
      options: { removeOnComplete: 1_000 },
    },
  );

  const payload = JSON.stringify({ items: Array.from({ length: 50 }, (_, i) => i) });
  let n = 0;
  const producer = setInterval(() => {
    const batch = Array.from({ length: RATE / 10 }, () => {
      n++;
      return {
        name: 'work',
        data: { payload, n },
        options: {
          attempts: 2,
          removeOnComplete: 1_000,
          removeOnFail: 1_000,
          tags: [`customer:${n % 100}`],
        },
      };
    });
    void queue.addBulk(batch);
  }, 100);

  const started = Date.now();
  const samples: Sample[] = [];
  let lastCompleted = 0;
  const totalCompleted = async () => {
    const buckets = await queue.getMetrics(Math.ceil(MINUTES) + 2);
    return buckets.reduce((s, b) => s + b.completed + b.failed, 0);
  };

  console.log(`soak: ${MINUTES} min at ${RATE} jobs/s, sampling every ${SAMPLE_MS / 1000}s`);
  while (Date.now() - started < MINUTES * 60_000) {
    await new Promise((r) => setTimeout(r, SAMPLE_MS));
    const completed = await totalCompleted();
    const info = await admin.info('memory');
    const sample: Sample = {
      minute: Math.round(((Date.now() - started) / 60_000) * 10) / 10,
      jobsPerSecond: Math.round((completed - lastCompleted) / (SAMPLE_MS / 1000)),
      waiting: (await queue.getJobCounts()).waiting,
      workerRssMb: Math.round(rssMb(pool.pids)),
      producerHeapMb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      redisMb: Math.round(Number(/used_memory:(\d+)/.exec(info)?.[1] ?? 0) / 1024 / 1024),
      keys: await admin.dbsize(),
      eventsLength: await admin.xlen(queue.keys.events),
      tagIndexSize: await queue.countJobsByTag('customer:1'),
      completedKept: (await queue.getJobCounts()).completed,
    };
    lastCompleted = completed;
    samples.push(sample);
    console.log(JSON.stringify(sample));
  }

  clearInterval(producer);
  await pool.stop();

  // Growth per hour, ignoring the first fifth of the run (warm-up).
  const steady = samples.slice(Math.floor(samples.length / 5));
  const perHour = (pick: (s: Sample) => number) =>
    Math.round(slope(steady.map((s) => [s.minute / 60, pick(s)])) * 10) / 10;
  const growth = {
    workerRss: perHour((s) => s.workerRssMb),
    redis: perHour((s) => s.redisMb),
    keys: perHour((s) => s.keys),
  };

  const rows = samples.map(
    (s) =>
      `| ${s.minute} | ${s.jobsPerSecond} | ${s.waiting} | ${s.workerRssMb} | ${s.redisMb} | ${s.keys.toLocaleString('en-US')} | ${s.eventsLength.toLocaleString('en-US')} | ${s.tagIndexSize} |`,
  );
  const report = `# Soak test results

Node ${process.version}, ${cpus()[0]?.model ?? 'unknown CPU'}; Redis at ${new URL(REDIS_URL).host}.
${MINUTES} minutes at ${RATE} jobs/s into one queue, 4 worker processes x concurrency 25,
recycling off. Jobs keep the newest 1,000 completed and failed, carry one of 100 tags, fail
once in 50 attempts (retried) and permanently once in 997. A scheduler adds a job every second.

**Growth per hour** after warm-up (least-squares slope): worker RSS ${growth.workerRss} MB/h,
Redis memory ${growth.redis} MB/h, keys ${growth.keys}/h. Worker crashes: ${crashes}.

| Minute | Jobs/s | Waiting | Worker RSS (MB, 4 processes) | Redis (MB) | Keys | Events stream | Tag index (customer:1) |
|---:|---:|---:|---:|---:|---:|---:|---:|
${rows.join('\n')}
`;
  writeFileSync(new URL('./soak-results.md', import.meta.url), report);
  console.log(
    `\ngrowth per hour: ${JSON.stringify(growth)}, crashes: ${crashes}\nWritten to bench/soak-results.md`,
  );

  await queue.obliterate();
  await queue.close();
  admin.disconnect();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
