// Adds a steady stream of emails, bursts of image jobs and the odd report.
import { Queue } from '@meridian/core';

const connection = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const emails = new Queue('emails', { connection });
const images = new Queue('images', { connection });
const reports = new Queue('reports', { connection });

let n = 0;
setInterval(async () => {
  n++;
  const to = n % 97 === 0 ? `broken-address-${n}` : `user${n}@example.com`;
  const customer = `customer:${n % 40}`;
  await emails.add(
    'welcome',
    { to },
    {
      attempts: 3,
      backoff: { type: 'exponential', delay: 1_000, jitter: 0.3 },
      removeOnComplete: 5_000,
      tags: [customer, 'welcome'],
    },
  );
}, 100);

// Every 30s, a burst of 150 image jobs: watch the supervisor give "images" more processes.
async function burst() {
  const jobs = Array.from({ length: 150 }, (_, i) => ({
    name: 'thumbnail',
    data: { path: `/uploads/${Date.now()}-${i}.png`, width: 320 },
    options: { attempts: 2, removeOnComplete: 2_000 },
  }));
  await images.addBulk(jobs);
  console.log('producer: added a burst of 150 image jobs');
}
void burst();
setInterval(burst, 30_000);

// Every 20 seconds, like a cron job. Upserting on every start is a no-op.
await reports.upsertScheduler(
  'team-digest',
  { pattern: '*/20 * * * * *' },
  { name: 'weekly-digest', data: { team: 'growth' }, options: { tags: ['reports'] } },
);

// The email provider allows 600 messages a minute; every worker respects it.
await emails.setRateLimit({ max: 600, duration: 60_000 });

console.log('producer: adding jobs to emails, images and reports');
