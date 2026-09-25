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
  await emails.add(
    'welcome',
    { to },
    {
      attempts: 3,
      backoff: { type: 'exponential', delay: 1_000, jitter: 0.3 },
      removeOnComplete: 5_000,
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

setInterval(() => {
  void reports.add(
    'weekly-digest',
    { team: ['growth', 'infra', 'billing'][n % 3] },
    { delay: 5_000 },
  );
}, 15_000);

console.log('producer: adding jobs to emails, images and reports');
