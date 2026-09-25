// Worker for the soak test: a little CPU and garbage per job, and a small
// share of failures so retries, failed-job retention and stacks are exercised.
import type { Job } from '@meridian/core';

export default async function processor(job: Job<{ payload: string; n: number }>) {
  const parsed = JSON.parse(job.data.payload) as { items: number[] };
  const sum = parsed.items.reduce((a, b) => a + b, 0);
  await new Promise((resolve) => setTimeout(resolve, 2 + Math.random() * 6));
  if (job.data.n % 50 === 0 && job.attemptsMade === 0) throw new Error('transient failure');
  if (job.data.n % 997 === 0) throw new Error('permanent failure');
  return { sum };
}
