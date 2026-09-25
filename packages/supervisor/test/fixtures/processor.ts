import type { Job } from '@meridian/core';

export interface FixtureData {
  n: number;
  /** Sleep this long before finishing, in ms. */
  sleep?: number;
  /** Kill the whole worker process, as a segfault or OOM would. */
  crash?: boolean;
}

export default async function processor(
  job: Job<FixtureData>,
): Promise<{ pid: number; n: number }> {
  if (job.data.crash) process.exit(1);
  if (job.data.sleep) await new Promise((resolve) => setTimeout(resolve, job.data.sleep));
  return { pid: process.pid, n: job.data.n * 2 };
}
