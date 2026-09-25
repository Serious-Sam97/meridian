import { setTimeout as sleep } from 'node:timers/promises';

/** Pretends to aggregate data for a report: slow, rare. */
export default async function buildReport(job, signal) {
  await sleep(2_000 + Math.random() * 2_000, undefined, { signal });
  return { rows: Math.floor(Math.random() * 10_000), team: job.data.team };
}
