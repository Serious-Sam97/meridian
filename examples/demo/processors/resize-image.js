import { setTimeout as sleep } from 'node:timers/promises';

/** Pretends to resize an image: slow and CPU-ish, so bursts need more processes. */
export default async function resizeImage(job, signal) {
  await sleep(400 + Math.random() * 600, undefined, { signal });
  return { path: job.data.path.replace('.png', `-${job.data.width}w.webp`) };
}
