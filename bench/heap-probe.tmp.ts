import { Queue, Worker } from '@meridian/core';
import processor from './soak/processor.js';

const gc = (globalThis as { gc?: () => void }).gc;
if (!gc) throw new Error('run with --expose-gc');
const opts = { prefix: 'heapprobe' };
const queue = new Queue<{ payload: string; n: number }>('work', opts);
await queue.obliterate();
const worker = new Worker('work', processor, { ...opts, concurrency: 25 });
worker.on('error', () => {});
await queue.upsertScheduler('tick', { every: 1_000 }, {
  name: 'tick',
  data: { payload: '{"items":[1]}', n: 1 },
  options: { removeOnComplete: 1_000 },
});
const payload = JSON.stringify({ items: Array.from({ length: 50 }, (_, i) => i) });
let n = 0;
const producer = setInterval(() => {
  void queue.addBulk(
    Array.from({ length: 20 }, () => {
      n++;
      return {
        name: 'work',
        data: { payload, n },
        options: { attempts: 2, removeOnComplete: 1_000, removeOnFail: 1_000, tags: [`customer:${n % 100}`] },
      };
    }),
  );
}, 100);

const start = Date.now();
for (let i = 0; i < 12; i++) {
  await new Promise((r) => setTimeout(r, 30_000));
  gc(); gc();
  const m = process.memoryUsage();
  console.log(`t=${Math.round((Date.now() - start) / 1000)}s heapAfterGC=${(m.heapUsed / 1048576).toFixed(1)}MB rss=${Math.round(m.rss / 1048576)}MB jobs=${n}`);
}
clearInterval(producer);
await worker.close();
await queue.obliterate();
await queue.close();
process.exit(0);
