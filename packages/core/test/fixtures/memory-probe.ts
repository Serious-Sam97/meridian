// Runs a worker through many idle/wake cycles and prints its heap after a
// forced GC at two checkpoints. Started by memory.test.ts with --expose-gc.
import { Queue, Worker } from '../../src/index.js';

const [queueName, redisUrl] = process.argv.slice(2);
if (!queueName || !redisUrl) throw new Error('usage: memory-probe.ts <queue> <redisUrl>');
const gc = (globalThis as { gc?: () => void }).gc;
if (!gc) throw new Error('run with --expose-gc');

const queue = new Queue(queueName, { connection: redisUrl });
let done = 0;
const worker = new Worker(queueName, async () => {}, { connection: redisUrl, concurrency: 25 });
worker.on('completed', () => done++);

async function runJobs(count: number): Promise<void> {
  const target = done + count;
  // Small bursts, so the worker keeps filling its slots and going idle again.
  while (done < target) {
    await queue.addBulk(
      Array.from({ length: 30 }, () => ({
        name: 'j',
        data: {},
        options: { removeOnComplete: true },
      })),
    );
    await new Promise((r) => setTimeout(r, 5));
  }
}

function heapAfterGc(): number {
  gc?.();
  gc?.();
  return process.memoryUsage().heapUsed;
}

await runJobs(2_000); // warm-up
const before = heapAfterGc();
await runJobs(12_000);
const after = heapAfterGc();
console.log(JSON.stringify({ before, after }));

await worker.close();
await queue.obliterate();
await queue.close();
process.exit(0);
