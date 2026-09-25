// Runs a worker in a separate process so tests can SIGKILL it mid-job.
// Usage: node --import tsx slow-worker.ts <queueName> <redisUrl>
import { Worker } from '../../src/index.js';

const [queueName, redisUrl] = process.argv.slice(2);
if (!queueName || !redisUrl) throw new Error('usage: slow-worker.ts <queueName> <redisUrl>');

const worker = new Worker(
  queueName,
  async () => {
    await new Promise((resolve) => setTimeout(resolve, 60_000));
  },
  // Same stalledInterval as the test's workers: the check throttle is shared by all of them.
  { connection: redisUrl, concurrency: 5, lockDuration: 500, stalledInterval: 200 },
);

worker.on('active', (job) => process.send?.({ type: 'active', jobId: job.id }));
