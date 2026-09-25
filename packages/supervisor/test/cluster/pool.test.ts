import { Queue } from '@meridian/core';
import { describe, expect, it } from 'vitest';
import { ProcessPool } from '../../src/index.js';
import type { FixtureData } from '../fixtures/processor.js';
import { CHILD_EXEC_ARGV, PROCESSOR, uniqueQueueName, waitFor } from '../helpers.js';

const NODES = process.env.REDIS_CLUSTER_NODES?.split(',').map((node) => {
  const [host = '127.0.0.1', port = '7100'] = node.split(':');
  return { host, port: Number(port) };
});

describe.skipIf(!NODES)('ProcessPool on Redis Cluster', { timeout: 30_000 }, () => {
  it('runs worker processes that connect with a cluster config', async () => {
    const connection = { cluster: NODES ?? [] };
    const queue = new Queue<FixtureData>(uniqueQueueName('cluster-pool'), { connection });
    const pool = new ProcessPool({
      child: {
        queue: queue.name,
        processor: PROCESSOR,
        connection,
        concurrency: 2,
        shutdownTimeout: 1_000,
        workerOptions: { blockTimeout: 500 },
      },
      execArgv: CHILD_EXEC_ARGV,
    });

    try {
      await pool.scale(2);
      const jobs = await queue.addBulk([1, 2, 3, 4].map((n) => ({ name: 'double', data: { n } })));
      await waitFor(async () => (await queue.getJobCounts()).completed === 4, { timeout: 15_000 });

      const results = await Promise.all(jobs.map((j) => queue.getJob<{ n: number }>(j.id)));
      expect(results.map((r) => r?.returnValue?.n)).toEqual([2, 4, 6, 8]);
    } finally {
      await pool.stop();
      await queue.obliterate();
      await queue.close();
    }
  });
});
