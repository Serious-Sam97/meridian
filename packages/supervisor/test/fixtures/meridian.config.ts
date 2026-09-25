import { defineConfig } from '../../src/config.js';

// Filled in by the CLI test through environment variables.
export default defineConfig({
  name: process.env.TEST_SUPERVISOR_NAME,
  connection: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
  queues: {
    [process.env.TEST_QUEUE ?? 'cli-test']: { processor: './processor.ts' },
  },
  maxProcesses: 2,
  balanceInterval: 200,
  shutdownTimeout: 1_000,
  workerOptions: { blockTimeout: 500 },
  execArgv: ['--import', 'tsx', '--conditions=@meridian/source'],
});
