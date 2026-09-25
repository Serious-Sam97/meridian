// Supervisor configuration for the demo. Processor paths are relative to this file.
/** @type {import('@meridian/supervisor').MeridianConfig} */
export default {
  name: 'demo',
  connection: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
  queues: {
    emails: { processor: './processors/send-email.js', concurrency: 5 },
    images: { processor: './processors/resize-image.js', concurrency: 2 },
    reports: { processor: './processors/build-report.js' },
  },
  // Like Horizon's auto balancing: processes follow the workload.
  balance: 'auto',
  minProcesses: 1,
  maxProcesses: 8,
  maxShift: 1,
  balanceInterval: 2_000,
  shutdownTimeout: 5_000,
};
