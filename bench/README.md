# Benchmarks

Compares Meridian with [BullMQ](https://bullmq.io) on the same Redis, with the same
payload and the same worker concurrency.

```bash
npm run redis:up
npm run bench                                   # defaults: 20,000 jobs, concurrency 50
npm run bench -- --jobs 50000 --concurrency 100
```

It measures three things:

- **Enqueue**: jobs per second added with each library's bulk API
- **Process**: jobs per second completed by one worker running no-op jobs
- **Latency**: time from `add()` to the `completed` event for one job at a time on an idle
  worker, so the job has to wake the worker

Results are written to [`results.md`](results.md). The numbers depend heavily on the machine
and on the network distance to Redis (these runs use a local Docker container), so compare
the libraries with each other rather than across machines. No-op jobs measure queue overhead
only. Real handlers usually dominate that overhead.
