# @meridian/supervisor

Horizon-style process management for [`@meridian/core`](../core). The supervisor runs a pool
of worker processes per queue and moves processes between queues as their workload
changes.

```bash
npm install @meridian/core @meridian/supervisor
```

## Configure

```js
// meridian.config.js
import { defineConfig } from '@meridian/supervisor';

export default defineConfig({
  connection: process.env.REDIS_URL,
  queues: {
    // Each processor module default-exports `async (job, signal) => result`.
    emails: { processor: './jobs/send-email.js', concurrency: 10 },
    images: { processor: './jobs/resize-image.js', concurrency: 2 },
  },
  balance: 'auto',       // or 'simple' for an even split
  minProcesses: 1,       // per queue
  maxProcesses: 10,      // for the whole supervisor
  maxShift: 1,           // processes a queue can gain or lose per round
  balanceInterval: 3000, // ms between rounds
  shutdownTimeout: 10000,
});
```

A config file can also export an array of supervisors, for example one for critical
queues and one for bulk work, each with its own limits.

## Run

```bash
npx meridian-supervisor                 # reads meridian.config.js
npx meridian-supervisor path/to/config.js
```

`SIGTERM` or `SIGINT` stops it gracefully: each process finishes or releases its jobs. A
second signal exits immediately.

To run it from code:

```ts
import { Supervisor } from '@meridian/supervisor';

const supervisor = new Supervisor({ queues: { emails: { processor: './jobs/send-email.js' } } });
supervisor.on('scaled', (allocation) => console.log(allocation)); // { emails: 3 }
await supervisor.start();
```

## How balancing works

See [ADR 0004](../../docs/adr/0004-supervisor-and-balancing.md) for the details.

- **simple** splits `maxProcesses` evenly between the queues.
- **auto** gives every queue `minProcesses`, then shares the rest in proportion to each
  queue's **time to clear** (`waiting × average runtime`). A queue never gets more
  processes than it can keep busy, and capacity one queue cannot use goes to the others.
- Queues change by at most `maxShift` per round, and `maxProcesses` is a hard cap.

Worker processes that crash are restarted with exponential backoff, and processes that
ignore a shutdown are killed after `shutdownTimeout` plus a grace period. Worker processes
exit on their own if the supervisor dies.

The supervisor publishes a heartbeat that [`@meridian/dashboard`](../dashboard) uses to show
supervisors and their process counts.
