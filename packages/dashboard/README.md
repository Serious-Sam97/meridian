# @meridian/dashboard

A web dashboard for Meridian. It shows throughput, queue depth and runtimes, lets you
browse jobs (by state or by tag) and inspect or retry failures, and lists schedulers, rate
limits, live supervisors and a live event feed.

```bash
npm install @meridian/dashboard
```

## Standalone

```bash
npx meridian-dashboard --port 3000 --redis redis://localhost:6379
MERIDIAN_DASHBOARD_AUTH=admin:change-me npx meridian-dashboard --host 0.0.0.0
```

It binds to `127.0.0.1` by default. Set `MERIDIAN_DASHBOARD_AUTH` before exposing it
anywhere else.

## Inside your app

The dashboard is a plain Node request handler, so it works with `node:http` and as
Express/Connect middleware:

```ts
import express from 'express';
import { createDashboard } from '@meridian/dashboard';

const dashboard = createDashboard({
  connection: process.env.REDIS_URL,
  authorize: (req) => isAdmin(req), // runs on every request
});

const app = express();
app.use('/admin/queues', dashboard.handler);
```

## Security

The dashboard shows job payloads and can delete jobs. It has these protections
([ADR 0005](../../docs/adr/0005-dashboard.md)):

- an `authorize` hook, or Basic auth in the CLI, compared in constant time
- CSRF protection: mutating requests must send `x-meridian-request: 1`, which
  cross-origin pages cannot do
- a strict Content Security Policy with no inline scripts, and job data rendered as text,
  never as HTML
- malformed requests get a 4xx response and never reach an unhandled rejection

## API

The UI uses a JSON API that you can also call directly:

| Method | Path | |
|---|---|---|
| GET | `/api/overview` | Queues, supervisors and totals |
| GET | `/api/metrics?minutes=60` | Throughput of all queues per minute |
| GET | `/api/events` | Server-Sent Events stream of job events |
| GET | `/api/queues/:name` | One queue's summary |
| GET | `/api/queues/:name/metrics` | One queue's per-minute metrics |
| GET | `/api/queues/:name/jobs?state=failed&page=0&size=20` | Jobs by state |
| GET | `/api/queues/:name/jobs?tag=customer:42` | Jobs by tag |
| GET | `/api/schedulers` | Schedulers of all queues, soonest first |
| DELETE | `/api/queues/:name/schedulers/:id` | Remove a scheduler |
| PUT / DELETE | `/api/queues/:name/rate-limit` | Set (`{ "max": 10, "duration": 1000 }`) or clear the rate limit |
| GET | `/api/queues/:name/jobs/:id` | A job with its state |
| POST | `/api/queues/:name/jobs/:id/retry` | Retry a failed job |
| DELETE | `/api/queues/:name/jobs/:id` | Delete a job |
| POST | `/api/queues/:name/retry-failed` | Retry every failed job |
| POST | `/api/queues/:name/pause` / `resume` | Pause or resume a queue |
