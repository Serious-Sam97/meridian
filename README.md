# Meridian

A Redis-backed job queue for Node.js, with a Horizon-style supervisor and dashboard.

> Work in progress.

## Packages

| Package | Description |
|---|---|
| [`@meridian/core`](packages/core) | Queue engine: jobs, workers, retries, delayed jobs, stalled-job recovery |

## Development

```bash
npm install
npm run redis:up
npm test
```
