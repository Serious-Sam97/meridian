#!/usr/bin/env node
import { createServer } from 'node:http';
import { parseArgs } from 'node:util';
import { basicAuth, requestCredentials } from './basic-auth.js';
import { createDashboard } from './dashboard.js';

const USAGE = `Usage: meridian-dashboard [options]

Options:
  --port <n>       Port to listen on (default: 3000, or $PORT)
  --host <host>    Interface to bind (default: 127.0.0.1)
  --redis <url>    Redis URL (default: $REDIS_URL or redis://127.0.0.1:6379)
  --prefix <name>  Key prefix of the queues (default: meridian)
  -h, --help       Show this help

Set MERIDIAN_DASHBOARD_AUTH=user:password to require HTTP Basic auth.
The dashboard shows job payloads and can delete jobs: protect it before
binding to anything other than localhost.`;

const { values } = parseArgs({
  options: {
    port: { type: 'string' },
    host: { type: 'string', default: '127.0.0.1' },
    redis: { type: 'string' },
    prefix: { type: 'string', default: 'meridian' },
    help: { type: 'boolean', short: 'h' },
  },
});

if (values.help) {
  console.log(USAGE);
  process.exit(0);
}

const port = Number(values.port ?? process.env.PORT ?? 3000);
const host = values.host ?? '127.0.0.1';
const credentials = process.env.MERIDIAN_DASHBOARD_AUTH;
const authorized = credentials ? basicAuth(credentials) : undefined;

if (!authorized && host !== '127.0.0.1' && host !== 'localhost' && host !== '::1') {
  console.warn(
    `Warning: serving on ${host} without authentication. Set MERIDIAN_DASHBOARD_AUTH=user:password.`,
  );
}

const dashboard = createDashboard({
  connection: values.redis ?? process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
  prefix: values.prefix,
});

const server = createServer((req, res) => {
  if (authorized && !authorized(req)) {
    requestCredentials(res);
    return;
  }
  void dashboard.handler(req, res);
});

server.listen(port, host, () => {
  console.log(`Meridian dashboard on http://${host.includes(':') ? `[${host}]` : host}:${port}`);
});

const shutdown = () => {
  // SSE connections stay open, so close them explicitly instead of waiting.
  server.closeAllConnections();
  server.close();
  void dashboard.close().then(() => process.exit(0));
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
