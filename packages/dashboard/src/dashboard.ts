import type { IncomingMessage, ServerResponse } from 'node:http';
import { Redis, type RedisOptions } from 'ioredis';
import { createApi } from './api.js';
import { sendJson } from './router.js';

export interface DashboardOptions {
  /** Redis URL, options or client. A client you pass is not closed by close(). */
  connection?: string | RedisOptions | Redis;
  prefix?: string;
  /**
   * Decides whether a request may use the dashboard. The dashboard shows job
   * payloads and can retry or delete jobs, so protect it outside development.
   */
  authorize?: (req: IncomingMessage) => boolean | Promise<boolean>;
}

export interface Dashboard {
  /** A Node request handler; also works as Express/Connect middleware. */
  handler: (req: IncomingMessage, res: ServerResponse, next?: () => void) => Promise<void>;
  close(): Promise<void>;
}

const MUTATING = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

export function createDashboard(options: DashboardOptions = {}): Dashboard {
  const prefix = options.prefix ?? 'meridian';
  const ownsClient = !(options.connection instanceof Redis);
  const client =
    options.connection instanceof Redis
      ? options.connection
      : typeof options.connection === 'object'
        ? new Redis({ ...options.connection, maxRetriesPerRequest: null })
        : new Redis(options.connection ?? 'redis://127.0.0.1:6379', { maxRetriesPerRequest: null });

  const api = createApi({ client, prefix });

  async function handler(req: IncomingMessage, res: ServerResponse, next?: () => void) {
    // Express strips its mount path from req.url, so routes are always relative.
    const url = new URL(req.url ?? '/', 'http://dashboard.local');

    if (options.authorize && !(await options.authorize(req))) {
      sendJson(res, 403, { error: 'Forbidden' });
      return;
    }

    // CSRF protection: browsers cannot send a custom header cross-origin
    // without a CORS preflight, which this server never approves.
    if (MUTATING.has(req.method ?? '') && req.headers['x-meridian-request'] !== '1') {
      sendJson(res, 403, { error: 'Missing x-meridian-request header' });
      return;
    }

    if (await api.handle(req, res, url)) return;

    if (next) next();
    else sendJson(res, 404, { error: 'Not found' });
  }

  return {
    handler,
    async close() {
      if (ownsClient) await client.quit();
    },
  };
}
