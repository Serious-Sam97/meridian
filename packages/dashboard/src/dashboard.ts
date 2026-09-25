import { readFileSync } from 'node:fs';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { Redis, type RedisOptions } from 'ioredis';
import { createApi } from './api.js';
import { EventHub } from './events.js';
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

const UI_DIR = new URL('../ui/', import.meta.url);
const ASSETS: Record<string, { file: string; type: string }> = {
  '/': { file: 'index.html', type: 'text/html; charset=utf-8' },
  '/index.html': { file: 'index.html', type: 'text/html; charset=utf-8' },
  '/assets/app.js': { file: 'app.js', type: 'text/javascript; charset=utf-8' },
  '/assets/app.css': { file: 'app.css', type: 'text/css; charset=utf-8' },
};

const SECURITY_HEADERS = {
  // Everything is served from the dashboard itself; no inline scripts.
  'content-security-policy':
    "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
  'x-content-type-options': 'nosniff',
  'referrer-policy': 'no-referrer',
};

const assetCache = new Map<string, Buffer>();

function serveAsset(req: IncomingMessage, res: ServerResponse, pathname: string): boolean {
  const asset = ASSETS[pathname];
  if (!asset || (req.method !== 'GET' && req.method !== 'HEAD')) return false;

  // Mounted under a path (app.use('/queues', handler)) and opened without a
  // trailing slash: relative asset and API URLs would resolve one level too high.
  const original = (req as IncomingMessage & { originalUrl?: string }).originalUrl;
  if (pathname === '/' && original) {
    const [path, query] = original.split('?');
    if (path && !path.endsWith('/')) {
      res.writeHead(301, { location: `${path}/${query ? `?${query}` : ''}` });
      res.end();
      return true;
    }
  }

  let body = assetCache.get(asset.file);
  if (!body) {
    body = readFileSync(new URL(asset.file, UI_DIR));
    assetCache.set(asset.file, body);
  }
  res.writeHead(200, {
    'content-type': asset.type,
    'cache-control': 'no-cache',
    ...SECURITY_HEADERS,
  });
  res.end(req.method === 'HEAD' ? undefined : body);
  return true;
}

export function createDashboard(options: DashboardOptions = {}): Dashboard {
  const prefix = options.prefix ?? 'meridian';
  const ownsClient = !(options.connection instanceof Redis);
  const client =
    options.connection instanceof Redis
      ? options.connection
      : typeof options.connection === 'object'
        ? new Redis({ ...options.connection, maxRetriesPerRequest: null })
        : new Redis(options.connection ?? 'redis://127.0.0.1:6379', { maxRetriesPerRequest: null });

  const events = new EventHub(client, prefix);
  const api = createApi({ client, prefix, events });

  async function handler(req: IncomingMessage, res: ServerResponse, next?: () => void) {
    // Never let a request reject: callers typically fire-and-forget the handler,
    // and an unhandled rejection would take the whole process down.
    try {
      await handle(req, res, next);
    } catch (err) {
      console.error('[meridian] dashboard request failed:', err);
      if (!res.headersSent) sendJson(res, 500, { error: 'Internal error' });
      else res.end();
    }
  }

  async function handle(req: IncomingMessage, res: ServerResponse, next?: () => void) {
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
    if (serveAsset(req, res, url.pathname)) return;

    if (next) next();
    else sendJson(res, 404, { error: 'Not found' });
  }

  return {
    handler,
    async close() {
      await events.close();
      if (ownsClient) await client.quit();
    },
  };
}
