import type { IncomingMessage, ServerResponse } from 'node:http';

export interface RequestContext {
  req: IncomingMessage;
  res: ServerResponse;
  params: Record<string, string>;
  query: URLSearchParams;
}

export type Handler = (ctx: RequestContext) => Promise<unknown> | unknown;

/** An error whose message is safe to show to API clients. */
export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

interface Route {
  method: string;
  pattern: RegExp;
  keys: string[];
  handler: Handler;
}

/** A minimal router: `/queues/:name/jobs/:id` style paths, JSON responses. */
export class Router {
  private readonly routes: Route[] = [];

  on(method: string, path: string, handler: Handler): this {
    const keys: string[] = [];
    const source = path.replace(/:(\w+)/g, (_, key: string) => {
      keys.push(key);
      return '([^/]+)';
    });
    this.routes.push({ method, pattern: new RegExp(`^${source}$`), keys, handler });
    return this;
  }

  /** Returns false when no route matches, so the caller can serve something else. */
  async handle(req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> {
    let pathMatched = false;
    for (const route of this.routes) {
      const match = route.pattern.exec(url.pathname);
      if (!match) continue;
      pathMatched = true;
      if (route.method !== req.method) continue;

      try {
        const params: Record<string, string> = {};
        route.keys.forEach((key, i) => {
          params[key] = decodeParam(match[i + 1] ?? '');
        });
        const body = await route.handler({ req, res, params, query: url.searchParams });
        // Handlers that stream (SSE) write the response themselves.
        if (!res.headersSent) sendJson(res, 200, body ?? { ok: true });
      } catch (err) {
        if (err instanceof HttpError) sendJson(res, err.status, { error: err.message });
        else {
          console.error('[meridian] dashboard request failed:', err);
          sendJson(res, 500, { error: 'Internal error' });
        }
      }
      return true;
    }

    if (pathMatched) {
      sendJson(res, 405, { error: 'Method not allowed' });
      return true;
    }
    return false;
  }
}

function decodeParam(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new HttpError(400, 'Malformed URL encoding');
  }
}

export function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const json = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  });
  res.end(json);
}
