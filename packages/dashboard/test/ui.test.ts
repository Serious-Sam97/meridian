import type { Server } from 'node:http';
import type { Redis } from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createDashboard, type Dashboard } from '../src/index.js';
import { createRedis, serve } from './helpers.js';

describe('dashboard ui', () => {
  let redis: Redis;
  let dashboard: Dashboard;
  let server: Server;
  let base: string;

  beforeAll(async () => {
    redis = createRedis();
    dashboard = createDashboard({ connection: redis, prefix: 'ui-test' });
    ({ server, url: base } = await serve(dashboard));
  });

  afterAll(async () => {
    server.close();
    await dashboard.close();
    await redis.quit();
  });

  it('serves the page with a strict content security policy', async () => {
    const res = await fetch(`${base}/`);
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('text/html');
    expect(res.headers.get('content-security-policy')).toContain("script-src 'self'");
    expect(res.headers.get('content-security-policy')).toContain("frame-ancestors 'none'");
    expect(res.headers.get('x-content-type-options')).toBe('nosniff');
    // Relative URLs keep the dashboard working under any mount path.
    const html = await res.text();
    expect(html).toContain('src="assets/app.js"');
    expect(html).not.toMatch(/<script>(?!\s*<\/script>)/);
  });

  it('serves the script and stylesheet', async () => {
    const js = await fetch(`${base}/assets/app.js`);
    const css = await fetch(`${base}/assets/app.css`);
    expect(js.headers.get('content-type')).toContain('text/javascript');
    expect(css.headers.get('content-type')).toContain('text/css');
    expect(await js.text()).toContain('EventSource');
  });

  it('never renders job data as html', async () => {
    const js = await (await fetch(`${base}/assets/app.js`)).text();
    expect(js).not.toMatch(/\.innerHTML\s*=/);
    expect(js).not.toMatch(/insertAdjacentHTML/);
  });

  it('redirects to a trailing slash when mounted under a path', async () => {
    const res = { headers: {} as Record<string, unknown>, status: 0, ended: false };
    const fakeRes = {
      writeHead(status: number, headers: Record<string, unknown>) {
        res.status = status;
        res.headers = headers;
      },
      end() {
        res.ended = true;
      },
    };
    // What Express passes when the dashboard is mounted with app.use('/queues', handler).
    const req = { method: 'GET', url: '/', originalUrl: '/queues?tab=failed', headers: {} };
    await dashboard.handler(req as never, fakeRes as never);
    expect(res.status).toBe(301);
    expect(res.headers.location).toBe('/queues/?tab=failed');
  });

  it('returns 404 for unknown paths', async () => {
    expect((await fetch(`${base}/nope`)).status).toBe(404);
  });

  it('passes unknown paths to next() when used as middleware', async () => {
    let called = false;
    const fakeRes = { writeHead() {}, end() {} };
    await dashboard.handler(
      { method: 'GET', url: '/nope', headers: {} } as never,
      fakeRes as never,
      () => {
        called = true;
      },
    );
    expect(called).toBe(true);
  });
});
