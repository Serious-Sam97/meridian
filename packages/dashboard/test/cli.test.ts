import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { basicAuth } from '../src/index.js';
import { REDIS_URL, waitFor } from './helpers.js';

const CLI = fileURLToPath(new URL('../src/cli.ts', import.meta.url));

function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const server = createServer().listen(0, () => {
      const address = server.address();
      server.close(() => resolve(typeof address === 'object' && address ? address.port : 0));
    });
  });
}

describe('basicAuth', () => {
  const check = basicAuth('admin:s3cret');
  const request = (value?: string) =>
    ({ headers: value ? { authorization: value } : {} }) as Parameters<typeof check>[0];
  const encode = (s: string) => `Basic ${Buffer.from(s).toString('base64')}`;

  it('accepts the right credentials only', () => {
    expect(check(request(encode('admin:s3cret')))).toBe(true);
    expect(check(request(encode('admin:wrong')))).toBe(false);
    expect(check(request(encode('admin:s3cret-and-more')))).toBe(false);
    expect(check(request('Bearer s3cret'))).toBe(false);
    expect(check(request())).toBe(false);
  });

  it('rejects malformed credentials in the configuration', () => {
    expect(() => basicAuth('no-colon')).toThrow();
  });
});

describe('meridian-dashboard CLI', { timeout: 20_000 }, () => {
  it('serves the dashboard behind basic auth and stops on SIGTERM', async () => {
    const port = await freePort();
    const cli = spawn(
      process.execPath,
      [
        '--import',
        'tsx',
        '--conditions=@meridian/source',
        CLI,
        '--port',
        String(port),
        '--redis',
        REDIS_URL,
      ],
      { env: { ...process.env, MERIDIAN_DASHBOARD_AUTH: 'admin:s3cret' }, stdio: 'pipe' },
    );
    const exited = new Promise<number | null>((resolve) => cli.once('exit', resolve));
    const url = `http://127.0.0.1:${port}/api/overview`;

    try {
      await waitFor(
        async () => {
          try {
            await fetch(url);
            return true;
          } catch {
            return false;
          }
        },
        { timeout: 10_000, interval: 100 },
      );

      const anonymous = await fetch(url);
      expect(anonymous.status).toBe(401);
      expect(anonymous.headers.get('www-authenticate')).toContain('Basic');

      const authorized = await fetch(url, {
        headers: { authorization: `Basic ${Buffer.from('admin:s3cret').toString('base64')}` },
      });
      expect(authorized.status).toBe(200);

      cli.kill('SIGTERM');
      expect(await exited).toBe(0);
    } finally {
      cli.kill('SIGKILL');
    }
  });
});
