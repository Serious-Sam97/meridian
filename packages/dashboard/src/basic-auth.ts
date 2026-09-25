import { createHash, timingSafeEqual } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';

/**
 * HTTP Basic authentication for the standalone dashboard. Browsers prompt
 * for it natively and resend it on every request, EventSource included.
 */
export function basicAuth(credentials: string): (req: IncomingMessage) => boolean {
  if (!credentials.includes(':')) throw new Error('Credentials must look like user:password');
  // Hash both sides so the comparison is constant-time regardless of length.
  const expected = digest(credentials);

  return (req) => {
    const header = req.headers.authorization ?? '';
    if (!header.startsWith('Basic ')) return false;
    const given = Buffer.from(header.slice(6), 'base64').toString('utf8');
    return timingSafeEqual(digest(given), expected);
  };
}

export function requestCredentials(res: ServerResponse): void {
  res.writeHead(401, { 'www-authenticate': 'Basic realm="Meridian", charset="UTF-8"' });
  res.end('Authentication required');
}

function digest(value: string): Buffer {
  return createHash('sha256').update(value).digest();
}
