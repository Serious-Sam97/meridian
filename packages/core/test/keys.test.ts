import { describe, expect, it } from 'vitest';
import { queueKeys } from '../src/keys.js';

describe('queueKeys', () => {
  it('wraps the queue name in a cluster hash tag', () => {
    const keys = queueKeys('emails');
    expect(keys.wait).toBe('meridian:{emails}:wait');
    expect(keys.job('42')).toBe('meridian:{emails}:job:42');
    expect(keys.lock('42')).toBe('meridian:{emails}:job:42:lock');
  });

  it('supports a custom prefix', () => {
    expect(queueKeys('emails', 'app').delayed).toBe('app:{emails}:delayed');
  });

  it('rejects names that would break the hash tag', () => {
    expect(() => queueKeys('')).toThrow();
    expect(() => queueKeys('a{b}')).toThrow();
  });
});
