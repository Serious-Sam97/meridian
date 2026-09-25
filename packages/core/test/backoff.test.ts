import { describe, expect, it } from 'vitest';
import { computeBackoff } from '../src/backoff.js';

describe('computeBackoff', () => {
  it('returns 0 without a backoff option', () => {
    expect(computeBackoff(undefined, 3)).toBe(0);
  });

  it('treats a number as a fixed delay', () => {
    expect(computeBackoff(500, 1)).toBe(500);
    expect(computeBackoff(500, 4)).toBe(500);
  });

  it('keeps the same delay for the fixed strategy', () => {
    expect(computeBackoff({ type: 'fixed', delay: 200 }, 5)).toBe(200);
  });

  it('doubles the delay on each attempt for the exponential strategy', () => {
    const backoff = { type: 'exponential', delay: 100 } as const;
    expect([1, 2, 3, 4].map((n) => computeBackoff(backoff, n))).toEqual([100, 200, 400, 800]);
  });

  it('applies jitter as a reduction of at most the given fraction', () => {
    const backoff = { type: 'fixed', delay: 1000, jitter: 0.5 } as const;
    expect(computeBackoff(backoff, 1, () => 0)).toBe(1000);
    expect(computeBackoff(backoff, 1, () => 0.5)).toBe(750);
    expect(computeBackoff(backoff, 1, () => 1)).toBe(500);
  });

  it('clamps jitter to the 0..1 range', () => {
    expect(computeBackoff({ type: 'fixed', delay: 100, jitter: 5 }, 1, () => 1)).toBe(0);
    expect(computeBackoff({ type: 'fixed', delay: 100, jitter: -1 }, 1, () => 1)).toBe(100);
  });
});
