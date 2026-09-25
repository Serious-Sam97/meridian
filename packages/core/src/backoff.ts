import type { BackoffOptions } from './types.js';

/**
 * Delay before the next attempt, in ms.
 *
 * @param attemptsMade attempts already made, including the one that just failed (>= 1)
 * @param random injectable for tests; defaults to Math.random
 */
export function computeBackoff(
  backoff: number | BackoffOptions | undefined,
  attemptsMade: number,
  random: () => number = Math.random,
): number {
  if (backoff === undefined) return 0;
  if (typeof backoff === 'number') return Math.max(0, backoff);

  const base =
    backoff.type === 'exponential' ? backoff.delay * 2 ** (attemptsMade - 1) : backoff.delay;

  // Jitter only shortens the delay, so `delay` stays an upper bound.
  const jitter = Math.min(Math.max(backoff.jitter ?? 0, 0), 1);
  return Math.round(base * (1 - jitter * random()));
}
