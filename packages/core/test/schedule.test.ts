import { describe, expect, it } from 'vitest';
import { nextRun } from '../src/index.js';

describe('nextRun', () => {
  it('aligns every-schedules to multiples of the interval', () => {
    expect(nextRun({ every: 1_000 }, 12_345)).toBe(13_000);
    expect(nextRun({ every: 1_000 }, 13_000)).toBe(14_000);
  });

  it('is strictly after the given time for cron patterns', () => {
    const at = Date.parse('2026-09-25T09:00:00Z');
    expect(nextRun({ pattern: '0 9 * * *', tz: 'UTC' }, at)).toBe(
      Date.parse('2026-09-26T09:00:00Z'),
    );
  });

  it('respects time zones', () => {
    // Friday noon in São Paulo: the next weekday 09:00 there is Monday, 12:00 UTC.
    const friday = Date.parse('2026-09-25T15:00:00Z');
    expect(nextRun({ pattern: '0 9 * * 1-5', tz: 'America/Sao_Paulo' }, friday)).toBe(
      Date.parse('2026-09-28T12:00:00Z'),
    );
  });

  it('supports a seconds field', () => {
    const at = Date.parse('2026-09-25T10:00:00.500Z');
    expect(nextRun({ pattern: '*/5 * * * * *', tz: 'UTC' }, at)).toBe(
      Date.parse('2026-09-25T10:00:05Z'),
    );
  });
});
