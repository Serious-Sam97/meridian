import { CronExpressionParser } from 'cron-parser';
import type { Redis } from 'ioredis';

/** When a scheduler runs: every N ms, or on a cron pattern (5 or 6 fields). */
export type Schedule = { every: number } | { pattern: string; tz?: string };

/**
 * The first run strictly after `after` (ms). `every` runs are aligned to
 * multiples of the interval, so re-upserting a scheduler keeps its next run.
 */
export function nextRun(schedule: Schedule, after: number): number {
  if ('every' in schedule) {
    return Math.floor(after / schedule.every) * schedule.every + schedule.every;
  }
  return CronExpressionParser.parse(schedule.pattern, {
    currentDate: new Date(after),
    ...(schedule.tz ? { tz: schedule.tz } : {}),
  })
    .next()
    .getTime();
}

export function validateSchedule(schedule: Schedule): void {
  if ('every' in schedule) {
    if (!Number.isInteger(schedule.every) || schedule.every < 1) {
      throw new RangeError('every must be a positive integer (ms)');
    }
    return;
  }
  try {
    nextRun(schedule, Date.now());
  } catch (err) {
    throw new RangeError(`Invalid cron pattern "${schedule.pattern}": ${(err as Error).message}`);
  }
}

/** Current time on the Redis clock, in ms. */
export async function redisNow(client: Redis): Promise<number> {
  const [seconds, micros] = await client.time();
  return Number(seconds) * 1000 + Math.floor(Number(micros) / 1000);
}
