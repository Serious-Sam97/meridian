import type { WorkerOptions } from '@meridian/core';
import type { RedisOptions } from 'ioredis';

/**
 * Replaces a worker process once it crosses a limit, like Horizon's `memory`
 * and `maxJobs`. Guards against leaks in job code without anyone restarting
 * workers by hand.
 */
export interface RecycleOptions {
  /** Resident memory (RSS) in MB above which the process is replaced. */
  maxMemory?: number;
  /** Jobs a process handles before it is replaced. */
  maxJobs?: number;
  /** Lifetime of a process in ms before it is replaced. */
  maxTime?: number;
}

/** What a worker process needs to know, passed to it as JSON in an env var. */
export interface ChildConfig {
  queue: string;
  /** Absolute path of a module whose default export is the processor. */
  processor: string;
  connection: string | RedisOptions;
  prefix?: string;
  concurrency: number;
  /** How long a scale-down waits for running jobs before releasing them, in ms. */
  shutdownTimeout: number;
  recycle?: RecycleOptions;
  workerOptions?: Pick<
    WorkerOptions,
    'lockDuration' | 'blockTimeout' | 'stalledInterval' | 'maxStalledCount'
  >;
}

export const CHILD_CONFIG_ENV = 'MERIDIAN_CHILD_CONFIG';
