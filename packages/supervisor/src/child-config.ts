import type { WorkerOptions } from '@meridian/core';
import type { RedisOptions } from 'ioredis';

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
  workerOptions?: Pick<
    WorkerOptions,
    'lockDuration' | 'blockTimeout' | 'stalledInterval' | 'maxStalledCount'
  >;
}

export const CHILD_CONFIG_ENV = 'MERIDIAN_CHILD_CONFIG';
