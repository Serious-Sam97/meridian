export type BackoffStrategy = 'fixed' | 'exponential';

export interface BackoffOptions {
  type: BackoffStrategy;
  /** Base delay in ms. */
  delay: number;
  /**
   * Randomizes the delay by up to this fraction (0..1) to avoid many failed
   * jobs retrying at the same moment. Defaults to 0.
   */
  jitter?: number;
}

export interface JobOptions {
  /** Custom id. Adding a job whose id already exists is a no-op. */
  jobId?: string;
  /** Milliseconds to wait before the job becomes available. */
  delay?: number;
  /** Lower runs first. 0 (the default) is the highest priority. */
  priority?: number;
  /** Total attempts including the first one. Defaults to 1 (no retries). */
  attempts?: number;
  /** Delay between retries: a fixed number of ms or a strategy. */
  backoff?: number | BackoffOptions;
  /** true removes the job on completion; a number keeps only the last N. */
  removeOnComplete?: boolean | number;
  /** true removes the job on final failure; a number keeps only the last N. */
  removeOnFail?: boolean | number;
  /** Set by Meridian on jobs created by a scheduler; do not set it yourself. */
  repeat?: { scheduler: string; runAt: number };
}

export type JobState = 'waiting' | 'delayed' | 'active' | 'completed' | 'failed' | 'unknown';

export type JobCounts = Record<Exclude<JobState, 'unknown'>, number>;

/** Jobs finished during one minute. */
export interface MetricsBucket {
  /** Start of the minute, in ms since the epoch (Redis clock). */
  timestamp: number;
  completed: number;
  failed: number;
  /** Average processing time of the jobs finished in this minute, in ms. */
  avgRuntime: number;
  /** Average time from creation to the start of the final attempt, in ms. */
  avgWait: number;
}

/** At most `max` jobs start per `duration` ms, across all workers of a queue. */
export interface RateLimit {
  max: number;
  duration: number;
}

export const MAX_PRIORITY = 2 ** 20;
