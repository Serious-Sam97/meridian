import type { Redis } from 'ioredis';
import { type ConnectionOptions, createConnection } from './connection.js';
import { Job } from './job.js';
import { type QueueKeys, queueKeys } from './keys.js';
import { runScript } from './scripts.js';
import {
  type JobCounts,
  type JobOptions,
  type JobState,
  MAX_PRIORITY,
  type MetricsBucket,
} from './types.js';

export type ListableState = Exclude<JobState, 'unknown'>;

export interface QueueOptions {
  connection?: ConnectionOptions;
  /** Key prefix. Defaults to `meridian`. */
  prefix?: string;
  /** Approximate cap for the events stream. Defaults to 10,000. */
  maxEvents?: number;
  /** Options applied to every job, overridden per job. */
  defaultJobOptions?: JobOptions;
}

export class Queue<Data = unknown> {
  readonly keys: QueueKeys;
  readonly client: Redis;
  private readonly ownsClient: boolean;
  private readonly maxEvents: number;
  private readonly defaultJobOptions: JobOptions;

  constructor(
    readonly name: string,
    options: QueueOptions = {},
  ) {
    this.keys = queueKeys(name, options.prefix);
    const { client, owned } = createConnection(options.connection);
    this.client = client;
    this.ownsClient = owned;
    this.maxEvents = options.maxEvents ?? 10_000;
    this.defaultJobOptions = options.defaultJobOptions ?? {};
  }

  async add(name: string, data: Data, options: JobOptions = {}): Promise<Job<Data>> {
    const opts = { ...this.defaultJobOptions, ...options };
    validateOptions(opts);

    const [id, created] = await runScript<[string, number]>(
      this.client,
      'addJob',
      [
        this.keys.id,
        this.keys.wait,
        this.keys.delayed,
        this.keys.marker,
        this.keys.events,
        this.keys.jobPrefix,
      ],
      [
        opts.jobId ?? '',
        name,
        JSON.stringify(data ?? null),
        JSON.stringify(opts),
        opts.priority ?? 0,
        opts.delay ?? 0,
        this.maxEvents,
      ],
    );

    if (created === 0) {
      // Duplicate id: hand back the job that already exists, not the rejected payload.
      const existing = await this.getJob(id);
      if (existing) return existing;
    }
    return new Job<Data>(id, name, data, opts, Date.now());
  }

  async getJob<Result = unknown>(id: string): Promise<Job<Data, Result> | undefined> {
    const hash = await this.client.hgetall(this.keys.job(id));
    return Object.keys(hash).length === 0 ? undefined : Job.fromHash<Data, Result>(id, hash);
  }

  async getJobState(id: string): Promise<JobState> {
    const pipeline = this.client.pipeline();
    const sets = ['waiting', 'delayed', 'active', 'completed', 'failed'] as const;
    const keys = [
      this.keys.wait,
      this.keys.delayed,
      this.keys.active,
      this.keys.completed,
      this.keys.failed,
    ];
    for (const key of keys) pipeline.zscore(key, id);
    const results = (await pipeline.exec()) ?? [];

    const index = results.findIndex(([, score]) => score !== null);
    return index === -1 ? 'unknown' : (sets[index] ?? 'unknown');
  }

  async getJobCounts(): Promise<JobCounts> {
    const [waiting, delayed, active, completed, failed] = await Promise.all([
      this.client.zcard(this.keys.wait),
      this.client.zcard(this.keys.delayed),
      this.client.zcard(this.keys.active),
      this.client.zcard(this.keys.completed),
      this.client.zcard(this.keys.failed),
    ]);
    return { waiting, delayed, active, completed, failed };
  }

  /**
   * Lists jobs in a state. Finished jobs come newest first; waiting jobs in
   * the order they will run; delayed jobs by due time.
   */
  async getJobs<Result = unknown>(
    state: ListableState,
    start = 0,
    end = 19,
  ): Promise<Job<Data, Result>[]> {
    const key = this.stateKey(state);
    const newestFirst = state === 'completed' || state === 'failed';
    const ids = newestFirst
      ? await this.client.zrevrange(key, start, end)
      : await this.client.zrange(key, String(start), String(end));
    if (ids.length === 0) return [];

    const pipeline = this.client.pipeline();
    for (const id of ids) pipeline.hgetall(this.keys.job(id));
    const hashes = (await pipeline.exec()) ?? [];

    const jobs: Job<Data, Result>[] = [];
    ids.forEach((id, i) => {
      const hash = hashes[i]?.[1] as Record<string, string> | undefined;
      // A job can be removed between the two calls by retention trimming.
      if (hash && Object.keys(hash).length > 0) jobs.push(Job.fromHash<Data, Result>(id, hash));
    });
    return jobs;
  }

  /** Moves a failed job back to the queue with a fresh set of attempts. */
  async retryJob(id: string): Promise<boolean> {
    const moved = await runScript<number>(
      this.client,
      'retryFailedJob',
      [this.keys.failed, this.keys.wait, this.keys.marker, this.keys.events, this.keys.jobPrefix],
      [id, this.maxEvents],
    );
    return moved === 1;
  }

  /** Retries up to `limit` failed jobs, oldest first. Returns how many were moved. */
  async retryAllFailed(limit = 1_000): Promise<number> {
    const ids = await this.client.zrange(this.keys.failed, '0', String(limit - 1));
    let moved = 0;
    for (const id of ids) if (await this.retryJob(id)) moved++;
    return moved;
  }

  /**
   * Deletes a job. Returns false when the job does not exist.
   * Throws when the job is active: its worker owns it until it finishes.
   */
  async removeJob(id: string): Promise<boolean> {
    const code = await runScript<number>(
      this.client,
      'removeJob',
      [
        this.keys.wait,
        this.keys.delayed,
        this.keys.completed,
        this.keys.failed,
        this.keys.events,
        this.keys.jobPrefix,
      ],
      [id, this.maxEvents],
    );
    if (code === -1) throw new Error(`Job ${id} is active and cannot be removed`);
    return code === 1;
  }

  /** Per-minute throughput and timings for the last `minutes` minutes, oldest first. */
  async getMetrics(minutes = 60): Promise<MetricsBucket[]> {
    const [seconds] = await this.client.time();
    const currentMinute = Math.floor((Number(seconds) * 1000) / 60_000) * 60_000;
    const timestamps = Array.from(
      { length: minutes },
      (_, i) => currentMinute - (minutes - 1 - i) * 60_000,
    );

    const pipeline = this.client.pipeline();
    for (const ts of timestamps) pipeline.hgetall(`${this.keys.metricsPrefix}${ts}`);
    const results = (await pipeline.exec()) ?? [];

    return timestamps.map((timestamp, i) => {
      const hash = (results[i]?.[1] ?? {}) as Record<string, string>;
      const completed = Number(hash.completed ?? 0);
      const failed = Number(hash.failed ?? 0);
      const finished = completed + failed;
      return {
        timestamp,
        completed,
        failed,
        avgRuntime: finished ? Math.round(Number(hash.runtime ?? 0) / finished) : 0,
        avgWait: finished ? Math.round(Number(hash.wait ?? 0) / finished) : 0,
      };
    });
  }

  /**
   * Finds the names of all queues under a prefix. Uses SCAN, so it is meant
   * for dashboards and tooling, not hot paths.
   */
  static async discover(client: Redis, prefix = 'meridian'): Promise<string[]> {
    const names = new Set<string>();
    let cursor = '0';
    do {
      const [next, keys] = await client.scan(cursor, 'MATCH', `${prefix}:{*}:id`, 'COUNT', 1_000);
      for (const key of keys) {
        const match = /^.*?:\{(.+)\}:id$/.exec(key);
        if (match?.[1]) names.add(match[1]);
      }
      cursor = next;
    } while (cursor !== '0');
    return [...names].sort();
  }

  private stateKey(state: ListableState): string {
    const keys: Record<ListableState, string> = {
      waiting: this.keys.wait,
      delayed: this.keys.delayed,
      active: this.keys.active,
      completed: this.keys.completed,
      failed: this.keys.failed,
    };
    return keys[state];
  }

  /**
   * Stops workers from taking new jobs. Jobs already running finish normally,
   * and jobs can still be added while paused.
   */
  async pause(): Promise<void> {
    await this.client
      .multi()
      .hset(this.keys.meta, 'paused', '1')
      .xadd(this.keys.events, 'MAXLEN', '~', this.maxEvents, '*', 'event', 'paused')
      .exec();
  }

  async resume(): Promise<void> {
    await this.client
      .multi()
      .hdel(this.keys.meta, 'paused')
      // Wake idle workers now instead of on their next poll.
      .lpush(this.keys.marker, '1')
      .ltrim(this.keys.marker, 0, 99)
      .xadd(this.keys.events, 'MAXLEN', '~', this.maxEvents, '*', 'event', 'resumed')
      .exec();
  }

  async isPaused(): Promise<boolean> {
    return (await this.client.hexists(this.keys.meta, 'paused')) === 1;
  }

  /** Deletes every key of this queue. Meant for tests and local development. */
  async obliterate(): Promise<void> {
    let cursor = '0';
    do {
      const [next, keys] = await this.client.scan(
        cursor,
        'MATCH',
        `${this.keys.base}*`,
        'COUNT',
        500,
      );
      if (keys.length > 0) await this.client.unlink(...keys);
      cursor = next;
    } while (cursor !== '0');
  }

  async close(): Promise<void> {
    if (this.ownsClient) await this.client.quit();
  }
}

function validateOptions(opts: JobOptions): void {
  const priority = opts.priority ?? 0;
  if (!Number.isInteger(priority) || priority < 0 || priority > MAX_PRIORITY) {
    throw new RangeError(`priority must be an integer between 0 and ${MAX_PRIORITY}`);
  }
  if (opts.delay !== undefined && (!Number.isFinite(opts.delay) || opts.delay < 0)) {
    throw new RangeError('delay must be a non-negative number');
  }
  if (opts.attempts !== undefined && (!Number.isInteger(opts.attempts) || opts.attempts < 1)) {
    throw new RangeError('attempts must be a positive integer');
  }
  if (opts.jobId !== undefined && (opts.jobId === '' || opts.jobId.includes(':'))) {
    throw new RangeError('jobId must be non-empty and must not contain ":"');
  }
}
