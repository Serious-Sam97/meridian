import type { Redis } from 'ioredis';
import { type ConnectionOptions, createConnection } from './connection.js';
import { Job } from './job.js';
import { type QueueKeys, queueKeys } from './keys.js';
import { runScript } from './scripts.js';
import { type JobCounts, type JobOptions, type JobState, MAX_PRIORITY } from './types.js';

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
