import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import type { Redis } from 'ioredis';
import { computeBackoff } from './backoff.js';
import { type ConnectionOptions, createConnection, duplicateConnection } from './connection.js';
import { LockLostError, UnrecoverableError } from './errors.js';
import { Job } from './job.js';
import { type QueueKeys, queueKeys } from './keys.js';
import { runScript } from './scripts.js';

/**
 * Handles one job. The signal is aborted when the worker loses the job's lock,
 * so long-running handlers can stop early; their result would be discarded.
 */
export type Processor<Data, Result> = (
  job: Job<Data, Result>,
  signal: AbortSignal,
) => Promise<Result>;

interface ActiveJob {
  token: string;
  controller: AbortController;
  task: Promise<void>;
  /** Set once the result is being written, when the lock is released on purpose. */
  finishing: boolean;
}

export interface WorkerOptions {
  connection?: ConnectionOptions;
  prefix?: string;
  /** Jobs processed in parallel by this worker. Defaults to 1. */
  concurrency?: number;
  /** How long a job lock lives without renewal, in ms. Defaults to 30s. */
  lockDuration?: number;
  /** Longest time an idle worker sleeps before polling again, in ms. Defaults to 5s. */
  blockTimeout?: number;
  /**
   * How often to look for jobs abandoned by dead workers, in ms. Defaults to 30s.
   * The check is throttled queue-wide, so use the same value on every worker.
   */
  stalledInterval?: number;
  /** Times a job may stall before it is failed instead of recovered. Defaults to 1. */
  maxStalledCount?: number;
  maxEvents?: number;
  /** Start processing immediately. Defaults to true. */
  autorun?: boolean;
}

export interface WorkerEvents<Data, Result> {
  active: [job: Job<Data, Result>];
  completed: [job: Job<Data, Result>, result: Result];
  /** The attempt failed and the job will run again after `delay` ms. */
  retrying: [job: Job<Data, Result>, error: Error, delay: number];
  /** The job failed for good: no attempts left, or an UnrecoverableError. */
  failed: [job: Job<Data, Result>, error: Error];
  /** Jobs this worker found abandoned by a dead worker and put back in the queue. */
  stalled: [jobIds: string[]];
  error: [error: Error];
}

export class Worker<Data = unknown, Result = unknown> extends EventEmitter<
  WorkerEvents<Data, Result>
> {
  readonly keys: QueueKeys;
  private readonly client: Redis;
  private readonly ownsClient: boolean;
  private readonly blockingClient: Redis;
  private readonly concurrency: number;
  private readonly lockDuration: number;
  private readonly blockTimeout: number;
  private readonly stalledInterval: number;
  private readonly maxStalledCount: number;
  private readonly maxEvents: number;

  private readonly active = new Map<string, ActiveJob>();
  private running?: Promise<void>;
  private lockTimer?: NodeJS.Timeout;
  private stalledTimer?: NodeJS.Timeout;
  private closing?: Promise<void>;

  constructor(
    readonly queueName: string,
    private readonly processor: Processor<Data, Result>,
    options: WorkerOptions = {},
  ) {
    super();
    this.keys = queueKeys(queueName, options.prefix);
    const { client, owned } = createConnection(options.connection);
    this.client = client;
    this.ownsClient = owned;
    this.blockingClient = duplicateConnection(client);
    this.concurrency = options.concurrency ?? 1;
    this.lockDuration = options.lockDuration ?? 30_000;
    this.blockTimeout = options.blockTimeout ?? 5_000;
    this.stalledInterval = options.stalledInterval ?? 30_000;
    this.maxStalledCount = options.maxStalledCount ?? 1;
    this.maxEvents = options.maxEvents ?? 10_000;

    if (!Number.isInteger(this.concurrency) || this.concurrency < 1) {
      throw new RangeError('concurrency must be a positive integer');
    }
    if (options.autorun ?? true) this.run();
  }

  get isRunning(): boolean {
    return this.running !== undefined && this.closing === undefined;
  }

  run(): void {
    if (this.running) return;
    this.running = this.loop().catch((err) => this.reportError(err));
    this.lockTimer = setInterval(() => void this.extendLocks(), this.lockDuration / 2);
    // Check right away too: jobs left behind by a crashed process are recovered on restart.
    void this.checkStalled();
    this.stalledTimer = setInterval(() => void this.checkStalled(), this.stalledInterval);
  }

  /** Stops fetching new jobs and waits for the ones in progress to finish. */
  close(): Promise<void> {
    this.closing ??= this.shutdown();
    return this.closing;
  }

  private async loop(): Promise<void> {
    while (!this.closing) {
      if (this.active.size >= this.concurrency) {
        await Promise.race(this.activeTasks());
        continue;
      }

      let fetched: Fetched<Data, Result>;
      try {
        fetched = await this.fetchNext();
      } catch (err) {
        if (this.closing) break;
        this.reportError(err);
        await delay(1_000);
        continue;
      }

      if ('sleepMs' in fetched) {
        await this.waitForWork(fetched.sleepMs);
        continue;
      }

      const { job, token } = fetched;
      const controller = new AbortController();
      const task = this.process(job, token, controller.signal).finally(() =>
        this.active.delete(job.id),
      );
      this.active.set(job.id, { token, controller, task, finishing: false });
    }
  }

  private async fetchNext(): Promise<Fetched<Data, Result>> {
    const token = randomUUID();
    const reply = await runScript<[string, string[]] | [number]>(
      this.client,
      'moveToActive',
      [this.keys.wait, this.keys.active, this.keys.delayed, this.keys.events, this.keys.jobPrefix],
      [token, this.lockDuration, this.maxEvents],
    );

    if (reply.length === 1) {
      // Nothing ready: sleep until the next delayed job is due, but no longer
      // than blockTimeout so the loop still checks in regularly.
      const [untilDelayed] = reply;
      const sleepMs =
        untilDelayed < 0 ? this.blockTimeout : Math.min(untilDelayed, this.blockTimeout);
      return { sleepMs };
    }

    const [id, flat] = reply;
    return { job: Job.fromHash<Data, Result>(id, toObject(flat)), token };
  }

  /** Sleeps on the marker list until a job is added or the timeout passes. */
  private async waitForWork(timeoutMs: number): Promise<void> {
    // BLPOP treats 0 as "forever" and has 10ms resolution.
    const seconds = Math.max(timeoutMs, 10) / 1000;
    try {
      await this.blockingClient.blpop(this.keys.marker, seconds);
    } catch (err) {
      // close() disconnects the blocking client to interrupt BLPOP.
      if (!this.closing) {
        this.reportError(err);
        await delay(1_000);
      }
    }
  }

  private async process(job: Job<Data, Result>, token: string, signal: AbortSignal): Promise<void> {
    let outcome: { ok: true; result: Result } | { ok: false; error: Error };
    try {
      this.emit('active', job);
      outcome = { ok: true, result: await this.processor(job, signal) };
    } catch (err) {
      outcome = { ok: false, error: toError(err) };
    }

    // Another worker may already own the job; the loss was reported by extendLocks.
    if (signal.aborted) return;
    const entry = this.active.get(job.id);
    if (entry) entry.finishing = true;

    // Errors from here on are infrastructure problems (Redis down, lock lost),
    // not job failures, so they go to the 'error' event instead.
    try {
      if (outcome.ok) {
        const value = JSON.stringify(outcome.result ?? null);
        await this.finish(job, token, 'completed', value, '', job.opts.removeOnComplete);
        job.returnValue = outcome.result;
        this.emit('completed', job, outcome.result);
      } else if (this.shouldRetry(job, outcome.error)) {
        const { error } = outcome;
        const backoff = computeBackoff(job.opts.backoff, job.attemptsMade + 1);
        await this.retry(job, token, backoff, error);
        job.failedReason = error.message;
        this.emit('retrying', job, error, backoff);
      } else {
        const { error } = outcome;
        await this.finish(
          job,
          token,
          'failed',
          error.message,
          error.stack ?? '',
          job.opts.removeOnFail,
        );
        job.failedReason = error.message;
        this.emit('failed', job, error);
      }
    } catch (err) {
      this.reportError(err);
    }
  }

  private async finish(
    job: Job<Data, Result>,
    token: string,
    event: 'completed' | 'failed',
    value: string,
    stacktrace: string,
    removeOption: boolean | number | undefined,
  ): Promise<void> {
    const code = await runScript<number>(
      this.client,
      'moveToFinished',
      [
        this.keys.active,
        event === 'completed' ? this.keys.completed : this.keys.failed,
        this.keys.events,
        this.keys.jobPrefix,
      ],
      [
        job.id,
        token,
        event,
        event === 'completed' ? 'returnValue' : 'failedReason',
        value,
        stacktrace,
        retention(removeOption),
        this.maxEvents,
      ],
    );
    if (code !== 0) throw new LockLostError(job.id);
    job.attemptsMade += 1;
  }

  private shouldRetry(job: Job<Data, Result>, error: Error): boolean {
    if (error instanceof UnrecoverableError) return false;
    return job.attemptsMade + 1 < (job.opts.attempts ?? 1);
  }

  private async retry(
    job: Job<Data, Result>,
    token: string,
    backoff: number,
    error: Error,
  ): Promise<void> {
    const code = await runScript<number>(
      this.client,
      'retryJob',
      [
        this.keys.active,
        this.keys.wait,
        this.keys.delayed,
        this.keys.marker,
        this.keys.events,
        this.keys.jobPrefix,
      ],
      [job.id, token, backoff, error.message, error.stack ?? '', this.maxEvents],
    );
    if (code !== 0) throw new LockLostError(job.id);
    job.attemptsMade += 1;
  }

  /** Renews the locks of all active jobs and aborts the ones that were lost. */
  private async extendLocks(): Promise<void> {
    if (this.active.size === 0) return;

    const args: (string | number)[] = [this.lockDuration];
    for (const [id, { token }] of this.active) args.push(id, token);

    let lost: string[];
    try {
      lost = await runScript<string[]>(this.client, 'extendLocks', [this.keys.jobPrefix], args);
    } catch (err) {
      this.reportError(err);
      return;
    }

    for (const id of lost) {
      const entry = this.active.get(id);
      if (!entry || entry.finishing || entry.controller.signal.aborted) continue;
      const error = new LockLostError(id);
      entry.controller.abort(error);
      this.reportError(error);
    }
  }

  private async checkStalled(): Promise<void> {
    try {
      const [recovered, failed] = await runScript<[string[], string[]]>(
        this.client,
        'moveStalledJobs',
        [
          this.keys.stalledCheck,
          this.keys.active,
          this.keys.wait,
          this.keys.failed,
          this.keys.marker,
          this.keys.events,
          this.keys.jobPrefix,
        ],
        [this.stalledInterval, this.maxStalledCount, this.maxEvents],
      );
      if (recovered.length > 0 || failed.length > 0)
        this.emit('stalled', [...recovered, ...failed]);
    } catch (err) {
      if (!this.closing) this.reportError(err);
    }
  }

  private activeTasks(): Promise<void>[] {
    return [...this.active.values()].map((entry) => entry.task);
  }

  private async shutdown(): Promise<void> {
    this.blockingClient.disconnect();
    await this.running;
    await Promise.allSettled(this.activeTasks());
    clearInterval(this.lockTimer);
    clearInterval(this.stalledTimer);
    if (this.ownsClient) await this.client.quit();
  }

  private reportError(err: unknown): void {
    const error = toError(err);
    if (this.listenerCount('error') > 0) this.emit('error', error);
    else console.error(`[meridian] worker error on queue "${this.queueName}":`, error);
  }
}

type Fetched<Data, Result> = { job: Job<Data, Result>; token: string } | { sleepMs: number };

function retention(option: boolean | number | undefined): number {
  if (option === true) return 0;
  if (option === false || option === undefined) return -1;
  return Math.max(0, Math.floor(option));
}

function toObject(flat: string[]): Record<string, string> {
  const hash: Record<string, string> = {};
  for (let i = 0; i < flat.length; i += 2) hash[flat[i] as string] = flat[i + 1] as string;
  return hash;
}

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
