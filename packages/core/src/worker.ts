import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import type { Redis } from 'ioredis';
import { type ConnectionOptions, createConnection, duplicateConnection } from './connection.js';
import { LockLostError } from './errors.js';
import { Job } from './job.js';
import { type QueueKeys, queueKeys } from './keys.js';
import { runScript } from './scripts.js';

export type Processor<Data, Result> = (job: Job<Data, Result>) => Promise<Result>;

export interface WorkerOptions {
  connection?: ConnectionOptions;
  prefix?: string;
  /** Jobs processed in parallel by this worker. Defaults to 1. */
  concurrency?: number;
  /** How long a job lock lives without renewal, in ms. Defaults to 30s. */
  lockDuration?: number;
  /** Longest time an idle worker sleeps before polling again, in ms. Defaults to 5s. */
  blockTimeout?: number;
  maxEvents?: number;
  /** Start processing immediately. Defaults to true. */
  autorun?: boolean;
}

export interface WorkerEvents<Data, Result> {
  active: [job: Job<Data, Result>];
  completed: [job: Job<Data, Result>, result: Result];
  failed: [job: Job<Data, Result>, error: Error];
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
  private readonly maxEvents: number;

  private readonly active = new Map<string, Promise<void>>();
  private running?: Promise<void>;
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
  }

  /** Stops fetching new jobs and waits for the ones in progress to finish. */
  close(): Promise<void> {
    this.closing ??= this.shutdown();
    return this.closing;
  }

  private async loop(): Promise<void> {
    while (!this.closing) {
      if (this.active.size >= this.concurrency) {
        await Promise.race(this.active.values());
        continue;
      }

      let fetched: { job: Job<Data, Result>; token: string } | undefined;
      try {
        fetched = await this.fetchNext();
      } catch (err) {
        if (this.closing) break;
        this.reportError(err);
        await delay(1_000);
        continue;
      }

      if (!fetched) {
        await this.waitForWork();
        continue;
      }

      const { job, token } = fetched;
      const task = this.process(job, token).finally(() => this.active.delete(job.id));
      this.active.set(job.id, task);
    }
  }

  private async fetchNext(): Promise<{ job: Job<Data, Result>; token: string } | undefined> {
    const token = randomUUID();
    const reply = await runScript<[string, string[]] | []>(
      this.client,
      'moveToActive',
      [this.keys.wait, this.keys.active, this.keys.events, this.keys.jobPrefix],
      [token, this.lockDuration, this.maxEvents],
    );
    if (reply.length === 0) return undefined;

    const [id, flat] = reply;
    return { job: Job.fromHash<Data, Result>(id, toObject(flat)), token };
  }

  /** Sleeps on the marker list until a job is added or the timeout passes. */
  private async waitForWork(): Promise<void> {
    try {
      await this.blockingClient.blpop(this.keys.marker, this.blockTimeout / 1000);
    } catch (err) {
      // close() disconnects the blocking client to interrupt BLPOP.
      if (!this.closing) {
        this.reportError(err);
        await delay(1_000);
      }
    }
  }

  private async process(job: Job<Data, Result>, token: string): Promise<void> {
    let outcome: { ok: true; result: Result } | { ok: false; error: Error };
    try {
      this.emit('active', job);
      outcome = { ok: true, result: await this.processor(job) };
    } catch (err) {
      outcome = { ok: false, error: toError(err) };
    }

    // Errors from here on are infrastructure problems (Redis down, lock lost),
    // not job failures, so they go to the 'error' event instead.
    try {
      if (outcome.ok) {
        const value = JSON.stringify(outcome.result ?? null);
        await this.finish(job, token, 'completed', value, '', job.opts.removeOnComplete);
        job.returnValue = outcome.result;
        this.emit('completed', job, outcome.result);
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

  private async shutdown(): Promise<void> {
    this.blockingClient.disconnect();
    await this.running;
    await Promise.allSettled(this.active.values());
    if (this.ownsClient) await this.client.quit();
  }

  private reportError(err: unknown): void {
    const error = toError(err);
    if (this.listenerCount('error') > 0) this.emit('error', error);
    else console.error(`[meridian] worker error on queue "${this.queueName}":`, error);
  }
}

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
