import { EventEmitter } from 'node:events';
import { hostname } from 'node:os';
import { resolve } from 'node:path';
import { Queue } from '@meridian/core';
import { Redis, type RedisOptions } from 'ioredis';
import { type Allocation, type BalanceStrategy, balance, type QueueLoad } from './balancer.js';
import type { ChildConfig, RecycleOptions } from './child-config.js';
import { ProcessPool } from './pool.js';

type ChildWorkerOptions = ChildConfig['workerOptions'];

export interface SupervisedQueue {
  /** Path of a module whose default export is the processor for this queue. */
  processor: string;
  /** Jobs each process runs at once. Defaults to 1. */
  concurrency?: number;
  workerOptions?: ChildWorkerOptions;
  /** Recycle limits for this queue's processes, overriding the supervisor's. */
  recycle?: RecycleOptions;
}

export interface SupervisorOptions {
  /** Shown in the dashboard. Defaults to `<hostname>:<pid>`. */
  name?: string;
  /** Redis URL or options. Must be serializable: child processes connect with it too. */
  connection?: string | RedisOptions;
  prefix?: string;
  queues: Record<string, SupervisedQueue>;
  /** Defaults to 'auto'. */
  balance?: BalanceStrategy;
  /** Defaults to 1. */
  minProcesses?: number;
  /** Defaults to 10. */
  maxProcesses?: number;
  /** Defaults to 1, like Horizon's balanceMaxShift. */
  maxShift?: number;
  /** Time between balancing rounds, in ms. Defaults to 3s, like Horizon's balanceCooldown. */
  balanceInterval?: number;
  /** Minutes of metrics used to estimate job runtime. Defaults to 5. */
  metricsWindow?: number;
  /** How long a stopping process waits for its jobs, in ms. Defaults to 10s. */
  shutdownTimeout?: number;
  /** Replace worker processes that cross these limits (Horizon's memory/maxJobs). */
  recycle?: RecycleOptions;
  /** Worker options applied to every queue, overridden per queue. */
  workerOptions?: ChildWorkerOptions;
  /** Extra Node flags for worker processes. */
  execArgv?: string[];
}

export interface SupervisorStatus {
  name: string;
  host: string;
  pid: number;
  strategy: BalanceStrategy;
  maxProcesses: number;
  startedAt: number;
  updatedAt: number;
  queues: Record<string, { processes: number; target: number }>;
}

export interface SupervisorEvents {
  scaled: [allocation: Allocation];
  crash: [queue: string, pid: number, restartIn: number];
  recycle: [queue: string, pid: number, reason: string];
  error: [error: Error];
}

export function supervisorsKey(prefix = 'meridian'): string {
  return `${prefix}:supervisors`;
}

/**
 * Runs a pool of worker processes per queue and rebalances them by workload
 * (ADR 0004). The supervisor itself never runs job code.
 */
export class Supervisor extends EventEmitter<SupervisorEvents> {
  readonly name: string;
  private readonly client: Redis;
  private readonly queues = new Map<string, Queue>();
  private readonly pools = new Map<string, ProcessPool>();
  private readonly strategy: BalanceStrategy;
  private readonly minProcesses: number;
  private readonly maxProcesses: number;
  private readonly maxShift: number;
  private readonly balanceInterval: number;
  private readonly metricsWindow: number;
  private readonly prefix: string;

  private startedAt = 0;
  private balancedOnce = false;
  private timer?: NodeJS.Timeout;
  private balancing?: Promise<void>;
  private stopping?: Promise<void>;

  constructor(private readonly options: SupervisorOptions) {
    super();
    this.name = options.name ?? `${hostname()}:${process.pid}`;
    this.prefix = options.prefix ?? 'meridian';
    this.strategy = options.balance ?? 'auto';
    this.minProcesses = options.minProcesses ?? 1;
    this.maxProcesses = options.maxProcesses ?? 10;
    this.maxShift = options.maxShift ?? 1;
    this.balanceInterval = options.balanceInterval ?? 3_000;
    this.metricsWindow = options.metricsWindow ?? 5;

    const names = Object.keys(options.queues);
    if (names.length === 0) throw new Error('A supervisor needs at least one queue');
    if (this.maxProcesses < names.length * this.minProcesses) {
      throw new RangeError(
        `maxProcesses (${this.maxProcesses}) is lower than minProcesses x queues (${names.length * this.minProcesses})`,
      );
    }

    const connection = options.connection ?? 'redis://127.0.0.1:6379';
    this.client =
      typeof connection === 'string'
        ? new Redis(connection, { maxRetriesPerRequest: null })
        : new Redis({ ...connection, maxRetriesPerRequest: null });

    for (const [queue, config] of Object.entries(options.queues)) {
      this.queues.set(queue, new Queue(queue, { connection: this.client, prefix: this.prefix }));
      const pool = new ProcessPool({
        child: {
          queue,
          processor: resolve(config.processor),
          connection,
          prefix: this.prefix,
          concurrency: config.concurrency ?? 1,
          shutdownTimeout: options.shutdownTimeout ?? 10_000,
          workerOptions: { ...options.workerOptions, ...config.workerOptions },
          recycle: { ...options.recycle, ...config.recycle },
        },
        execArgv: options.execArgv,
      });
      pool.on('crash', (pid, _code, _signal, restartIn) =>
        this.emit('crash', queue, pid, restartIn),
      );
      pool.on('recycle', (pid, reason) => this.emit('recycle', queue, pid, reason));
      this.pools.set(queue, pool);
    }
  }

  async start(): Promise<void> {
    if (this.startedAt) return;
    this.startedAt = Date.now();
    await this.rebalance();
    this.timer = setInterval(() => {
      // Rounds never overlap: a slow scale-down just delays the next one.
      this.balancing ??= this.rebalance().finally(() => {
        this.balancing = undefined;
      });
    }, this.balanceInterval);
  }

  /** Stops every worker process (letting them finish their jobs) and the supervisor. */
  stop(): Promise<void> {
    this.stopping ??= this.shutdown();
    return this.stopping;
  }

  /** Current process counts per queue. */
  get allocation(): Allocation {
    return Object.fromEntries([...this.pools].map(([queue, pool]) => [queue, pool.size]));
  }

  status(): SupervisorStatus {
    return {
      name: this.name,
      host: hostname(),
      pid: process.pid,
      strategy: this.strategy,
      maxProcesses: this.maxProcesses,
      startedAt: this.startedAt,
      updatedAt: Date.now(),
      queues: Object.fromEntries(
        [...this.pools].map(([queue, pool]) => [
          queue,
          { processes: pool.size, target: pool.target },
        ]),
      ),
    };
  }

  /** Supervisors whose heartbeat is younger than `maxAge` ms. */
  static async list(
    client: Redis,
    prefix = 'meridian',
    maxAge = 15_000,
  ): Promise<SupervisorStatus[]> {
    const entries = await client.hgetall(supervisorsKey(prefix));
    const now = Date.now();
    return Object.values(entries)
      .map((raw) => JSON.parse(raw) as SupervisorStatus)
      .filter((status) => now - status.updatedAt <= maxAge)
      .sort((a, b) => a.name.localeCompare(b.name));
  }

  private async rebalance(): Promise<void> {
    if (this.stopping) return;
    try {
      const loads = await Promise.all([...this.queues.keys()].map((queue) => this.loadOf(queue)));
      // The first round jumps straight to the targets; later ones move by maxShift.
      const current: Allocation = this.balancedOnce
        ? Object.fromEntries([...this.pools].map(([queue, pool]) => [queue, pool.target]))
        : {};
      this.balancedOnce = true;

      const next = balance(loads, current, {
        strategy: this.strategy,
        minProcesses: this.minProcesses,
        maxProcesses: this.maxProcesses,
        maxShift: this.maxShift,
      });

      const changes = Object.entries(next).flatMap(([queue, count]) => {
        const pool = this.pools.get(queue);
        return pool && pool.target !== count ? [{ pool, count, down: count < pool.target }] : [];
      });

      // Wait for scale-downs to exit before starting new processes, so the
      // total never goes over maxProcesses during a transition.
      await Promise.all(changes.filter((c) => c.down).map((c) => c.pool.scale(c.count)));
      await Promise.all(changes.filter((c) => !c.down).map((c) => c.pool.scale(c.count)));
      if (changes.length > 0) this.emit('scaled', next);
      await this.heartbeat();
    } catch (err) {
      this.reportError(err);
    }
  }

  private async loadOf(queue: string): Promise<QueueLoad> {
    const q = this.queues.get(queue) as Queue;
    const [counts, metrics] = await Promise.all([
      q.getJobCounts(),
      q.getMetrics(this.metricsWindow),
    ]);

    let finished = 0;
    let runtime = 0;
    for (const bucket of metrics) {
      const n = bucket.completed + bucket.failed;
      finished += n;
      runtime += bucket.avgRuntime * n;
    }

    return {
      name: queue,
      waiting: counts.waiting,
      active: counts.active,
      avgRuntime: finished ? runtime / finished : 0,
      concurrency: this.options.queues[queue]?.concurrency ?? 1,
    };
  }

  private async heartbeat(): Promise<void> {
    await this.client.hset(supervisorsKey(this.prefix), this.name, JSON.stringify(this.status()));
  }

  private async shutdown(): Promise<void> {
    clearInterval(this.timer);
    await this.balancing;
    await Promise.all([...this.pools.values()].map((pool) => pool.stop()));
    try {
      await this.client.hdel(supervisorsKey(this.prefix), this.name);
    } finally {
      await this.client.quit();
    }
  }

  private reportError(err: unknown): void {
    const error = err instanceof Error ? err : new Error(String(err));
    if (this.listenerCount('error') > 0) this.emit('error', error);
    else console.error(`[meridian] supervisor ${this.name}:`, error);
  }
}
