import { type ChildProcess, fork } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { CHILD_CONFIG_ENV, type ChildConfig } from './child-config.js';

// Run the TypeScript entry when this module itself runs from source (tests).
const CHILD_ENTRY = new URL(
  import.meta.url.endsWith('.ts') ? './child.ts' : './child.js',
  import.meta.url,
);

export interface PoolOptions {
  child: ChildConfig;
  /** Extra Node flags for child processes, e.g. a loader for TypeScript processors. */
  execArgv?: string[];
  /** Grace period after shutdownTimeout before a child is SIGKILLed, in ms. Defaults to 5s. */
  killGrace?: number;
  /** First delay before restarting a crashed child, doubled per crash, in ms. Defaults to 1s. */
  restartDelay?: number;
  /** Upper bound for the restart delay, in ms. Defaults to 30s. */
  maxRestartDelay?: number;
}

export interface PoolEvents {
  spawn: [pid: number];
  exit: [pid: number, code: number | null, signal: NodeJS.Signals | null];
  /** A child exited without being asked to; it will be replaced after `restartIn` ms. */
  crash: [pid: number, code: number | null, signal: NodeJS.Signals | null, restartIn: number];
  /** A child crossed a recycle limit and was replaced (not a crash). */
  recycle: [pid: number, reason: string];
}

interface ManagedChild {
  process: ChildProcess;
  startedAt: number;
  stopping: boolean;
  /** Set when the child announced a planned exit after crossing a recycle limit. */
  recycling?: string;
  exited: Promise<void>;
}

/** A child that ran this long before crashing resets the restart backoff. */
const HEALTHY_UPTIME = 10_000;

/**
 * Keeps a number of worker processes running for one queue: spawns and stops
 * them to match scale(), and replaces the ones that crash, with backoff.
 */
export class ProcessPool extends EventEmitter<PoolEvents> {
  private readonly children = new Set<ManagedChild>();
  private desired = 0;
  private stopped = false;
  private consecutiveCrashes = 0;
  private restartAt = 0;
  private restartTimer?: NodeJS.Timeout;

  private readonly killGrace: number;
  private readonly restartDelay: number;
  private readonly maxRestartDelay: number;

  constructor(private readonly options: PoolOptions) {
    super();
    this.killGrace = options.killGrace ?? 5_000;
    this.restartDelay = options.restartDelay ?? 1_000;
    this.maxRestartDelay = options.maxRestartDelay ?? 30_000;
  }

  get queue(): string {
    return this.options.child.queue;
  }

  /** Processes running and not shutting down. */
  get size(): number {
    return [...this.children].filter((c) => !c.stopping).length;
  }

  get target(): number {
    return this.desired;
  }

  get pids(): number[] {
    return [...this.children].flatMap((c) => (c.stopping || !c.process.pid ? [] : [c.process.pid]));
  }

  /** Sets how many processes should run. Resolves once scale-downs have exited. */
  async scale(count: number): Promise<void> {
    if (this.stopped) return;
    this.desired = Math.max(0, Math.floor(count));
    await this.reconcile();
  }

  /** Stops every process and stops restarting them. */
  async stop(): Promise<void> {
    this.stopped = true;
    this.desired = 0;
    clearTimeout(this.restartTimer);
    await Promise.all([...this.children].map((child) => this.stopChild(child)));
  }

  private async reconcile(): Promise<void> {
    const running = [...this.children].filter((c) => !c.stopping);

    if (running.length > this.desired) {
      // Stop the newest first; long-running processes have warm caches.
      const excess = running
        .sort((a, b) => b.startedAt - a.startedAt)
        .slice(0, running.length - this.desired);
      await Promise.all(excess.map((child) => this.stopChild(child)));
      return;
    }

    const missing = this.desired - running.length;
    if (missing <= 0) return;

    const wait = this.restartAt - Date.now();
    if (wait > 0) {
      // Crash backoff in effect: try again when it ends.
      this.restartTimer ??= setTimeout(() => {
        this.restartTimer = undefined;
        void this.reconcile();
      }, wait);
      return;
    }
    for (let i = 0; i < missing; i++) this.spawn();
  }

  private spawn(): void {
    const child = fork(CHILD_ENTRY, [], {
      env: { ...process.env, [CHILD_CONFIG_ENV]: JSON.stringify(this.options.child) },
      execArgv: this.options.execArgv ?? [],
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    });

    const managed: ManagedChild = {
      process: child,
      startedAt: Date.now(),
      stopping: false,
      exited: new Promise((resolve) => child.once('exit', () => resolve())),
    };
    this.children.add(managed);
    if (child.pid) this.emit('spawn', child.pid);

    child.on('message', (message: { type?: string; reason?: string }) => {
      if (message?.type === 'recycle') managed.recycling = message.reason ?? 'recycled';
    });

    child.once('exit', (code, signal) => {
      this.children.delete(managed);
      const pid = child.pid ?? -1;
      this.emit('exit', pid, code, signal);
      if (managed.stopping || this.stopped) return;
      if (managed.recycling !== undefined && code === 0) {
        // Planned: replace it right away, without crash backoff. The
        // replacement starts only now, so maxProcesses is never exceeded.
        this.emit('recycle', pid, managed.recycling);
        void this.reconcile();
        return;
      }
      this.onCrash(managed, pid, code, signal);
    });
  }

  private onCrash(
    child: ManagedChild,
    pid: number,
    code: number | null,
    signal: NodeJS.Signals | null,
  ): void {
    if (Date.now() - child.startedAt > HEALTHY_UPTIME) this.consecutiveCrashes = 0;
    const delay = Math.min(this.restartDelay * 2 ** this.consecutiveCrashes, this.maxRestartDelay);
    this.consecutiveCrashes++;
    this.restartAt = Date.now() + delay;
    this.emit('crash', pid, code, signal, delay);
    void this.reconcile();
  }

  private async stopChild(child: ManagedChild): Promise<void> {
    if (!child.stopping) {
      child.stopping = true;
      if (child.process.connected) child.process.send({ type: 'shutdown' });
      else child.process.kill('SIGTERM');

      const killAfter = this.options.child.shutdownTimeout + this.killGrace;
      const timer = setTimeout(() => child.process.kill('SIGKILL'), killAfter);
      void child.exited.then(() => clearTimeout(timer));
    }
    await child.exited;
  }
}
