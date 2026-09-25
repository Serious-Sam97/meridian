import type { ServerResponse } from 'node:http';
import { Queue, queueKeys } from '@meridian/core';
import type { Redis } from 'ioredis';

export interface QueueEvent {
  id: string;
  queue: string;
  event: string;
  jobId?: string;
  [field: string]: string | undefined;
}

const DISCOVERY_INTERVAL = 5_000;
const KEEPALIVE_INTERVAL = 15_000;
const BLOCK_MS = 2_000;

/**
 * Tails the events stream of every queue with one blocking XREAD loop and
 * fans the events out to Server-Sent Events clients. The loop only runs
 * while at least one client is connected.
 */
export class EventHub {
  private readonly clients = new Set<ServerResponse>();
  /** Last delivered stream id per events key. */
  private readonly cursors = new Map<string, string>();
  private reader?: Redis;
  /** Loops still winding down after stop() are awaited by close(). */
  private readonly loops = new Set<Promise<void>>();
  private keepalive?: NodeJS.Timeout;
  private lastDiscovery = 0;

  constructor(
    private readonly client: Redis,
    private readonly prefix: string,
  ) {}

  subscribe(res: ServerResponse): void {
    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-store',
      connection: 'keep-alive',
      // Stop nginx and similar proxies from buffering the stream.
      'x-accel-buffering': 'no',
    });
    res.write('retry: 3000\n\n');
    this.clients.add(res);
    res.on('close', () => {
      this.clients.delete(res);
      if (this.clients.size === 0) this.stop();
    });
    this.start();
  }

  get clientCount(): number {
    return this.clients.size;
  }

  async close(): Promise<void> {
    for (const res of this.clients) res.end();
    this.clients.clear();
    this.stop();
    await Promise.all(this.loops);
  }

  private start(): void {
    // Keyed on the reader, not the loop: after a quick disconnect/reconnect
    // the previous loop may still be exiting, and must not block a new one.
    if (this.reader) return;
    const reader = this.client.duplicate({ maxRetriesPerRequest: null });
    this.reader = reader;
    this.keepalive = setInterval(() => this.broadcast(': keepalive\n\n'), KEEPALIVE_INTERVAL);
    const loop = this.run(reader)
      .catch((err) => console.error('[meridian] dashboard event stream stopped:', err))
      .finally(() => this.loops.delete(loop));
    this.loops.add(loop);
  }

  private stop(): void {
    clearInterval(this.keepalive);
    this.reader?.disconnect();
    this.reader = undefined;
    this.cursors.clear();
    this.lastDiscovery = 0;
  }

  private async run(reader: Redis): Promise<void> {
    while (this.reader === reader) {
      await this.discover(reader);
      if (this.cursors.size === 0) {
        await new Promise((resolve) => setTimeout(resolve, 1_000));
        continue;
      }

      const keys = [...this.cursors.keys()];
      let reply: [string, [string, string[]][]][] | null;
      try {
        reply = (await reader.xread(
          'COUNT',
          500,
          'BLOCK',
          BLOCK_MS,
          'STREAMS',
          ...keys,
          ...keys.map((key) => this.cursors.get(key) ?? '$'),
        )) as [string, [string, string[]][]][] | null;
      } catch (err) {
        if (this.reader !== reader) return; // disconnected by stop()
        throw err;
      }
      if (this.reader !== reader) return;

      for (const [key, entries] of reply ?? []) {
        const queue = this.queueOf(key);
        for (const [id, fields] of entries) {
          this.cursors.set(key, id);
          const event: QueueEvent = { id, queue, event: '' };
          for (let i = 0; i < fields.length; i += 2) event[fields[i] as string] = fields[i + 1];
          this.broadcast(`data: ${JSON.stringify(event)}\n\n`);
        }
      }
    }
  }

  /** Adds streams of queues created since the last check, starting from now. */
  private async discover(reader: Redis): Promise<void> {
    if (Date.now() - this.lastDiscovery < DISCOVERY_INTERVAL) return;
    this.lastDiscovery = Date.now();

    const names = await Queue.discover(this.client, this.prefix);
    // Cursors are shared: a loop that was stopped meanwhile must not touch them.
    if (this.reader !== reader) return;
    const missing = names
      .map((name) => queueKeys(name, this.prefix).events)
      .filter((key) => !this.cursors.has(key));
    if (missing.length === 0) return;

    // An explicit id instead of '$': '$' is re-evaluated on every XREAD and
    // would skip events added between two reads. Redis time avoids clock skew.
    const [seconds, micros] = await this.client.time();
    const now = `${Number(seconds) * 1000 + Math.floor(Number(micros) / 1000)}-0`;
    if (this.reader !== reader) return;
    for (const key of missing) this.cursors.set(key, now);
  }

  private queueOf(eventsKey: string): string {
    return /\{(.+)\}:events$/.exec(eventsKey)?.[1] ?? eventsKey;
  }

  private broadcast(message: string): void {
    for (const res of this.clients) res.write(message);
  }
}
