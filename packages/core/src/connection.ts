import { Redis, type RedisOptions } from 'ioredis';

export type ConnectionOptions = Redis | RedisOptions | string;

export interface OwnedConnection {
  client: Redis;
  /** False when the caller passed their own client; we must not close it then. */
  owned: boolean;
}

export function createConnection(options: ConnectionOptions | undefined): OwnedConnection {
  if (options instanceof Redis) {
    return { client: options, owned: false };
  }
  if (typeof options === 'string') {
    return { client: new Redis(options, { maxRetriesPerRequest: null }), owned: true };
  }
  // Commands must wait for a reconnect instead of failing a job mid-flight.
  return { client: new Redis({ ...options, maxRetriesPerRequest: null }), owned: true };
}

/**
 * Blocking commands (BLPOP) hold a connection until they return, so workers
 * need a dedicated connection that is never shared with regular commands.
 */
export function duplicateConnection(client: Redis): Redis {
  return client.duplicate({ maxRetriesPerRequest: null });
}
