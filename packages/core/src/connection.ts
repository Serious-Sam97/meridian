import { Cluster, type ClusterNode, type ClusterOptions, Redis, type RedisOptions } from 'ioredis';

/** A single Redis server or a Redis Cluster. */
export type RedisClient = Redis | Cluster;

/**
 * Cluster settings as a plain object. Unlike a Cluster instance it can be
 * serialized, so supervisor worker processes can connect with it too.
 */
export interface ClusterConnection {
  cluster: ClusterNode[];
  clusterOptions?: ClusterOptions;
}

export type ConnectionOptions = RedisClient | ClusterConnection | RedisOptions | string;

export interface OwnedConnection {
  client: RedisClient;
  /** False when the caller passed their own client; we must not close it then. */
  owned: boolean;
}

export function isClusterConnection(options: unknown): options is ClusterConnection {
  return (
    typeof options === 'object' &&
    options !== null &&
    Array.isArray((options as ClusterConnection).cluster)
  );
}

export function createConnection(options: ConnectionOptions | undefined): OwnedConnection {
  if (options instanceof Redis || options instanceof Cluster) {
    return { client: options, owned: false };
  }
  // Commands must wait for a reconnect instead of failing a job mid-flight.
  if (typeof options === 'string') {
    return { client: new Redis(options, { maxRetriesPerRequest: null }), owned: true };
  }
  if (isClusterConnection(options)) {
    const { clusterOptions } = options;
    const client = new Cluster(options.cluster, {
      ...clusterOptions,
      redisOptions: { ...clusterOptions?.redisOptions, maxRetriesPerRequest: null },
    });
    return { client, owned: true };
  }
  return { client: new Redis({ ...options, maxRetriesPerRequest: null }), owned: true };
}

/**
 * Blocking commands (BLPOP) hold a connection until they return, so workers
 * need a dedicated connection that is never shared with regular commands.
 */
export function duplicateConnection(client: RedisClient): RedisClient {
  // A Cluster duplicate keeps the original's options, retries included.
  if (client instanceof Cluster) return client.duplicate();
  return client.duplicate({ maxRetriesPerRequest: null });
}

/**
 * The servers to run node-local commands such as SCAN on: every master of a
 * cluster, or the single server.
 */
export async function masterNodes(client: RedisClient): Promise<Redis[]> {
  if (!(client instanceof Cluster)) return [client];
  if (client.status !== 'ready') {
    await new Promise<void>((resolve, reject) => {
      client.once('ready', resolve);
      client.once('error', reject);
    });
  }
  return client.nodes('master');
}
