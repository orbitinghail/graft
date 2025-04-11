import Redis, { Cluster, type ClusterNode, type ClusterOptions, type RedisOptions as _RedisOptions } from "ioredis";
export interface RedisOptions extends _RedisOptions {
    /**
     * Optional prefix to use for all keys. Can be used for namespacing.
     */
    base?: string;
    /**
     * Url to use for connecting to redis. Takes precedence over `host` option. Has the format `redis://<REDIS_USER>:<REDIS_PASSWORD>@<REDIS_HOST>:<REDIS_PORT>`
     */
    url?: string;
    /**
     * List of redis nodes to use for cluster mode. Takes precedence over `url` and `host` options.
     */
    cluster?: ClusterNode[];
    /**
     * Options to use for cluster mode.
     */
    clusterOptions?: ClusterOptions;
    /**
     * Default TTL for all items in seconds.
     */
    ttl?: number;
}
declare const _default: (opts: RedisOptions) => import("..").Driver<RedisOptions, Redis | Cluster>;
export default _default;
