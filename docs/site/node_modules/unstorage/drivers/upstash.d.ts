import { type RedisConfigNodejs, Redis } from "@upstash/redis";
export interface UpstashOptions extends Partial<RedisConfigNodejs> {
    /**
     * Optional prefix to use for all keys. Can be used for namespacing.
     */
    base?: string;
    /**
     * Default TTL for all items in seconds.
     */
    ttl?: number;
}
declare const _default: (opts: UpstashOptions) => import("..").Driver<UpstashOptions, Redis>;
export default _default;
