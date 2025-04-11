import type { VercelKV } from "@vercel/kv";
import type { RedisConfigNodejs } from "@upstash/redis";
export interface VercelKVOptions extends Partial<RedisConfigNodejs> {
    /**
     * Optional prefix to use for all keys. Can be used for namespacing.
     */
    base?: string;
    /**
     * Optional flag to customize environment variable prefix (Default is `KV`). Set to `false` to disable env inference for `url` and `token` options
     */
    env?: false | string;
    /**
     * Default TTL for all items in seconds.
     */
    ttl?: number;
}
declare const _default: (opts: VercelKVOptions) => import("..").Driver<VercelKVOptions, VercelKV>;
export default _default;
