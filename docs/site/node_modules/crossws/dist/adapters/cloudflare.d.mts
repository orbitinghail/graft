import { AdapterInstance, AdapterOptions, Adapter } from '../index.mjs';
import * as CF from '@cloudflare/workers-types';
import '../shared/crossws.ChIJSJVK.mjs';

interface CloudflareAdapter extends AdapterInstance {
    handleUpgrade(req: CF.Request, env: unknown, context: CF.ExecutionContext): Promise<CF.Response>;
}
interface CloudflareOptions extends AdapterOptions {
}
declare const cloudflareAdapter: Adapter<CloudflareAdapter, CloudflareOptions>;

export { type CloudflareAdapter, type CloudflareOptions, cloudflareAdapter as default };
