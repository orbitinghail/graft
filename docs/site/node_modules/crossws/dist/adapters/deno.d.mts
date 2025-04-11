import { AdapterInstance, AdapterOptions, Adapter } from '../index.mjs';
import '../shared/crossws.ChIJSJVK.mjs';

interface DenoAdapter extends AdapterInstance {
    handleUpgrade(req: Request, info: ServeHandlerInfo): Promise<Response>;
}
interface DenoOptions extends AdapterOptions {
}
type ServeHandlerInfo = {
    remoteAddr?: {
        transport: string;
        hostname: string;
        port: number;
    };
};
declare const denoAdapter: Adapter<DenoAdapter, DenoOptions>;

export { type DenoAdapter, type DenoOptions, denoAdapter as default };
