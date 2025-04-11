import type { AstroConfig } from '../types/public/index.js';
export declare const createEnvLoader: (mode: string, config: AstroConfig) => {
    get: () => Record<string, string>;
    getPrivateEnv: () => Record<string, string>;
};
export type EnvLoader = ReturnType<typeof createEnvLoader>;
