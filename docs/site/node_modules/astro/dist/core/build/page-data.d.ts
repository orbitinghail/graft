import type { AstroSettings, RoutesList } from '../../types/astro.js';
import type { Logger } from '../logger/core.js';
import type { AllPagesData } from './types.js';
export interface CollectPagesDataOptions {
    settings: AstroSettings;
    logger: Logger;
    manifest: RoutesList;
}
export interface CollectPagesDataResult {
    assets: Record<string, string>;
    allPages: AllPagesData;
}
export declare function collectPagesData(opts: CollectPagesDataOptions): CollectPagesDataResult;
