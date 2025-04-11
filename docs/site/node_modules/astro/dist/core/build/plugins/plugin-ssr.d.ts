import type { BuildInternals } from '../internal.js';
import type { AstroBuildPlugin } from '../plugin.js';
import type { StaticBuildOptions } from '../types.js';
export declare const SSR_VIRTUAL_MODULE_ID = "@astrojs-ssr-virtual-entry";
export declare const RESOLVED_SSR_VIRTUAL_MODULE_ID: string;
export declare function pluginSSR(options: StaticBuildOptions, internals: BuildInternals): AstroBuildPlugin;
