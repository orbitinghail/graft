import type { PluginContext } from 'rollup';
import { z } from 'zod';
export declare function createImage(pluginContext: PluginContext, shouldEmitFile: boolean, entryFilePath: string, experimentalSvgEnabled: boolean): () => z.ZodEffects<z.ZodString, z.ZodNever | {
    ASTRO_ASSET: string;
    width: number;
    height: number;
    src: string;
    format: import("../assets/types.js").ImageInputFormat;
    fsPath: string;
    orientation?: number | undefined;
}, string>;
