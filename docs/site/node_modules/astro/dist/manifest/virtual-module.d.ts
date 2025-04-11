import type { Plugin } from 'vite';
import type { Logger } from '../core/logger/core.js';
import type { AstroSettings } from '../types/astro.js';
import type { SSRManifest } from '../types/public/index.js';
export default function virtualModulePlugin({ settings, manifest, logger: _logger, }: {
    settings: AstroSettings;
    manifest: SSRManifest;
    logger: Logger;
}): Plugin;
