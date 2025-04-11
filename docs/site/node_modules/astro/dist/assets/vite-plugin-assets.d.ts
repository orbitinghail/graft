import type * as vite from 'vite';
import type { AstroSettings } from '../types/astro.js';
export default function assets({ settings }: {
    settings: AstroSettings;
}): vite.Plugin[];
