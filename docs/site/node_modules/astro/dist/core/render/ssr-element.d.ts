import type { SSRElement } from '../../types/public/internal.js';
import type { AssetsPrefix, StylesheetAsset } from '../app/types.js';
export declare function createAssetLink(href: string, base?: string, assetsPrefix?: AssetsPrefix): string;
export declare function createStylesheetElement(stylesheet: StylesheetAsset, base?: string, assetsPrefix?: AssetsPrefix): SSRElement;
export declare function createStylesheetElementSet(stylesheets: StylesheetAsset[], base?: string, assetsPrefix?: AssetsPrefix): Set<SSRElement>;
export declare function createModuleScriptElement(script: {
    type: 'inline' | 'external';
    value: string;
}, base?: string, assetsPrefix?: AssetsPrefix): SSRElement;
export declare function createModuleScriptElementWithSrc(src: string, base?: string, assetsPrefix?: AssetsPrefix): SSRElement;
