import { markdownConfigDefaults } from '@astrojs/markdown-remark';
import type { AstroIntegration, ContainerRenderer } from 'astro';
import type { Options as RemarkRehypeOptions } from 'remark-rehype';
import type { PluggableList } from 'unified';
import type { OptimizeOptions } from './rehype-optimize-static.js';
export type MdxOptions = Omit<typeof markdownConfigDefaults, 'remarkPlugins' | 'rehypePlugins'> & {
    extendMarkdownConfig: boolean;
    recmaPlugins: PluggableList;
    remarkPlugins: PluggableList;
    rehypePlugins: PluggableList;
    remarkRehype: RemarkRehypeOptions;
    optimize: boolean | OptimizeOptions;
};
export declare function getContainerRenderer(): ContainerRenderer;
export default function mdx(partialMdxOptions?: Partial<MdxOptions>): AstroIntegration;
