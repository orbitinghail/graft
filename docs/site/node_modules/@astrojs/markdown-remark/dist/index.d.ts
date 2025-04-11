import type { AstroMarkdownOptions, AstroMarkdownProcessorOptions, MarkdownProcessor, SyntaxHighlightConfig } from './types.js';
export { rehypeHeadingIds } from './rehype-collect-headings.js';
export { remarkCollectImages } from './remark-collect-images.js';
export { rehypePrism } from './rehype-prism.js';
export { rehypeShiki } from './rehype-shiki.js';
export { isFrontmatterValid, extractFrontmatter, parseFrontmatter, type ParseFrontmatterOptions, type ParseFrontmatterResult, } from './frontmatter.js';
export { createShikiHighlighter, type ShikiHighlighter, type CreateShikiHighlighterOptions, type ShikiHighlighterHighlightOptions, } from './shiki.js';
export * from './types.js';
export declare const syntaxHighlightDefaults: Required<SyntaxHighlightConfig>;
export declare const markdownConfigDefaults: Required<AstroMarkdownOptions>;
/**
 * Create a markdown preprocessor to render multiple markdown files
 */
export declare function createMarkdownProcessor(opts?: AstroMarkdownProcessorOptions): Promise<MarkdownProcessor>;
