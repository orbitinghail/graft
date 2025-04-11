import { EnumChangefreq as ChangeFreq } from 'sitemap';
import { z } from 'zod';
export declare const SitemapOptionsSchema: z.ZodDefault<z.ZodObject<{
    filter: z.ZodOptional<z.ZodFunction<z.ZodTuple<[z.ZodString], z.ZodUnknown>, z.ZodBoolean>>;
    customPages: z.ZodOptional<z.ZodArray<z.ZodString, "many">>;
    canonicalURL: z.ZodOptional<z.ZodString>;
    xslURL: z.ZodOptional<z.ZodString>;
    i18n: z.ZodOptional<z.ZodEffects<z.ZodObject<{
        defaultLocale: z.ZodString;
        locales: z.ZodRecord<z.ZodString, z.ZodString>;
    }, "strip", z.ZodTypeAny, {
        defaultLocale: string;
        locales: Record<string, string>;
    }, {
        defaultLocale: string;
        locales: Record<string, string>;
    }>, {
        defaultLocale: string;
        locales: Record<string, string>;
    }, {
        defaultLocale: string;
        locales: Record<string, string>;
    }>>;
    entryLimit: z.ZodDefault<z.ZodOptional<z.ZodNumber>>;
    serialize: z.ZodOptional<z.ZodFunction<z.ZodTuple<[z.ZodAny], z.ZodUnknown>, z.ZodAny>>;
    changefreq: z.ZodOptional<z.ZodNativeEnum<typeof ChangeFreq>>;
    lastmod: z.ZodOptional<z.ZodDate>;
    priority: z.ZodOptional<z.ZodNumber>;
}, "strict", z.ZodTypeAny, {
    entryLimit: number;
    changefreq?: ChangeFreq | undefined;
    priority?: number | undefined;
    lastmod?: Date | undefined;
    i18n?: {
        defaultLocale: string;
        locales: Record<string, string>;
    } | undefined;
    filter?: ((args_0: string, ...args: unknown[]) => boolean) | undefined;
    customPages?: string[] | undefined;
    canonicalURL?: string | undefined;
    xslURL?: string | undefined;
    serialize?: ((args_0: any, ...args: unknown[]) => any) | undefined;
}, {
    changefreq?: ChangeFreq | undefined;
    priority?: number | undefined;
    lastmod?: Date | undefined;
    i18n?: {
        defaultLocale: string;
        locales: Record<string, string>;
    } | undefined;
    filter?: ((args_0: string, ...args: unknown[]) => boolean) | undefined;
    customPages?: string[] | undefined;
    canonicalURL?: string | undefined;
    xslURL?: string | undefined;
    entryLimit?: number | undefined;
    serialize?: ((args_0: any, ...args: unknown[]) => any) | undefined;
}>>;
