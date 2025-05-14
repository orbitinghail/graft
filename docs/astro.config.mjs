// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightLlmsTxt from "starlight-llms-txt";
import starlightDocSearch from "@astrojs/starlight-docsearch";
import sitemap from "@astrojs/sitemap";

// https://astro.build/config
export default defineConfig({
  site: "https://graft.rs/",
  integrations: [
    starlight({
      plugins: [
        starlightLlmsTxt(),
        starlightDocSearch({
          clientOptionsModule: "./src/config/docsearch.ts",
        }),
      ],
      title: "Graft",
      pagination: false,
      logo: {
        light: "./src/assets/logo-light.svg",
        dark: "./src/assets/logo-dark.svg",
      },
      head: [
        {
          tag: "script",
          attrs: {
            src: "https://cdn.usefathom.com/script.js",
            "data-site": "MEZQWTLT",
            defer: true,
          },
        },
        {
          tag: "link",
          attrs: {
            rel: "sitemap",
            href: "/sitemap-index.xml",
          },
        },
        {
          tag: "link",
          attrs: {
            rel: "preconnect",
            href: "https://gs869rqcpn-dsn.algolia.net",
            crossorigin: true,
          },
        },
      ],
      expressiveCode: {
        themes: ["dracula", "solarized-light"],
      },
      lastUpdated: true,
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/orbitinghail/graft",
        },
        {
          icon: "discord",
          label: "Discord",
          href: "https://discord.gg/SXPbDJ863Z",
        },
      ],
      customCss: ["./src/styles/global.css"],
      editLink: {
        baseUrl: "https://github.com/orbitinghail/graft/blob/main/docs/",
      },
      sidebar: [
        {
          label: "About",
          collapsed: true,
          items: [
            { label: "Introduction", slug: "docs/about" },
            { label: "Comparison", slug: "docs/about/comparison" },
            { label: "FAQ", slug: "docs/about/faq" },
          ],
        },
        {
          label: "Concepts",
          collapsed: true,
          items: [
            { label: "Volumes", slug: "docs/concepts/volumes" },
            { label: "Consistency", slug: "docs/concepts/consistency" },
          ],
        },
        {
          label: "SQLite extension",
          collapsed: true,
          items: [
            { label: "Overview", slug: "docs/sqlite" },
            { label: "Compatibility", slug: "docs/sqlite/compatibility" },
            { label: "Databases", slug: "docs/sqlite/databases" },
            { label: "Config", slug: "docs/sqlite/config" },
            { label: "Pragmas", slug: "docs/sqlite/pragmas" },
            {
              label: "Using with...",
              autogenerate: { directory: "docs/sqlite/usage" },
            },
          ],
        },
        {
          label: "Backend",
          collapsed: true,
          items: [
            { label: "Overview", slug: "docs/backend" },
            { label: "Deploy", slug: "docs/backend/deploy" },
            { label: "Config", slug: "docs/backend/config" },
            { label: "Auth", slug: "docs/backend/auth" },
            { label: "API", slug: "docs/backend/api" },
          ],
        },
        {
          label: "Internals",
          collapsed: true,
          autogenerate: { directory: "docs/internals" },
        },
        {
          label: "RFCs",
          collapsed: true,
          autogenerate: { directory: "docs/rfcs" },
        },
      ],
    }),
    sitemap(),
  ],
});
