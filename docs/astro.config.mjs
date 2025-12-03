// @ts-check
import { execSync } from "node:child_process";
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightLlmsTxt from "starlight-llms-txt";
import starlightDocSearch from "@astrojs/starlight-docsearch";
import sitemap from "@astrojs/sitemap";

// find the current branch name
export function currentBranch() {
  const branch =
    process.env.GITHUB_HEAD_REF || // PR source branch in GitHub Actions
    process.env.GITHUB_REF_NAME || // push/tag ref in GitHub Actions
    process.env.CF_PAGES_BRANCH; // branch name in CloudFlare pages build

  if (branch) {
    return branch;
  }

  // fallback to checking git
  return execSync("git rev-parse --abbrev-ref HEAD", {
    encoding: "utf8",
  }).trim();
}

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
          href: "https://discord.gg/dhyjne5XK9",
        },
      ],
      customCss: ["./src/styles/global.css"],
      editLink: {
        baseUrl: `https://github.com/orbitinghail/graft/blob/${currentBranch()}/docs/`,
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
          label: "Internals",
          collapsed: true,
          autogenerate: { directory: "docs/internals" },
        },
      ],
    }),
    sitemap(),
  ],
});
