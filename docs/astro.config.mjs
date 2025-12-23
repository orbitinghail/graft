// @ts-check
import { execSync } from "node:child_process";
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightDocSearch from "@astrojs/starlight-docsearch";
import sitemap from "@astrojs/sitemap";
import llmifyPlugin from "./src/plugins/llmify";
import { sidebar } from "./src/config/sidebar";

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
      sidebar,
    }),
    sitemap(),
    llmifyPlugin(),
  ],
});
