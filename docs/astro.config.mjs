// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightLlmsTxt from "starlight-llms-txt";

import starlightLinksValidator from "starlight-links-validator";

const plugins = [starlightLlmsTxt()];

if (process.env.CHECK_LINKS) {
  plugins.push(starlightLinksValidator());
}

// https://astro.build/config
export default defineConfig({
  site: "https://graft.rs/",
  integrations: [
    starlight({
      plugins,
      title: "Graft",
      pagination: false,
      logo: {
        light: "./src/assets/tight-light.svg",
        dark: "./src/assets/tight-dark.svg",
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
          items: [
            { label: "Introduction", slug: "about/intro" },
            { label: "FAQ", slug: "about/faq" },
            { label: "Architecture", slug: "about/architecture" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { label: "Volumes", slug: "concepts/volumes" },
            { label: "Consistency", slug: "concepts/consistency" },
          ],
        },
        {
          label: "SQLite extension",
          items: [
            { label: "Overview", slug: "sqlite/overview" },
            { label: "Compatibility", slug: "sqlite/compatibility" },
            { label: "Databases", slug: "sqlite/databases" },
            { label: "Config", slug: "sqlite/config" },
            { label: "Pragmas", slug: "sqlite/pragmas" },
            {
              label: "Using with...",
              autogenerate: { directory: "sqlite/usage" },
            },
          ],
        },
        {
          label: "Server",
          items: [
            { label: "Overview", slug: "server/overview" },
            { label: "Deploy", slug: "server/deploy" },
            { label: "Config", slug: "server/config" },
            { label: "Auth", slug: "server/auth" },
            { label: "API", slug: "server/api" },
          ],
        },
      ],
    }),
  ],
});
