// Shared sidebar configuration used by both astro.config.mjs and llmify plugin

// Types for llmify plugin to traverse the sidebar structure
export interface SidebarItem {
  label: string;
  slug?: string;
  items?: SidebarItem[];
  autogenerate?: { directory: string };
}

export interface SidebarSection {
  label: string;
  collapsed?: boolean;
  items?: SidebarItem[];
  autogenerate?: { directory: string };
}

export const sidebar = [
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
];
