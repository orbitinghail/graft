import type { AstroIntegration } from "astro";
import fg from "fast-glob";
import * as fs from "fs/promises";
import yaml from "js-yaml";
import * as path from "path";
import {
  sidebar,
  type SidebarItem,
  type SidebarSection,
} from "../config/sidebar";

const GRAFT_DESCRIPTION = `# Graft Documentation

Graft is an open-source transactional storage engine designed for efficient data
synchronization at the edge. It supports lazy, partial replication with strong
consistency, ensuring applications replicate only the data they need.

## Core Benefits

- **Lazy Replication**: Clients sync data on demand, saving network and compute.
- **Partial Replication**: Minimize bandwidth by syncing only required data.
- **Edge Optimization**: Lightweight client designed for edge, mobile, and embedded environments.
- **Strong Consistency**: Serializable Snapshot Isolation ensures correct, consistent data views.
- **Transactional Object Storage**: Graft turns object storage into a transactional system.
- **Instant Read Replicas**: Decoupled metadata and data allow replicas to spin up immediately.

## Documentation Sitemap

`;

interface PageInfo {
  slug: string;
  label: string;
  description?: string;
}

interface Frontmatter {
  title?: string;
  description?: string;
}

/** Parse YAML frontmatter from markdown content */
function parseFrontmatter(content: string): Frontmatter {
  const match = content.match(/^---\n([\s\S]*?)\n---/);
  if (!match) return {};
  try {
    return (yaml.load(match[1]) as Frontmatter) || {};
  } catch {
    return {};
  }
}

/** Read a markdown file and extract its frontmatter */
async function readFrontmatter(filePath: string): Promise<Frontmatter> {
  try {
    const content = await fs.readFile(filePath, "utf-8");
    return parseFrontmatter(content);
  } catch {
    return {};
  }
}

/** Find a markdown file for a given slug, trying common patterns */
async function findMarkdownFile(
  sourceDir: string,
  slug: string,
): Promise<string | null> {
  const patterns = [
    `${slug}.{md,mdx}`,
    `${slug}/index.{md,mdx}`,
  ];
  const files = await fg(patterns, { cwd: sourceDir, absolute: true });
  return files[0] || null;
}

/** Get all markdown files in a directory (non-recursive) */
async function getMarkdownFiles(dirPath: string): Promise<string[]> {
  const files = await fg("*.{md,mdx}", { cwd: dirPath, absolute: true });
  return files.sort();
}

/** Collect page info by traversing the sidebar configuration */
async function collectPages(sourceDir: string): Promise<PageInfo[]> {
  const pages: PageInfo[] = [];

  // Add the homepage
  const homePath = await findMarkdownFile(sourceDir, "index");
  if (homePath) {
    const fm = await readFrontmatter(homePath);
    pages.push({
      slug: "",
      label: fm.title || "Home",
      description: fm.description,
    });
  }

  async function processAutogenerate(
    directory: string,
  ): Promise<void> {
    const dirPath = path.join(sourceDir, directory);
    for (const filePath of await getMarkdownFiles(dirPath)) {
      const fm = await readFrontmatter(filePath);
      const fileName = path.basename(filePath).replace(/\.(md|mdx)$/, "");
      const slug = fileName === "index" ? directory : `${directory}/${fileName}`;
      pages.push({
        slug,
        label: fm.title || fileName,
        description: fm.description,
      });
    }
  }

  async function processItem(item: SidebarItem): Promise<void> {
    if (item.slug) {
      const filePath = await findMarkdownFile(sourceDir, item.slug);
      if (filePath) {
        const fm = await readFrontmatter(filePath);
        pages.push({
          slug: item.slug,
          label: item.label,
          description: fm.description,
        });
      }
    }

    if (item.autogenerate) {
      await processAutogenerate(item.autogenerate.directory);
    }

    if (item.items) {
      for (const subItem of item.items) {
        await processItem(subItem);
      }
    }
  }

  async function processSection(section: SidebarSection): Promise<void> {
    if (section.items) {
      for (const item of section.items) {
        await processItem(item);
      }
    }

    if (section.autogenerate) {
      await processAutogenerate(section.autogenerate.directory);
    }
  }

  for (const section of sidebar) {
    await processSection(section);
  }

  return pages;
}

/** Generate the llms.txt index content */
function generateLlmsTxt(baseUrl: string, pages: PageInfo[]): string {
  const lines = pages.map((page) => {
    const url = page.slug ? `${baseUrl}${page.slug}.md` : `${baseUrl}index.md`;
    const description = page.description ? `: ${page.description}` : "";
    return `- [${page.label}](${url})${description}`;
  });
  return GRAFT_DESCRIPTION + lines.join("\n") + "\n";
}

/** Copy all markdown files from source to dest, converting .mdx to .md */
async function copyMarkdownFiles(
  sourceDir: string,
  destDir: string,
): Promise<number> {
  const files = await fg("**/*.{md,mdx}", { cwd: sourceDir });

  for (const file of files) {
    const srcPath = path.join(sourceDir, file);
    const destPath = path.join(destDir, file.replace(/\.mdx$/, ".md"));
    await fs.mkdir(path.dirname(destPath), { recursive: true });
    await fs.copyFile(srcPath, destPath);
  }

  return files.length;
}

export default function llmifyPlugin(): AstroIntegration {
  let siteUrl: string;

  return {
    name: "llmify",
    hooks: {
      "astro:config:done": ({ config }) => {
        siteUrl = config.site?.toString() ?? "";
        if (!siteUrl.endsWith("/")) {
          siteUrl += "/";
        }
      },
      "astro:build:done": async ({ dir, logger }) => {
        const sourceDir = path.resolve("./src/content/docs");
        const destDir = dir.pathname;

        // Copy markdown files to dist
        const fileCount = await copyMarkdownFiles(sourceDir, destDir);
        logger.info(`Copied ${fileCount} markdown files to dist`);

        // Generate llms.txt index
        const pages = await collectPages(sourceDir);
        await fs.writeFile(
          path.join(destDir, "llms.txt"),
          generateLlmsTxt(siteUrl, pages),
        );
        logger.info(`Generated llms.txt with ${pages.length} pages`);
      },
    },
  };
}
