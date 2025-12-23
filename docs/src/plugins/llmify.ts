import type { AstroIntegration } from "astro";
import * as fs from "fs/promises";
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

async function extractFrontmatter(
  filePath: string,
): Promise<{ title?: string; description?: string }> {
  try {
    const content = await fs.readFile(filePath, "utf-8");
    const match = content.match(/^---\n([\s\S]*?)\n---/);
    if (!match) return {};

    const frontmatter = match[1];
    const title = frontmatter.match(/^title:\s*(.+)$/m)?.[1]?.trim();
    const description = frontmatter
      .match(/^description:\s*(.+)$/m)?.[1]
      ?.trim();

    return { title, description };
  } catch {
    return {};
  }
}

async function findMarkdownFile(
  sourceDir: string,
  slug: string,
): Promise<string | null> {
  const basePath = path.join(sourceDir, slug);

  // Try different file patterns
  const candidates = [
    `${basePath}.md`,
    `${basePath}.mdx`,
    `${basePath}/index.md`,
    `${basePath}/index.mdx`,
    path.join(basePath, "index.md"),
    path.join(basePath, "index.mdx"),
  ];

  for (const candidate of candidates) {
    try {
      await fs.access(candidate);
      return candidate;
    } catch {
      // File doesn't exist, try next
    }
  }
  return null;
}

async function getFilesInDirectory(dirPath: string): Promise<string[]> {
  try {
    const entries = await fs.readdir(dirPath, { withFileTypes: true });
    const files: string[] = [];

    for (const entry of entries) {
      if (
        entry.isFile() &&
        (entry.name.endsWith(".md") || entry.name.endsWith(".mdx"))
      ) {
        files.push(path.join(dirPath, entry.name));
      }
    }

    return files.sort();
  } catch {
    return [];
  }
}

async function collectPages(sourceDir: string): Promise<PageInfo[]> {
  const pages: PageInfo[] = [];

  // Add the homepage
  const homePath = await findMarkdownFile(sourceDir, "index");
  if (homePath) {
    const fm = await extractFrontmatter(homePath);
    pages.push({
      slug: "",
      label: fm.title || "Home",
      description: fm.description,
    });
  }

  async function processItem(item: SidebarItem): Promise<void> {
    if (item.slug) {
      const filePath = await findMarkdownFile(sourceDir, item.slug);
      if (filePath) {
        const fm = await extractFrontmatter(filePath);
        pages.push({
          slug: item.slug,
          label: item.label,
          description: fm.description,
        });
      }
    }

    if (item.autogenerate) {
      const dirPath = path.join(sourceDir, item.autogenerate.directory);
      const files = await getFilesInDirectory(dirPath);

      for (const filePath of files) {
        const fm = await extractFrontmatter(filePath);
        const fileName = path.basename(filePath).replace(/\.(md|mdx)$/, "");
        const slug =
          fileName === "index"
            ? item.autogenerate.directory
            : `${item.autogenerate.directory}/${fileName}`;

        pages.push({
          slug,
          label: fm.title || fileName,
          description: fm.description,
        });
      }
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
      const dirPath = path.join(sourceDir, section.autogenerate.directory);
      const files = await getFilesInDirectory(dirPath);

      for (const filePath of files) {
        const fm = await extractFrontmatter(filePath);
        const fileName = path.basename(filePath).replace(/\.(md|mdx)$/, "");
        const slug =
          fileName === "index"
            ? section.autogenerate.directory
            : `${section.autogenerate.directory}/${fileName}`;

        pages.push({
          slug,
          label: fm.title || fileName,
          description: fm.description,
        });
      }
    }
  }

  for (const section of sidebar) {
    await processSection(section);
  }

  return pages;
}

function generateLlmsTxt(baseUrl: string, pages: PageInfo[]): string {
  let content = GRAFT_DESCRIPTION;

  for (const page of pages) {
    const url = page.slug ? `${baseUrl}${page.slug}.md` : `${baseUrl}index.md`;
    const description = page.description ? `: ${page.description}` : "";
    content += `- [${page.label}](${url})${description}\n`;
  }

  return content;
}

export default function llmifyPlugin(): AstroIntegration {
  return {
    name: "llmify",
    hooks: {
      "astro:build:done": async ({ dir, logger }) => {
        const sourceDir = path.resolve("./src/content/docs");
        const destDir = dir.pathname;
        const baseUrl = "https://graft.rs/";

        // Copy markdown files
        const copyFiles = async (src: string, dest: string) => {
          const entries = await fs.readdir(src, { withFileTypes: true });

          for (const entry of entries) {
            const srcPath = path.join(src, entry.name);

            if (entry.isDirectory()) {
              const destPath = path.join(dest, entry.name);
              await fs.mkdir(destPath, { recursive: true });
              await copyFiles(srcPath, destPath);
            } else if (entry.name.endsWith(".mdx")) {
              const destPath = path.join(
                dest,
                entry.name.replace(/\.mdx$/, ".md"),
              );
              await fs.copyFile(srcPath, destPath);
            } else if (entry.name.endsWith(".md")) {
              const destPath = path.join(dest, entry.name);
              await fs.copyFile(srcPath, destPath);
            }
          }
        };

        await copyFiles(sourceDir, destDir);
        logger.info("Copied markdown files to dist");

        // Generate llms.txt
        const pages = await collectPages(sourceDir);
        const llmsTxt = generateLlmsTxt(baseUrl, pages);
        await fs.writeFile(path.join(destDir, "llms.txt"), llmsTxt);
        logger.info(`Generated llms.txt with ${pages.length} pages`);
      },
    },
  };
}
