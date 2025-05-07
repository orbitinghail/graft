import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import yaml from "js-yaml";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RFC_DIR = path.join(
  __dirname,
  "..",
  "src",
  "content",
  "docs",
  "docs",
  "rfcs"
);
const GH_ROOT = "https://github.com/orbitinghail/graft";

const urlOrTilde = (kind, n) =>
  n == null || n === "~" ? "~" : `${GH_ROOT}/${kind}/${n}`;

const parseFrontmatter = (txt, file) => {
  const m = txt.match(/^---\n([\s\S]+?)\n---/);
  if (!m) {
    throw new Error(`Missing front-matter in ${file}`);
  }
  return yaml.load(m[1]);
};

const entries = await fs.readdir(RFC_DIR, { withFileTypes: true });

const rfcs = await Promise.all(
  entries.map(async (d) => {
    if (!d.isFile() || !d.name.endsWith(".mdx")) return null;

    // RFCs are named like "1234 - My RFC Title.mdx"
    const match = d.name.match(/^\d{4}\s*-/);
    if (!match) return null;

    const raw = await fs.readFile(path.join(RFC_DIR, d.name), "utf8");
    const fm = parseFrontmatter(raw, d.name).rfc ?? {};

    return {
      id: fm.id.toString(),
      slug: fm.slug ?? "",
      startDate: fm.startDate ? new Date(fm.startDate).toISOString().split("T")[0] : "",
      issue: urlOrTilde("issues", fm.issue),
      pr: urlOrTilde("pull", fm.pr),
    };
  })
);

const rows = rfcs.filter(Boolean).sort((a, b) => a.id.localeCompare(b.id));
const transformed = rows.reduce((acc, {id, ...x}) => { acc[id] = x; return acc}, {});

console.table(transformed);
