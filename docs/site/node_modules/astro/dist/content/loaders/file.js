import { promises as fs, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import yaml from "js-yaml";
import { posixRelative } from "../utils.js";
function file(fileName, options) {
  if (fileName.includes("*")) {
    throw new Error("Glob patterns are not supported in `file` loader. Use `glob` loader instead.");
  }
  let parse = null;
  const ext = fileName.split(".").at(-1);
  if (ext === "json") {
    parse = JSON.parse;
  } else if (ext === "yml" || ext === "yaml") {
    parse = (text) => yaml.load(text, {
      filename: fileName
    });
  }
  if (options?.parser) parse = options.parser;
  if (parse === null) {
    throw new Error(
      `No parser found for file '${fileName}'. Try passing a parser to the \`file\` loader.`
    );
  }
  async function syncData(filePath, { logger, parseData, store, config }) {
    let data;
    try {
      const contents = await fs.readFile(filePath, "utf-8");
      data = parse(contents);
    } catch (error) {
      logger.error(`Error reading data from ${fileName}`);
      logger.debug(error.message);
      return;
    }
    const normalizedFilePath = posixRelative(fileURLToPath(config.root), filePath);
    if (Array.isArray(data)) {
      if (data.length === 0) {
        logger.warn(`No items found in ${fileName}`);
      }
      logger.debug(`Found ${data.length} item array in ${fileName}`);
      store.clear();
      for (const rawItem of data) {
        const id = (rawItem.id ?? rawItem.slug)?.toString();
        if (!id) {
          logger.error(`Item in ${fileName} is missing an id or slug field.`);
          continue;
        }
        const parsedData = await parseData({ id, data: rawItem, filePath });
        store.set({ id, data: parsedData, filePath: normalizedFilePath });
      }
    } else if (typeof data === "object") {
      const entries = Object.entries(data);
      logger.debug(`Found object with ${entries.length} entries in ${fileName}`);
      store.clear();
      for (const [id, rawItem] of entries) {
        const parsedData = await parseData({ id, data: rawItem, filePath });
        store.set({ id, data: parsedData, filePath: normalizedFilePath });
      }
    } else {
      logger.error(`Invalid data in ${fileName}. Must be an array or object.`);
    }
  }
  return {
    name: "file-loader",
    load: async (context) => {
      const { config, logger, watcher } = context;
      logger.debug(`Loading data from ${fileName}`);
      const url = new URL(fileName, config.root);
      if (!existsSync(url)) {
        logger.error(`File not found: ${fileName}`);
        return;
      }
      const filePath = fileURLToPath(url);
      await syncData(filePath, context);
      watcher?.add(filePath);
      watcher?.on("change", async (changedPath) => {
        if (changedPath === filePath) {
          logger.info(`Reloading data from ${fileName}`);
          await syncData(filePath, context);
        }
      });
    }
  };
}
export {
  file
};
