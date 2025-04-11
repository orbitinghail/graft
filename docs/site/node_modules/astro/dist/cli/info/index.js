import { spawnSync } from "node:child_process";
import { arch, platform } from "node:os";
import * as colors from "kleur/colors";
import prompts from "prompts";
import { resolveConfig } from "../../core/config/index.js";
import { ASTRO_VERSION } from "../../core/constants.js";
import { apply as applyPolyfill } from "../../core/polyfill.js";
import { flagsToAstroInlineConfig } from "../flags.js";
async function getInfoOutput({
  userConfig,
  print
}) {
  const rows = [
    ["Astro", `v${ASTRO_VERSION}`],
    ["Node", process.version],
    ["System", getSystem()],
    ["Package Manager", getPackageManager()]
  ];
  try {
    rows.push(["Output", userConfig.output ?? "static"]);
    rows.push(["Adapter", userConfig.adapter?.name ?? "none"]);
    const integrations = (userConfig?.integrations ?? []).filter(Boolean).flat().map((i) => i?.name).filter(Boolean);
    rows.push(["Integrations", integrations.length > 0 ? integrations : "none"]);
  } catch {
  }
  let output = "";
  for (const [label, value] of rows) {
    output += printRow(label, value, print);
  }
  return output.trim();
}
async function printInfo({ flags }) {
  applyPolyfill();
  const { userConfig } = await resolveConfig(flagsToAstroInlineConfig(flags), "info");
  const output = await getInfoOutput({ userConfig, print: true });
  await copyToClipboard(output, flags.copy);
}
async function copyToClipboard(text, force) {
  text = text.trim();
  const system = platform();
  let command = "";
  let args = [];
  if (system === "darwin") {
    command = "pbcopy";
  } else if (system === "win32") {
    command = "clip";
  } else {
    const unixCommands = [
      ["xclip", ["-selection", "clipboard", "-l", "1"]],
      ["wl-copy", []]
    ];
    for (const [unixCommand, unixArgs] of unixCommands) {
      try {
        const output = spawnSync("which", [unixCommand], { encoding: "utf8" });
        if (output.stdout.trim()) {
          command = unixCommand;
          args = unixArgs;
          break;
        }
      } catch {
        continue;
      }
    }
  }
  if (!command) {
    console.error(colors.red("\nClipboard command not found!"));
    console.info("Please manually copy the text above.");
    return;
  }
  if (!force) {
    const { shouldCopy } = await prompts({
      type: "confirm",
      name: "shouldCopy",
      message: "Copy to clipboard?",
      initial: true
    });
    if (!shouldCopy) return;
  }
  try {
    const result = spawnSync(command, args, { input: text, stdio: ["pipe", "ignore", "ignore"] });
    if (result.error) {
      throw result.error;
    }
    console.info(colors.green("Copied to clipboard!"));
  } catch {
    console.error(
      colors.red(`
Sorry, something went wrong!`) + ` Please copy the text above manually.`
    );
  }
}
function readFromClipboard() {
  const system = platform();
  let command = "";
  let args = [];
  if (system === "darwin") {
    command = "pbpaste";
  } else if (system === "win32") {
    command = "powershell";
    args = ["-command", "Get-Clipboard"];
  } else {
    const unixCommands = [
      ["xclip", ["-sel", "clipboard", "-o"]],
      ["wl-paste", []]
    ];
    for (const [unixCommand, unixArgs] of unixCommands) {
      try {
        const output = spawnSync("which", [unixCommand], { encoding: "utf8" });
        if (output.stdout.trim()) {
          command = unixCommand;
          args = unixArgs;
          break;
        }
      } catch {
        continue;
      }
    }
  }
  if (!command) {
    throw new Error("Clipboard read command not found!");
  }
  const result = spawnSync(command, args, { encoding: "utf8" });
  if (result.error) {
    throw result.error;
  }
  return result.stdout.trim();
}
const PLATFORM_TO_OS = {
  darwin: "macOS",
  win32: "Windows",
  linux: "Linux"
};
function getSystem() {
  const system = PLATFORM_TO_OS[platform()] ?? platform();
  return `${system} (${arch()})`;
}
function getPackageManager() {
  if (!process.env.npm_config_user_agent) {
    return "unknown";
  }
  const specifier = process.env.npm_config_user_agent.split(" ")[0];
  const name = specifier.substring(0, specifier.lastIndexOf("/"));
  return name === "npminstall" ? "cnpm" : name;
}
const MAX_PADDING = 25;
function printRow(label, value, print) {
  const padding = MAX_PADDING - label.length;
  const [first, ...rest] = Array.isArray(value) ? value : [value];
  let plaintext = `${label}${" ".repeat(padding)}${first}`;
  let richtext = `${colors.bold(label)}${" ".repeat(padding)}${colors.green(first)}`;
  if (rest.length > 0) {
    for (const entry of rest) {
      plaintext += `
${" ".repeat(MAX_PADDING)}${entry}`;
      richtext += `
${" ".repeat(MAX_PADDING)}${colors.green(entry)}`;
    }
  }
  plaintext += "\n";
  if (print) {
    console.info(richtext);
  }
  return plaintext;
}
export {
  copyToClipboard,
  getInfoOutput,
  printInfo,
  readFromClipboard
};
