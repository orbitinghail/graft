import { getOutDirWithinCwd } from "../core/build/common.js";
function getPrerenderDefault(config) {
  return config.output !== "server";
}
function getOutputDirectory(settings) {
  if (settings.buildOutput === "server") {
    return settings.config.build.server;
  } else {
    return getOutDirWithinCwd(settings.config.outDir);
  }
}
export {
  getOutputDirectory,
  getPrerenderDefault
};
