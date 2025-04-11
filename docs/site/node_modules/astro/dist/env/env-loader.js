import { fileURLToPath } from "node:url";
import { loadEnv } from "vite";
const isValidIdentifierRe = /^[_$a-zA-Z][\w$]*$/;
function getPrivateEnv(fullEnv, astroConfig) {
  const viteConfig = astroConfig.vite;
  let envPrefixes = ["PUBLIC_"];
  if (viteConfig.envPrefix) {
    envPrefixes = Array.isArray(viteConfig.envPrefix) ? viteConfig.envPrefix : [viteConfig.envPrefix];
  }
  const privateEnv = {};
  for (const key in fullEnv) {
    if (isValidIdentifierRe.test(key) && envPrefixes.every((prefix) => !key.startsWith(prefix))) {
      if (typeof process.env[key] !== "undefined") {
        let value = process.env[key];
        if (typeof value !== "string") {
          value = `${value}`;
        }
        if (value === "0" || value === "1" || value === "true" || value === "false") {
          privateEnv[key] = value;
        } else {
          privateEnv[key] = `process.env.${key}`;
        }
      } else {
        privateEnv[key] = JSON.stringify(fullEnv[key]);
      }
    }
  }
  return privateEnv;
}
const createEnvLoader = (mode, config) => {
  const loaded = loadEnv(mode, config.vite.envDir ?? fileURLToPath(config.root), "");
  const privateEnv = getPrivateEnv(loaded, config);
  return {
    get: () => loaded,
    getPrivateEnv: () => privateEnv
  };
};
export {
  createEnvLoader
};
