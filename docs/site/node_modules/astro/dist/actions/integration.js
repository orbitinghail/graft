import { ActionsWithoutServerOutputError } from "../core/errors/errors-data.js";
import { AstroError } from "../core/errors/errors.js";
import { viteID } from "../core/util.js";
import { ACTIONS_TYPES_FILE, ACTION_RPC_ROUTE_PATTERN, VIRTUAL_MODULE_ID } from "./consts.js";
function astroIntegrationActionsRouteHandler({
  settings
}) {
  return {
    name: VIRTUAL_MODULE_ID,
    hooks: {
      async "astro:config:setup"() {
        settings.injectedRoutes.push({
          pattern: ACTION_RPC_ROUTE_PATTERN,
          entrypoint: "astro/actions/runtime/route.js",
          prerender: false,
          origin: "internal"
        });
      },
      "astro:config:done": async (params) => {
        if (params.buildOutput === "static") {
          const error = new AstroError(ActionsWithoutServerOutputError);
          error.stack = void 0;
          throw error;
        }
        const stringifiedActionsImport = JSON.stringify(
          viteID(new URL("./actions", params.config.srcDir))
        );
        settings.injectedTypes.push({
          filename: ACTIONS_TYPES_FILE,
          content: `declare module "astro:actions" {
	type Actions = typeof import(${stringifiedActionsImport})["server"];

	export const actions: Actions;
}`
        });
      }
    }
  };
}
export {
  astroIntegrationActionsRouteHandler as default
};
