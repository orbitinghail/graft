import { defineDriver } from "./utils/index.mjs";
import localstorage from "./localstorage.mjs";
const DRIVER_NAME = "session-storage";
export default defineDriver((opts = {}) => {
  return {
    name: DRIVER_NAME,
    ...localstorage({
      windowKey: "sessionStorage",
      ...opts
    })
  };
});
