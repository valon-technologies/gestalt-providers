/** Runtime flag injected by /local-dev mock servers into index.html. */
export const LOCAL_DEV_CHROME_RUNTIME_KEY = "__GESTALT_LOCAL_DEV_CHROME__";

type DevEnv = {
  DEV?: boolean;
};

declare global {
  interface Window {
    __GESTALT_LOCAL_DEV_CHROME__?: boolean;
  }
}

/**
 * Whether adapter-owned local development chrome should render.
 *
 * True for:
 * 1. Vite DEV (/prod-remote gestaltd_child Vite, `bun run vite`, …)
 * 2. Runtime `/local-dev` mock inject (`window.__GESTALT_LOCAL_DEV_CHROME__`)
 *
 * Real production deploys set neither, so ThemeSwitcher and similar tools stay off.
 */
export function isLocalDevChrome(
  env: DevEnv = import.meta.env,
  runtimeEnabled?: boolean | null,
): boolean {
  if (env.DEV) {
    return true;
  }
  if (runtimeEnabled !== undefined && runtimeEnabled !== null) {
    return runtimeEnabled === true;
  }
  if (typeof window === "undefined") {
    return false;
  }
  return window[LOCAL_DEV_CHROME_RUNTIME_KEY] === true;
}
