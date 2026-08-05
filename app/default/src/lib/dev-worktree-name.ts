/** Vite-visible checkout label injected by /prod-remote (adapter-owned). */
export const DEV_WORKTREE_NAME_ENV = "VITE_GESTALT_WORKTREE_NAME";

/** Runtime checkout label injected by /local-dev mock servers into index.html. */
export const DEV_WORKTREE_NAME_RUNTIME_KEY = "__GESTALT_DEV_WORKTREE_NAME__";

type DevEnv = {
  DEV?: boolean;
  VITE_GESTALT_WORKTREE_NAME?: string;
};

declare global {
  interface Window {
    __GESTALT_DEV_WORKTREE_NAME__?: string;
  }
}

function trimName(value: string | null | undefined): string {
  return typeof value === "string" ? value.trim() : "";
}

function readRuntimeWorktreeName(
  runtimeName?: string | null,
): string {
  if (runtimeName !== undefined) {
    return trimName(runtimeName);
  }
  if (typeof window === "undefined") {
    return "";
  }
  return trimName(window[DEV_WORKTREE_NAME_RUNTIME_KEY]);
}

/**
 * Resolve the adapter-injected worktree label for local DEV chrome.
 *
 * Sources (first match wins):
 * 1. Vite DEV + `VITE_GESTALT_WORKTREE_NAME` (/prod-remote)
 * 2. Runtime `window.__GESTALT_DEV_WORKTREE_NAME__` (/local-dev mock HTML inject)
 *
 * Production builds never set either channel, so the banner stays off.
 */
export function readDevWorktreeName(
  env: DevEnv = import.meta.env,
  runtimeName?: string | null,
): string | null {
  const fromVite = env.DEV
    ? trimName(env.VITE_GESTALT_WORKTREE_NAME)
    : "";
  const fromRuntime = readRuntimeWorktreeName(runtimeName);
  const name = fromVite || fromRuntime;
  return name || null;
}
