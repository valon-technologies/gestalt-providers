/** Vite-visible checkout label injected by /prod-remote (adapter-owned). */
export const DEV_WORKTREE_NAME_ENV = "VITE_GESTALT_WORKTREE_NAME";

type DevEnv = {
  DEV?: boolean;
  VITE_GESTALT_WORKTREE_NAME?: string;
};

/**
 * Resolve the adapter-injected worktree label for DEV chrome.
 * Returns null outside Vite DEV or when the owned env is absent/blank.
 */
export function readDevWorktreeName(
  env: DevEnv = import.meta.env,
): string | null {
  if (!env.DEV) {
    return null;
  }
  const name = env.VITE_GESTALT_WORKTREE_NAME?.trim() ?? "";
  return name || null;
}
