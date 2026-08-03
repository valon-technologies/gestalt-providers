/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Adapter-owned checkout label injected by /prod-remote (DEV chrome only). */
  readonly VITE_GESTALT_WORKTREE_NAME?: string;
}
