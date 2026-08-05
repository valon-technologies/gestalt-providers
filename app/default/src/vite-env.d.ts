/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Adapter-owned checkout label (/prod-remote Vite env; DEV chrome only). */
  readonly VITE_GESTALT_WORKTREE_NAME?: string;
}
