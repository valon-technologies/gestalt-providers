/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Adapter-owned checkout label (/prod-remote Vite env; DEV chrome only). */
  readonly VITE_GESTALT_WORKTREE_NAME?: string;
}

interface Window {
  /** Injected by index.html / gestaltd from `/brand.json` (platform product identity). */
  __GESTALT_PLATFORM_BRAND__?: {
    name?: string;
    markSrc?: string;
  } | null;
}
