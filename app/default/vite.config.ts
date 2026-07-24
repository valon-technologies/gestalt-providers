import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { gestalt } from "@valon-technologies/gestalt/vite";
import { defineConfig } from "vite";
import { serveTenantThemeInDev } from "./scripts/vite-serve-tenant-theme.mjs";

const projectDir = path.dirname(fileURLToPath(import.meta.url));

/** Valon deploy theme — Season Serif / Melange (Registry faces). */
function resolveValonThemeFile(): string | null {
  const explicit = process.env.GESTALT_THEME_FILE?.trim();
  if (explicit) {
    const resolved = path.resolve(explicit);
    return fs.existsSync(resolved) ? resolved : null;
  }
  const candidates = [
    path.resolve(
      process.env.HOME || "",
      "Work/toolshed/valon-tools/deploy/ui/theme.css",
    ),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

const resolvedThemeFile =
  process.env.NODE_ENV !== "production" ? resolveValonThemeFile() : null;
const devThemeSource = resolvedThemeFile;
const devThemeMirror = path.join(projectDir, ".dev", "theme.css");
if (devThemeSource) {
  fs.mkdirSync(path.dirname(devThemeMirror), { recursive: true });
  fs.rmSync(devThemeMirror, { force: true });
  fs.copyFileSync(devThemeSource, devThemeMirror);
  process.env.GESTALT_THEME_FILE = devThemeSource;
}

const themeTarget = devThemeSource
  ? path.resolve(devThemeMirror)
  : path.resolve(projectDir, "src/theme.stub.css");

// Local/prod-dev: browser stays same-origin; Vite forwards `/api` to the stack
// cookie-proxy (GESTALT_API_PROXY_TARGET). Never bake an API origin into bundles.
const apiOrigin =
  process.env.GESTALT_API_PROXY_TARGET?.trim() || "http://127.0.0.1:8080";

export default defineConfig({
  plugins: [react(), tailwindcss(), gestalt(), serveTenantThemeInDev()],
  resolve: {
    alias: {
      "@": path.resolve(projectDir, "src"),
      "@theme.css": themeTarget,
    },
  },
  server: {
    proxy: {
      "/api": {
        target: apiOrigin.replace(/\/+$/, ""),
        changeOrigin: true,
      },
    },
  },
});
