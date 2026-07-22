import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { gestalt } from "@valon-technologies/gestalt/vite";
import { defineConfig } from "vite";
import { gestaltDevMockApi } from "./scripts/vite-dev-mock-api.mjs";
import { serveTenantThemeInDev } from "./scripts/vite-serve-tenant-theme.mjs";

const projectDir = path.dirname(fileURLToPath(import.meta.url));

/** Valon deploy theme — Season Serif / Melange (Registry faces). */
function resolveValonThemeFile(): string | null {
  const explicit = process.env.GESTALT_THEME_FILE?.trim();
  if (explicit) {
    const resolved = path.resolve(explicit);
    return fs.existsSync(resolved) ? resolved : null;
  }
  // Default for Valon development: match Registry fonts 1:1 via tenant theme.
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

// Two-origin local/prod-dev (Vite + gestaltd/auth-proxy): browser stays
// same-origin; Vite forwards `/api` to the backend. Canonical env matches
// app-starter / local-dev stacks — never bake an API origin into client bundles.
//
// GESTALT_DEV_MOCK_AUTH=1: fulfill /api in Vite (UI boot without OAuth). Needed
// when gestaltd's login callback is a different origin (e.g. localhost:8080)
// than this SPA — Google OAuth cannot mint a cookie for the Vite port.
const mockAuth = process.env.GESTALT_DEV_MOCK_AUTH === "1";
const apiOrigin =
  process.env.GESTALT_API_PROXY_TARGET?.trim() || "http://127.0.0.1:8080";

const resolvedThemeFile =
  process.env.NODE_ENV !== "production" ? resolveValonThemeFile() : null;
const devThemeSource = resolvedThemeFile;
const devThemeMirror = path.join(projectDir, ".dev", "theme.css");
if (devThemeSource) {
  fs.mkdirSync(path.dirname(devThemeMirror), { recursive: true });
  fs.rmSync(devThemeMirror, { force: true });
  fs.copyFileSync(devThemeSource, devThemeMirror);
  // serveTenantThemeInDev reads GESTALT_THEME_FILE for /theme/* assets.
  process.env.GESTALT_THEME_FILE = devThemeSource;
}

const themeTarget = devThemeSource
  ? path.resolve(devThemeMirror)
  : path.resolve(projectDir, "src/theme.stub.css");

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
    gestalt(),
    serveTenantThemeInDev(),
    gestaltDevMockApi(),
  ],
  resolve: {
    alias: {
      "@": path.resolve(projectDir, "src"),
      "@theme.css": themeTarget,
    },
  },
  server: {
    proxy: mockAuth
      ? undefined
      : {
          "/api": {
            target: apiOrigin.replace(/\/+$/, ""),
            changeOrigin: true,
          },
        },
  },
});
