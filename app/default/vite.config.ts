import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { gestalt } from "@valon-technologies/gestalt-web/vite";
import { defineConfig } from "vite";
import { gestaltDevMockApi } from "./scripts/vite-dev-mock-api.mjs";
import { serveTenantThemeInDev } from "./scripts/vite-serve-tenant-theme.mjs";

const projectDir = path.dirname(fileURLToPath(import.meta.url));

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

const devThemeSource =
  process.env.NODE_ENV !== "production" && process.env.GESTALT_THEME_FILE
    ? path.resolve(process.env.GESTALT_THEME_FILE)
    : null;
const devThemeMirror = path.join(projectDir, ".dev", "theme.css");
if (devThemeSource) {
  fs.mkdirSync(path.dirname(devThemeMirror), { recursive: true });
  fs.rmSync(devThemeMirror, { force: true });
  fs.copyFileSync(devThemeSource, devThemeMirror);
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
      "@connectrpc/connect-node": path.resolve(
        projectDir,
        "src/stubs/connect-node-stub.ts",
      ),
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
