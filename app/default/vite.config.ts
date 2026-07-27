import path from "node:path";
import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { gestalt } from "@valon-technologies/gestalt/vite";
import { defineConfig, loadEnv } from "vite";

const projectDir = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig(({ mode }) => {
  // Keep direct Vite development same-origin. Gestaltd remains the only
  // authority that selects and serves a tenant theme and its assets.
  const env = loadEnv(mode, projectDir, "");
  const backendOrigin =
    env.GESTALT_API_PROXY_TARGET?.trim().replace(/\/+$/, "") ||
    "http://127.0.0.1:8080";

  return {
    // Production artifacts are mount-relative. The Gestalt Vite plugin
    // overrides this with GESTALT_DEV_BASE_PATH for native UI development.
    base: "./",
    plugins: [react(), tailwindcss(), gestalt()],
    resolve: {
      alias: {
        "@": path.resolve(projectDir, "src"),
      },
    },
    server: {
      proxy: {
        "/api": { target: backendOrigin, changeOrigin: true },
        "/theme.css": { target: backendOrigin, changeOrigin: true },
        "/theme/": { target: backendOrigin, changeOrigin: true },
      },
    },
  };
});
