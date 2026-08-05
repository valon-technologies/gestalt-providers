/// <reference types="vitest/config" />
import path from "node:path";
import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { gestalt } from "@valon-technologies/gestalt/vite";
import { defineConfig, loadEnv, type Plugin, type ProxyOptions } from "vite";
import {
  resolveWorktreeDisplayName,
  workflowsLocalMockEnabled,
  workflowsLocalMockMiddleware,
} from "./scripts/workflows-local-mock.mjs";

const projectDir = path.dirname(fileURLToPath(import.meta.url));

/** prod-dev cookie-proxy ports — disjoint from /local-dev (see prod-dev skill). */
const PROD_DEV_PROXY_PORT_MIN = 8100;
const PROD_DEV_PROXY_PORT_MAX = 8199;

function nativeApiProxy(backendOrigin: string): ProxyOptions {
  return {
    target: backendOrigin,
    changeOrigin: true,
    configure(proxy) {
      const token = process.env.GESTALT_DEV_API_PROXY_TOKEN?.trim();
      if (!token) return;

      proxy.on("proxyReq", (proxyRequest) => {
        // The native credential is available only to the supervised Vite
        // process. It must never be serialized into the browser bundle.
        proxyRequest.setHeader("Authorization", `Bearer ${token}`);
      });
    },
  };
}

function resolveGestaltPublicOrigin(
  env: Record<string, string>,
  backendOrigin: string,
): string {
  const configured = env.VITE_GESTALT_PUBLIC_ORIGIN?.trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }
  try {
    const url = new URL(backendOrigin);
    const port = Number(url.port);
    const isLoopback =
      url.hostname === "127.0.0.1" || url.hostname === "localhost";
    // Local Vite only bundles the console; prod-dev proxies API to production.
    if (
      isLoopback &&
      port >= PROD_DEV_PROXY_PORT_MIN &&
      port <= PROD_DEV_PROXY_PORT_MAX
    ) {
      const prodHost = env.GESTALT_PROD_HOST?.trim().replace(/\/+$/, "");
      return prodHost ? `https://${prodHost}` : "";
    }
  } catch {
    // ignore
  }
  return "";
}

/** Seeded Slack/workflow APIs for /local-dev Vite DEV (no gestaltd). */
function workflowsLocalMockPlugin(): Plugin {
  return {
    name: "workflows-local-mock",
    configureServer(server) {
      server.middlewares.use(workflowsLocalMockMiddleware);
    },
  };
}

export default defineConfig(({ mode }) => {
  // Keep direct Vite development same-origin. Gestaltd remains the only
  // authority that selects and serves a tenant theme and its assets.
  const env = loadEnv(mode, projectDir, "");
  const useWorkflowsMock = workflowsLocalMockEnabled({
    ...env,
    ...process.env,
  });
  const backendOrigin =
    env.GESTALT_API_PROXY_TARGET?.trim().replace(/\/+$/, "") ||
    "http://127.0.0.1:8080";
  const gestaltPublicOrigin = resolveGestaltPublicOrigin(env, backendOrigin);
  const worktreeName = resolveWorktreeDisplayName(
    process.env.VITE_GESTALT_WORKTREE_NAME || env.VITE_GESTALT_WORKTREE_NAME,
  );
  const tenantThemeLabel =
    process.env.VITE_THEME_SWITCHER_TENANT_LABEL?.trim() ||
    env.VITE_THEME_SWITCHER_TENANT_LABEL?.trim() ||
    "";

  return {
    // Production artifacts are mount-relative. The Gestalt Vite plugin
    // overrides this with GESTALT_DEV_BASE_PATH for native UI development.
    base: "./",
    plugins: [
      react(),
      tailwindcss(),
      gestalt(),
      ...(useWorkflowsMock ? [workflowsLocalMockPlugin()] : []),
    ],
    define: {
      "import.meta.env.VITE_GESTALT_PUBLIC_ORIGIN": JSON.stringify(
        gestaltPublicOrigin,
      ),
      ...(tenantThemeLabel
        ? {
            "import.meta.env.VITE_THEME_SWITCHER_TENANT_LABEL":
              JSON.stringify(tenantThemeLabel),
          }
        : {}),
      ...(useWorkflowsMock && worktreeName
        ? {
            "import.meta.env.VITE_GESTALT_WORKTREE_NAME":
              JSON.stringify(worktreeName),
          }
        : {}),
    },
    resolve: {
      alias: {
        "@": path.resolve(projectDir, "src"),
      },
    },
    server: {
      // Mock middleware owns /api + empty theme.css when enabled.
      proxy: useWorkflowsMock
        ? {}
        : {
            "/api": nativeApiProxy(backendOrigin),
            "/theme.css": nativeApiProxy(backendOrigin),
            "/theme/": nativeApiProxy(backendOrigin),
          },
    },
    test: {
      include: ["src/**/*.test.ts"],
    },
  };
});
