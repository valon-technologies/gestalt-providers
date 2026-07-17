/**
 * Same-origin DEV mock for `/api/*` when a real gestaltd auth loop cannot
 * complete back into Vite (typical: Google OAuth callback is on :8080 while
 * the SPA is on :300x). Browser traffic stays same-origin; no client API
 * origin. Disable with GESTALT_DEV_MOCK_AUTH=0 to proxy to gestaltd.
 *
 * @returns {import('vite').Plugin}
 */
export function gestaltDevMockApi() {
  const enabled = process.env.GESTALT_DEV_MOCK_AUTH === "1";

  return {
    name: "gestalt-dev-mock-api",
    configureServer(server) {
      if (!enabled) {
        return;
      }

      console.info(
        "[gestalt-dev-mock-api] serving same-origin /api fixtures (GESTALT_DEV_MOCK_AUTH=1)",
      );

      server.middlewares.use((req, res, next) => {
        const path = (req.url ?? "").split("?")[0];
        if (!path.startsWith("/api/")) {
          next();
          return;
        }

        const method = req.method ?? "GET";

        /** @param {number} status @param {unknown} body */
        const json = (status, body) => {
          res.statusCode = status;
          res.setHeader("Content-Type", "application/json; charset=utf-8");
          res.setHeader("Cache-Control", "no-store");
          res.end(JSON.stringify(body));
        };

        if (method === "GET" && path === "/api/v1/auth/session") {
          json(200, {
            subjectId: "user:dev@valon.com",
            email: "dev@valon.com",
            displayName: "Dev",
          });
          return;
        }

        if (method === "GET" && path === "/api/v1/auth/info") {
          json(200, {
            provider: "local",
            displayName: "Local Dev",
            loginSupported: false,
            features: { agent: false },
          });
          return;
        }

        if (method === "POST" && path === "/api/v1/auth/logout") {
          json(200, {});
          return;
        }

        if (
          method === "GET" &&
          (path === "/api/v1/apps" ||
            path === "/api/v1/tokens" ||
            path === "/api/v1/workflow/runs" ||
            path === "/api/v1/authorization/subjects")
        ) {
          json(200, []);
          return;
        }

        if (method === "GET" && path === "/api/v1/agent/providers") {
          json(200, { providers: [] });
          return;
        }

        if (method === "GET" && path.startsWith("/api/v1/agent/sessions")) {
          json(200, []);
          return;
        }

        if (method === "GET" && path.startsWith("/api/v1/agent/")) {
          json(412, { error: "agent not available in mock auth" });
          return;
        }

        json(404, {
          error: `gestalt-dev-mock-api: unhandled ${method} ${path}`,
        });
      });
    },
  };
}
