import { test as base, expect, type Page, type Route } from "@playwright/test";
import type {
  APIToken,
  Integration,
  IntegrationOperation,
  ManagedIdentity,
} from "../src/lib/api";
import type { WorkflowRun } from "../src/lib/workflow";

type MockWorkflowRunsOptions = {
  onCancel?: (
    run: WorkflowRun,
    body: { reason?: string } | null,
  ) => { status: number; json: unknown } | undefined;
};

type MockWorkflowRunsController = {
  setRuns: (runs: WorkflowRun[]) => void;
  getRuns: () => WorkflowRun[];
};

export async function mockIntegrations(
  page: Page,
  integrations: Integration[],
  opts?: { onDisconnect?: (name: string, url: URL) => void },
) {
  await page.route("**/api/v1/apps", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: integrations });
    } else {
      route.fallback();
    }
  });

  await page.route("**/api/v1/apps/*", (route: Route, request) => {
    if (request.method() === "DELETE") {
      const url = new URL(request.url());
      const name = url.pathname.split("/").pop() || "";
      opts?.onDisconnect?.(name, url);
      route.fulfill({ json: { status: "disconnected" } });
    } else {
      route.fallback();
    }
  });
}

export async function mockIntegrationOperations(
  page: Page,
  operationsByIntegrationName: Record<string, IntegrationOperation[]>,
) {
  await page.route("**/api/v1/apps/*/operations", async (route: Route, request) => {
    if (request.method() !== "GET") {
      await route.fallback();
      return;
    }

    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const integration = parts[parts.length - 2] || "";
    await route.fulfill({ json: operationsByIntegrationName[integration] || [] });
  });
}

export async function mockManualConnect(
  page: Page,
  opts?: { onConnect?: (integration: string, credential: string) => void },
) {
  await page.route(
    "**/api/v1/auth/connect-manual",
    async (route: Route, request) => {
      if (request.method() === "POST") {
        const body = request.postDataJSON() as {
          integration: string;
          credential: string;
          returnPath?: string;
        };
        opts?.onConnect?.(body.integration, body.credential);
        await route.fulfill({ json: { status: "connected" } });
      } else {
        await route.fallback();
      }
    },
  );
}

export async function mockManagedIdentities(
  page: Page,
  identities: ManagedIdentity[],
) {
  await page.route("**/api/v1/authorization/subjects", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: identities });
    } else {
      route.fallback();
    }
  });
}

export async function mockAuthInfo(
  page: Page,
  info: {
    provider: string;
    displayName: string;
    loginSupported?: boolean;
  },
) {
  await page.route("**/api/v1/auth/info", (route: Route) => {
    route.fulfill({ json: { loginSupported: true, ...info } });
  });
}

export async function mockAuthSession(
  page: Page,
  session: {
    subjectId?: string;
    email?: string;
    displayName?: string;
  } = {},
): Promise<void> {
  await page.route("**/api/v1/auth/session", (route) => {
    route.fulfill({
      json: {
        subjectId: "user:test@gestalt.dev",
        email: "test@gestalt.dev",
        ...session,
      },
    });
  });
}

export async function mockAuthSessionUnauthorized(page: Page): Promise<void> {
  await page.route("**/api/v1/auth/session", (route) => {
    route.fulfill({ status: 401, json: { error: "missing authorization" } });
  });
}

function isIdentityGrantsCollection(url: URL): boolean {
  return /\/api\/v2\/identity\/grants\/?$/.test(url.pathname);
}

function identityGrantIdFromUrl(url: URL): string | null {
  const match = url.pathname.match(/\/api\/v2\/identity\/grants\/([^/]+)\/?$/);
  return match ? decodeURIComponent(match[1]!) : null;
}

function identityGrantDetailJson(token: APIToken): {
  scopes: { scope: string; resource: string[] }[];
  createdAt: string;
  expiresAt: string;
  name?: string;
} {
  const createdMs = Date.parse(token.createdAt);
  const expiresMs = token.expiresAt ? Date.parse(token.expiresAt) : NaN;
  return {
    scopes: (token.scopes ?? []).map((scope) => ({ scope, resource: [] })),
    createdAt: Number.isFinite(createdMs)
      ? String(Math.floor(createdMs / 1000))
      : "0",
    expiresAt: Number.isFinite(expiresMs)
      ? String(Math.floor(expiresMs / 1000))
      : "0",
    ...(token.name?.trim() ? { name: token.name.trim() } : {}),
  };
}

/**
 * Mock Identity v2 personal-token grants (list + get + optional revoke).
 * Create remains `POST /api/v1/tokens` and is not mocked here.
 */
export async function mockTokens(
  page: Page,
  tokens: APIToken[] | (() => APIToken[]),
  opts?: {
    onRevoke?: (id: string) => void;
  },
) {
  const readTokens = () => (typeof tokens === "function" ? tokens() : tokens);

  await page.route(
    (url) => url.pathname.includes("/api/v2/identity/grants"),
    (route: Route, request) => {
    const url = new URL(request.url());
    if (isIdentityGrantsCollection(url)) {
      if (request.method() === "GET") {
        route.fulfill({
          json: { grantIds: readTokens().map((token) => token.id) },
        });
        return;
      }
      route.fallback();
      return;
    }

    const id = identityGrantIdFromUrl(url);
    if (!id) {
      route.fallback();
      return;
    }

    if (request.method() === "GET") {
      const token = readTokens().find((item) => item.id === id);
      if (!token) {
        route.fulfill({ status: 404, json: { error: "grant not found" } });
        return;
      }
      route.fulfill({ json: identityGrantDetailJson(token) });
      return;
    }

    if (request.method() === "DELETE") {
      opts?.onRevoke?.(id);
      route.fulfill({ json: {} });
      return;
    }

    route.fallback();
  },
  );
}

export async function mockWorkflowRuns(
  page: Page,
  runs: WorkflowRun[],
  opts?: MockWorkflowRunsOptions,
): Promise<MockWorkflowRunsController> {
  let currentRuns = runs.map((run) => structuredClone(run));

  await page.route("**/api/v1/workflow/runs**", (route: Route, request) => {
    const url = new URL(request.url());
    // Detail and cancel routes include an id path segment after /runs/.
    if (/\/api\/v1\/workflow\/runs\/.+/.test(url.pathname)) {
      route.fallback();
      return;
    }
    if (request.method() === "GET") {
      const targetApp =
        url.searchParams.get("app")?.trim() ||
        url.searchParams.get("targetApp")?.trim();
      const runs = targetApp
        ? currentRuns.filter((run) => {
            const names = run.target.steps
              .map((step) => step.app?.name)
              .filter((name): name is string => !!name);
            const definitionId = run.definitionId || "";
            return (
              names.includes(targetApp) ||
              definitionId === `app_${targetApp}` ||
              definitionId.startsWith(`app_${targetApp}_`)
            );
          })
        : currentRuns;
      route.fulfill({ json: { runs, nextPageToken: "" } });
    } else {
      route.fallback();
    }
  });

  await page.route("**/api/v1/workflow/runs/**", (route: Route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const id =
      parts[parts.length - 2] === "runs"
        ? parts[parts.length - 1]
        : parts[parts.length - 2];

    if (request.method() === "POST" && parts[parts.length - 1] === "cancel") {
      const run = currentRuns.find((item) => item.id === id);
      if (!run) {
        route.fulfill({ status: 404, json: { error: "not found" } });
        return;
      }
      const body =
        (request.postDataJSON() as { reason?: string } | null) ?? null;
      const override = opts?.onCancel?.(structuredClone(run), body);
      if (override) {
        route.fulfill({ status: override.status, json: override.json });
        return;
      }
      if (run.status !== "pending") {
        route.fulfill({
          status: 412,
          json: {
            error: "workflow run cannot be canceled once it has started",
          },
        });
        return;
      }
      run.status = "canceled";
      run.completedAt = new Date().toISOString();
      if (body?.reason) {
        run.statusMessage = body.reason;
      }
      route.fulfill({ json: run });
      return;
    }

    if (request.method() !== "GET") {
      route.fallback();
      return;
    }
    const run = currentRuns.find((item) => item.id === id);
    if (!run) {
      route.fulfill({ status: 404, json: { error: "not found" } });
      return;
    }
    route.fulfill({ json: run });
  });

  return {
    setRuns(nextRuns) {
      currentRuns = nextRuns.map((run) => structuredClone(run));
    },
    getRuns() {
      return currentRuns.map((run) => structuredClone(run));
    },
  };
}

type CustomFixtures = {
  authenticatedPage: Page;
};

export const test = base.extend<CustomFixtures>({
  authenticatedPage: async ({ page }, runAuthenticatedPage) => {
    await page.addInitScript(() => {
      localStorage.setItem(
        "gestalt.auth.session",
        JSON.stringify({
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
        }),
      );
    });
    await mockAuthSession(page);
    await runAuthenticatedPage(page);
  },
});

export { expect };
