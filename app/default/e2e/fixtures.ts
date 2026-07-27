import { test as base, expect, type Page, type Route } from "@playwright/test";
import type {
  APIToken,
  AppAdminRegistryResponse,
  AppAdminRegistryVersionResponse,
  AppAdminRegistryHistoryResponse,
  Integration,
  IntegrationOperation,
  ManagedIdentity,
  WorkflowRun,
} from "../src/lib/api";

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

type MockAppAdminRegistryOptions = {
  onSelectVersion?: (
    version: string,
    state: AppAdminRegistryResponse,
  ) =>
    | AppAdminRegistryVersionResponse
    | AppAdminRegistryResponse
    | { status: number; json: unknown; nextState?: AppAdminRegistryResponse };
};

export async function mockAppAdminRegistry(
  page: Page,
  app: string,
  initialState: AppAdminRegistryResponse,
  opts?: MockAppAdminRegistryOptions,
): Promise<{ getState: () => AppAdminRegistryResponse; setState: (state: AppAdminRegistryResponse) => void }> {
  let state = initialState;

  await page.route(`**/api/v1/apps/${app}/admin/registry`, (route: Route, request) => {
    const pathname = new URL(request.url()).pathname;
    if (pathname.endsWith("/history")) {
      route.fallback();
      return;
    }
    if (request.method() === "GET") {
      route.fulfill({ json: state });
      return;
    }
    route.fallback();
  });

  await page.route(
    `**/api/v1/apps/${app}/admin/registry/version`,
    async (route: Route, request) => {
      if (request.method() !== "POST") {
        await route.fallback();
        return;
      }

      const body = JSON.parse(request.postData() || "{}") as { version?: string };
      const version = body.version || "";
      if (opts?.onSelectVersion) {
        const result = opts.onSelectVersion(version, state);
        if ("status" in result) {
          if (result.nextState) {
            state = result.nextState;
          }
          await route.fulfill({ status: result.status, json: result.json });
          return;
        }
        if ("publishedVersions" in result) {
          state = result;
          await route.fulfill({
            json: {
              app: state.app,
              registry: state.registry,
              desiredVersion: version,
              rollout: state.rollout,
            } satisfies AppAdminRegistryVersionResponse,
          });
          return;
        }
        state = {
          ...state,
          desiredVersion: result.desiredVersion,
          rollout: result.rollout,
          selectionDisabled: true,
          disabledReason: "rollout in progress",
        };
        await route.fulfill({ json: result });
        return;
      }

      state = {
        ...state,
        desiredVersion: version,
        rollout: {
          version,
          state: "enrolling",
        },
        selectionDisabled: true,
        disabledReason: "rollout in progress",
      };
      await route.fulfill({
        json: {
          app: state.app,
          registry: state.registry,
          desiredVersion: version,
          rollout: state.rollout,
        } satisfies AppAdminRegistryVersionResponse,
      });
    },
  );

  return {
    getState: () => state,
    setState: (nextState) => {
      state = nextState;
    },
  };
}

export async function mockAppAdminRegistryHistory(
  page: Page,
  app: string,
  initialState: AppAdminRegistryHistoryResponse,
  opts?: {
    onRequest?: (cursor: string | null) => AppAdminRegistryHistoryResponse;
  },
): Promise<{ getState: () => AppAdminRegistryHistoryResponse }> {
  let state = initialState;

  await page.route(`**/api/v1/apps/${app}/admin/registry/history**`, (route: Route, request) => {
    if (request.method() === "GET") {
      const url = new URL(request.url());
      const cursor = url.searchParams.get("cursor");
      if (opts?.onRequest) {
        route.fulfill({ json: opts.onRequest(cursor) });
        return;
      }
      route.fulfill({ json: state });
      return;
    }
    route.fallback();
  });

  return {
    getState: () => state,
  };
}

export async function mockTokens(page: Page, tokens: APIToken[]) {
  await page.route("**/api/v1/tokens", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: tokens });
    } else {
      route.fallback();
    }
  });
}

export async function mockWorkflowRuns(
  page: Page,
  runs: WorkflowRun[],
  opts?: MockWorkflowRunsOptions,
): Promise<MockWorkflowRunsController> {
  let currentRuns = runs.map((run) => structuredClone(run));

  await page.route("**/api/v1/workflow/runs", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: { runs: currentRuns, nextPageToken: "" } });
    } else {
      route.fallback();
    }
  });

  await page.route("**/api/v1/workflow/runs/**", (route: Route, request) => {
    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const id = parts[parts.length - 2] === "runs"
      ? parts[parts.length - 1]
      : parts[parts.length - 2];

    if (request.method() === "POST" && parts[parts.length - 1] === "cancel") {
      const run = currentRuns.find((item) => item.id === id);
      if (!run) {
        route.fulfill({ status: 404, json: { error: "not found" } });
        return;
      }
      const body = (request.postDataJSON() as { reason?: string } | null) ?? null;
      const override = opts?.onCancel?.(structuredClone(run), body);
      if (override) {
        route.fulfill({ status: override.status, json: override.json });
        return;
      }
      if (run.status !== "pending") {
        route.fulfill({
          status: 412,
          json: { error: "workflow run cannot be canceled once it has started" },
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
