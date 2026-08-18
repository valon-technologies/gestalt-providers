import { test as base, expect, type Locator, type Page, type Route } from "@playwright/test";
import type {
  APIToken,
  AppAdminIdentity,
  AppAdminRegistryResponse,
  AppAdminRegistryVersionResponse,
  AppAdminRegistryHistoryResponse,
  Integration,
  IntegrationOperation,
  WorkflowDefinition,
  WorkflowRun,
} from "../src/lib/api";
import {
  workflowDefinitionMatchesApp,
  workflowRunMatchesApp,
} from "../src/lib/workflowActivity";
import {
  apiTokenToIdentityGrantWire,
  parseIdentityGrantIdFromUrl,
  PERSONAL_IDENTITY_GRANTS_PATH,
} from "../src/lib/personalGrants";

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
  integrations: Integration[] | (() => Integration[]),
  opts?: { onDisconnect?: (name: string, url: URL) => void },
) {
  const current = () =>
    typeof integrations === "function" ? integrations() : integrations;
  await page.route("**/api/v1/catalog/apps/*/icon", async (route, request) => {
    if (request.method() !== "GET") {
      await route.fallback();
      return;
    }
    const url = new URL(request.url());
    const parts = url.pathname.split("/");
    const name = parts[parts.length - 2] || "";
    const integration = current().find((item) => item.name === name);
    await route.fulfill({
      status: integration?.iconSvg ? 200 : 404,
      contentType: "image/svg+xml; charset=utf-8",
      body: integration?.iconSvg ?? "Not Found",
    });
  });
  await page.route("**/api/v1/catalog/apps", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: catalogEntriesFromMock(current()) });
    } else {
      route.fallback();
    }
  });
  await page.route("**/api/v1/me/app-connections", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: connectionOverlayFromMock(current()) });
    } else {
      route.fallback();
    }
  });
  await page.route("**/api/v1/apps", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: current() });
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

/** Replace directory mocks so a 503 can recover to `integrations`. */
export async function mockAppsDirectoryUnavailable(
  page: Page,
  recover: () => Integration[] | null,
) {
  await page.unroute("**/api/v1/catalog/apps");
  await page.unroute("**/api/v1/me/app-connections");
  await page.unroute("**/api/v1/apps");

  const fulfillOrFail = (route: Route, body: unknown) => {
    const integrations = recover();
    if (!integrations) {
      route.fulfill({
        status: 503,
        contentType: "application/json",
        body: JSON.stringify({ error: "Service Unavailable" }),
      });
      return;
    }
    route.fulfill({ json: body });
  };

  await page.route("**/api/v1/catalog/apps", (route: Route, request) => {
    if (request.method() !== "GET") {
      route.fallback();
      return;
    }
    const integrations = recover();
    fulfillOrFail(
      route,
      integrations ? catalogEntriesFromMock(integrations) : null,
    );
  });
  await page.route("**/api/v1/me/app-connections", (route: Route, request) => {
    if (request.method() !== "GET") {
      route.fallback();
      return;
    }
    const integrations = recover();
    fulfillOrFail(
      route,
      integrations ? connectionOverlayFromMock(integrations) : null,
    );
  });
  await page.route("**/api/v1/apps", (route: Route, request) => {
    if (request.method() !== "GET") {
      route.fallback();
      return;
    }
    const integrations = recover();
    fulfillOrFail(route, integrations ?? null);
  });
}

/** Catalog stays up; only the per-user connection overlay fails. */
export async function mockAppConnectionsUnavailable(
  page: Page,
  recover: () => Integration[] | null,
) {
  await page.unroute("**/api/v1/me/app-connections");
  await page.route("**/api/v1/me/app-connections", (route: Route, request) => {
    if (request.method() !== "GET") {
      route.fallback();
      return;
    }
    const integrations = recover();
    if (!integrations) {
      route.fulfill({
        status: 503,
        contentType: "application/json",
        body: JSON.stringify({ error: "Service Unavailable" }),
      });
      return;
    }
    route.fulfill({ json: connectionOverlayFromMock(integrations) });
  });
}

function catalogEntriesFromMock(integrations: Integration[]) {
  return integrations.map((integration) => ({
    name: integration.name,
    displayName: integration.displayName,
    description: integration.description,
    iconUrl: integration.iconSvg
      ? `/api/v1/catalog/apps/${encodeURIComponent(integration.name)}/icon`
      : integration.iconUrl,
    mountedPath: integration.mountedPath,
    managementPath: integration.managementPath,
    prompts: integration.prompts,
    connections: (integration.connections ?? []).map((connection) => ({
      name: connection.name,
      displayName: connection.displayName,
      authTypes: connection.authTypes,
      connectionParams: connection.connectionParams,
      credentialFields: connection.credentialFields,
      mode: connection.mode,
      mcpPassthrough: connection.mcpPassthrough,
    })),
  }));
}

function connectionOverlayFromMock(integrations: Integration[]) {
  return integrations.map((integration) => ({
    name: integration.name,
    status: integration.status,
    credentialState: integration.credentialState,
    healthState: integration.healthState,
    actions: integration.actions,
    connected:
      typeof integration.connected === "boolean"
        ? integration.connected
        : (integration.connections ?? []).some(
            (connection) => connection.connected === true,
          ),
    connections: (integration.connections ?? []).map((connection) => ({
      name: connection.name,
      status: connection.status,
      credentialState: connection.credentialState,
      healthState: connection.healthState,
      actions: connection.actions,
      credentialMode: connection.credentialMode,
      ownerKind: connection.ownerKind,
      instances: connection.instances,
      preferredInstance: connection.preferredInstance,
      connected: connection.connected === true,
      mcpPassthrough: connection.mcpPassthrough,
    })),
  }));
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

export async function mockAppAuthorizationMembers(
  page: Page,
  app: string,
  members: unknown[] = [],
) {
  await page.route(
    `**/api/v1/apps/${encodeURIComponent(app)}/admin/members`,
    (route: Route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: members });
      } else {
        route.fallback();
      }
    },
  );
}

export async function mockAppAdminIdentities(
  page: Page,
  app: string,
  identities: AppAdminIdentity[],
) {
  await page.route(
    `**/api/v1/apps/${encodeURIComponent(app)}/admin/identities`,
    (route: Route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: identities });
      } else {
        route.fallback();
      }
    },
  );
}

export async function mockAuthInfo(
  page: Page,
  info: {
    provider: string;
    displayName: string;
    loginSupported?: boolean;
    features?: {
      agent?: boolean;
      workflowDefaultProvider?: string;
    };
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
  let state: AppAdminRegistryResponse = {
    ...initialState,
    autoDeploy: initialState.autoDeploy ?? { enabled: false },
  };

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
    `**/api/v1/apps/${app}/admin/registry/auto-deploy`,
    async (route: Route, request) => {
      if (request.method() !== "PUT") {
        await route.fallback();
        return;
      }
      const body = JSON.parse(request.postData() || "{}") as { enabled?: boolean };
      if (typeof body.enabled !== "boolean") {
        await route.fulfill({ status: 400, json: { error: "enabled is required" } });
        return;
      }
      state = {
        ...state,
        autoDeploy: {
          ...state.autoDeploy,
          enabled: body.enabled,
          lastError: body.enabled ? undefined : state.autoDeploy?.lastError,
          pendingVersion: body.enabled ? state.autoDeploy?.pendingVersion : undefined,
        },
      };
      await route.fulfill({
        json: {
          app: state.app,
          autoDeploy: state.autoDeploy,
        },
      });
    },
  );

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

  await page.route(
    `**/api/v1/apps/${app}/admin/registry/history**`,
    (route: Route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: { app, revisions: [] } });
        return;
      }
      route.fallback();
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

export interface MockTokensController {
  setTokens(tokens: APIToken[]): void;
  getTokens(): APIToken[];
}

export async function mockTokens(
  page: Page,
  initialTokens: APIToken[],
  options?: {
    delayFirstListMs?: number;
  },
): Promise<MockTokensController> {
  let currentTokens = [...initialTokens];
  let listCount = 0;

  await page.route(`**${PERSONAL_IDENTITY_GRANTS_PATH}`, async (route: Route, request) => {
    if (request.method() === "GET") {
      listCount += 1;
      if (options?.delayFirstListMs && listCount === 1) {
        await new Promise((resolve) => setTimeout(resolve, options.delayFirstListMs));
        await route.fulfill({ json: { grantIds: [] } });
        return;
      }
      await route.fulfill({
        json: { grantIds: currentTokens.map((token) => token.id) },
      });
      return;
    }
    await route.fallback();
  });

  await page.route(`**${PERSONAL_IDENTITY_GRANTS_PATH}/*`, async (route: Route, request) => {
    const grantId = parseIdentityGrantIdFromUrl(request.url());
    if (request.method() === "GET") {
      const token = currentTokens.find((entry) => entry.id === grantId);
      if (!token) {
        await route.fulfill({ status: 404, json: { error: "grant not found" } });
        return;
      }
      await route.fulfill({ json: apiTokenToIdentityGrantWire(token) });
      return;
    }
    if (request.method() === "DELETE") {
      currentTokens = currentTokens.filter((entry) => entry.id !== grantId);
      await route.fulfill({ json: {} });
      return;
    }
    await route.fallback();
  });

  return {
    setTokens(tokens: APIToken[]) {
      currentTokens = [...tokens];
    },
    getTokens() {
      return [...currentTokens];
    },
  };
}

type CreatePersonalTokenBody = {
  name?: string;
  scopes?: string;
  expiresIn?: number;
};

export async function mockPersonalTokenCreate(
  page: Page,
  tokens: MockTokensController,
  handler: (
    body: CreatePersonalTokenBody,
  ) => Promise<{
    token: APIToken;
    plaintext: string;
    expiresAt?: string;
    status?: number;
  }>,
  options?: { listCreated?: boolean },
) {
  await page.route("**/api/v1/tokens", async (route: Route, request) => {
    if (request.method() === "POST") {
      const body = request.postDataJSON() as CreatePersonalTokenBody;
      const result = await handler(body);
      if (options?.listCreated !== false) {
        const rest = tokens
          .getTokens()
          .filter((token) => token.id !== result.token.id);
        tokens.setTokens([result.token, ...rest]);
      }
      await route.fulfill({
        status: result.status ?? 201,
        json: {
          id: result.token.id,
          token: result.plaintext,
          scopes: result.token.scopes,
          expiresAt: result.expiresAt,
        },
      });
      return;
    }
    await route.fallback();
  });
}

export async function mockWorkflowRuns(
  page: Page,
  runs: WorkflowRun[],
  opts?: MockWorkflowRunsOptions,
): Promise<MockWorkflowRunsController> {
  let currentRuns = runs.map((run) => structuredClone(run));

  await page.route(/\/api\/v2\/workflow\/runs(?:\?.*)?$/, (route: Route, request) => {
    if (request.method() === "GET") {
      const url = new URL(request.url());
      const appName = url.searchParams.get("targetApp")?.trim();
      const definitionId =
        url.searchParams.get("definitionId")?.trim() ||
        url.searchParams.get("definition_id")?.trim();
      let runs = appName
        ? currentRuns.filter((run) => workflowRunMatchesApp(run, appName))
        : currentRuns;
      if (definitionId) {
        runs = runs.filter(
          (run) => (run.definitionId?.trim() || "") === definitionId,
        );
      }
      route.fulfill({
        json: { runs, nextPageToken: "", totalCount: runs.length },
      });
    } else {
      route.fallback();
    }
  });

  await page.route("**/api/v2/workflow/runs/**", (route: Route, request) => {
    const url = new URL(request.url());
    const pathname = url.pathname;
    const cancelMatch = pathname.match(/\/api\/v2\/workflow\/runs\/([^/]+):cancel$/);
    const detailMatch = pathname.match(/\/api\/v2\/workflow\/runs\/([^/]+)$/);
    const id = cancelMatch?.[1] ?? detailMatch?.[1];

    if (request.method() === "POST" && cancelMatch) {
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

export async function mockWorkflowDefinitions(
  page: Page,
  definitions: WorkflowDefinition[],
): Promise<void> {
  let current = definitions.map((item) => structuredClone(item));

  await page.route(
    /\/api\/v2\/workflow\/definitions(?:\?.*)?$/,
    (route: Route, request) => {
      if (request.method() !== "GET") {
        route.fallback();
        return;
      }
      const url = new URL(request.url());
      const appName = url.searchParams.get("targetApp")?.trim();
      // Client filters by app; mock returns all and lets the UI filter, or
      // optionally filter here when targetApp is present (parity with runs).
      const items = appName
        ? current.filter((item) => workflowDefinitionMatchesApp(item, appName))
        : current;
      route.fulfill({ json: { definitions: items } });
    },
  );

  await page.route("**/api/v2/workflow/definitions/**", (route: Route, request) => {
    if (request.method() !== "GET") {
      route.fallback();
      return;
    }
    const id = new URL(request.url()).pathname.split("/").pop() || "";
    const definition = current.find((item) => item.id === id);
    if (!definition) {
      route.fulfill({ status: 404, json: { error: "not found" } });
      return;
    }
    route.fulfill({ json: definition });
  });
}

type CustomFixtures = {
  authenticatedPage: Page;
};

/**
 * Catalog and chrome tests represent a user already in the workspace.
 * Net-new auto-entry is opt-in via {@link enableSetupActivationPrompt}.
 * Keep in sync with SETUP_SKIPPED_STORAGE_KEY and
 * SETUP_RESUME_BANNER_DISMISSED_KEY in src/lib/buildPaths.ts.
 */
const SETUP_SKIPPED_STORAGE_KEY = "gestalt.setup.skipped";
const SETUP_RESUME_BANNER_DISMISSED_KEY = "gestalt.setup.resumeBannerDismissed";
const SETUP_E2E_FORCE_ACTIVATION_KEY = "gestalt.e2e.forceActivation";

export async function enableSetupActivationPrompt(page: Page) {
  await page.addInitScript(
    ({ skipKey, bannerKey, forceKey }) => {
      window.sessionStorage.setItem(forceKey, "1");
      if (!window.sessionStorage.getItem(`${forceKey}:armed`)) {
        window.sessionStorage.setItem(`${forceKey}:armed`, "1");
        window.localStorage.removeItem(skipKey);
        window.localStorage.removeItem(bannerKey);
      }
    },
    {
      skipKey: SETUP_SKIPPED_STORAGE_KEY,
      bannerKey: SETUP_RESUME_BANNER_DISMISSED_KEY,
      forceKey: SETUP_E2E_FORCE_ACTIVATION_KEY,
    },
  );
}

const SETUP_SESSION_API_TOKEN = "gst_api_test_token_for_install";

export type SetupSessionSeed = {
  introSeen?: boolean;
  installAgent?: string;
  selectedTokenId?: string;
  apiToken?: string;
  apiTokenGrantId?: string;
  /**
   * Bind plaintext to the grant id. Defaults to true when a grant id is set,
   * so resume fixtures satisfy the token-step credential contract.
   * `selectedTokenId` is an alias for `apiTokenGrantId`.
   */
  bindCredential?: boolean;
  mcpInstalled?: boolean;
  activeExemplarId?: string;
  trySeen?: boolean;
  tokenName?: string;
};

/** OAuth opens a popup so Setup and the catalog stay on this page. */
export async function clickOpensOAuthPopup(locator: Locator): Promise<Page> {
  const popupPromise = locator.page().waitForEvent("popup");
  await locator.click();
  return popupPromise;
}

/** Seed Setup sessionStorage so later steps can resume past earlier gates. */
export async function seedSetupSession(page: Page, seed: SetupSessionSeed) {
  await page.addInitScript(
    (s: SetupSessionSeed & { defaultToken: string; appliedKey: string }) => {
      if (sessionStorage.getItem(s.appliedKey) === "1") return;
      sessionStorage.setItem(s.appliedKey, "1");
      if (s.introSeen) {
        sessionStorage.setItem("gestalt.build.introSeen", "1");
      }
      if (s.installAgent) {
        sessionStorage.setItem("gestalt.build.installAgent.v2", s.installAgent);
      }
      const grantId = s.apiTokenGrantId ?? s.selectedTokenId;
      const bind = s.bindCredential ?? Boolean(grantId);
      if (bind && grantId) {
        sessionStorage.setItem("gestalt.build.apiTokenGrantId", grantId);
        sessionStorage.setItem(
          "gestalt.build.apiToken",
          s.apiToken ?? s.defaultToken,
        );
      } else {
        if (s.apiTokenGrantId) {
          sessionStorage.setItem(
            "gestalt.build.apiTokenGrantId",
            s.apiTokenGrantId,
          );
        }
        if (s.apiToken) {
          sessionStorage.setItem("gestalt.build.apiToken", s.apiToken);
        }
      }
      if (s.mcpInstalled) {
        const agents = s.installAgent ? [s.installAgent] : [];
        sessionStorage.setItem(
          "gestalt.build.mcpInstalledAgents",
          JSON.stringify(agents),
        );
      }
      if (s.activeExemplarId) {
        sessionStorage.setItem(
          "gestalt.build.activeExemplarId",
          s.activeExemplarId,
        );
      }
      if (s.trySeen) {
        sessionStorage.setItem("gestalt.build.trySeen", "1");
      }
      if (s.tokenName) {
        sessionStorage.setItem("gestalt.build.tokenName", s.tokenName);
      }
    },
    {
      ...seed,
      defaultToken: SETUP_SESSION_API_TOKEN,
      appliedKey: "gestalt.e2e.setupSessionApplied",
    },
  );
}

export const test = base.extend<CustomFixtures>({
  page: async ({ page }, use) => {
    await page.addInitScript(
      ({ skipKey, bannerKey, forceKey }) => {
        if (window.sessionStorage.getItem(forceKey) === "1") {
          return;
        }
        window.localStorage.setItem(skipKey, "1");
        window.localStorage.setItem(bannerKey, "1");
      },
      {
        skipKey: SETUP_SKIPPED_STORAGE_KEY,
        bannerKey: SETUP_RESUME_BANNER_DISMISSED_KEY,
        forceKey: SETUP_E2E_FORCE_ACTIVATION_KEY,
      },
    );
    await use(page);
  },
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
