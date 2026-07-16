import { test as base, expect, type Page, type Route } from "@playwright/test";
import type {
  APIToken,
  Integration,
  IntegrationOperation,
  ManagedIdentity,
} from "../src/lib/api";

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

export async function mockTokens(page: Page, tokens: APIToken[]) {
  await page.route("**/api/v1/tokens", (route: Route, request) => {
    if (request.method() === "GET") {
      route.fulfill({ json: tokens });
    } else {
      route.fallback();
    }
  });
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
