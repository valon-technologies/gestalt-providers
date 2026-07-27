import {
  test,
  expect,
  mockTokens,
  mockIntegrations,
} from "./fixtures";
import type { APIToken } from "../src/lib/api";

const sampleTokens: APIToken[] = [
  {
    id: "tok-1",
    scopes: ["my-app"],
    createdAt: "2026-01-15T10:00:00Z",
  },
  {
    id: "tok-2",
    scopes: ["other-app:read"],
    createdAt: "2026-02-20T14:30:00Z",
    expiresAt: "2027-02-20T14:30:00Z",
  },
];

test.describe("Token Management", () => {
  test("displays token list by grant ID", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockTokens(page, sampleTokens);
    await mockIntegrations(page, []);

    await page.goto("/authorization");
    await expect(
      page.getByRole("heading", { name: "Authorization" }),
    ).toBeVisible();
    await expect(page.getByText("tok-1")).toBeVisible();
    await expect(page.getByText("tok-2")).toBeVisible();
    await expect(page.getByText("my-app")).toBeVisible();
    await expect(page.getByText("other-app:read")).toBeVisible();
  });

  test("shows empty state when no tokens", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockTokens(page, []);
    await mockIntegrations(page, []);

    await page.goto("/authorization");
    await expect(page.getByText("No API tokens yet.")).toBeVisible();
  });

  test("creates a scoped token and shows plaintext once", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let tokens: APIToken[] = [];
    await page.route("**/api/v2/identity/grants", async (route, request) => {
      if (request.method() === "GET") {
        await route.fulfill({
          json: { grantIds: tokens.map((token) => token.id) },
        });
        return;
      }
      await route.continue();
    });
    await page.route("**/api/v2/identity/grants/*", async (route, request) => {
      const grantId = decodeURIComponent(
        request.url().split("/api/v2/identity/grants/")[1]?.split("?")[0] ?? "",
      );
      if (request.method() === "GET") {
        const token = tokens.find((entry) => entry.id === grantId);
        if (!token) {
          await route.fulfill({ status: 404, json: { error: "grant not found" } });
          return;
        }
        await route.fulfill({
          json: {
            scopes: (token.scopes ?? []).map((scope) => ({ scope, resource: [] })),
            createdAt: Math.floor(new Date(token.createdAt).getTime() / 1000),
            expiresAt: token.expiresAt
              ? Math.floor(new Date(token.expiresAt).getTime() / 1000)
              : 0,
          },
        });
        return;
      }
      await route.continue();
    });
    await page.route("**/api/v1/tokens", async (route, request) => {
      if (request.method() === "POST") {
        const body = request.postDataJSON() as { name?: string; scopes?: string; expiresIn?: number };
        expect(body).toEqual({ name: "audit-label", scopes: "my-app", expiresIn: 30 * 24 * 60 * 60 });
        tokens = [
          {
            id: "tok-new",
            scopes: body.scopes ? [body.scopes] : [],
            createdAt: "2026-03-01T12:00:00Z",
          },
        ];
        await route.fulfill({
          status: 201,
          json: {
            id: "tok-new",
            token: "gestalt_abc123secret",
            scopes: ["my-app"],
            expiresAt: "2027-03-01T12:00:00Z",
          },
        });
        return;
      }
      await route.continue();
    });
    await mockIntegrations(page, []);

    await page.goto("/authorization");
    await page.getByLabel("Token name").fill("audit-label");
    await page.getByLabel("Scopes").fill("my-app");
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(page.getByText("Copy this token now")).toBeVisible();
    await expect(page.getByText("gestalt_abc123secret")).toBeVisible();
    await expect(page.locator("tr", { hasText: "tok-new" })).toBeVisible();
    await expect(page.getByText("my-app")).toBeVisible();
  });

  test("keeps the created token visible when stale list requests finish later", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let tokens: APIToken[] = [];
    let listCount = 0;

    await page.route("**/api/v2/identity/grants", async (route, request) => {
      if (request.method() === "GET") {
        listCount += 1;
        if (listCount === 1) {
          await new Promise((resolve) => setTimeout(resolve, 250));
          await route.fulfill({ json: { grantIds: [] } });
          return;
        }
        await route.fulfill({
          json: { grantIds: tokens.map((token) => token.id) },
        });
        return;
      }
      await route.continue();
    });
    await page.route("**/api/v2/identity/grants/*", async (route, request) => {
      const grantId = decodeURIComponent(
        request.url().split("/api/v2/identity/grants/")[1]?.split("?")[0] ?? "",
      );
      if (request.method() === "GET") {
        const token = tokens.find((entry) => entry.id === grantId);
        if (!token) {
          await route.fulfill({ status: 404, json: { error: "grant not found" } });
          return;
        }
        await route.fulfill({
          json: {
            scopes: (token.scopes ?? []).map((scope) => ({ scope, resource: [] })),
            createdAt: Math.floor(new Date(token.createdAt).getTime() / 1000),
            expiresAt: token.expiresAt
              ? Math.floor(new Date(token.expiresAt).getTime() / 1000)
              : 0,
          },
        });
        return;
      }
      await route.continue();
    });

    await page.route("**/api/v1/tokens", async (route, request) => {
      if (request.method() === "POST") {
        const body = request.postDataJSON() as { name?: string; scopes?: string };
        expect(body.scopes).toBe("other-app");
        tokens = [
          {
            id: "tok-race",
            scopes: body.scopes ? [body.scopes] : [],
            createdAt: "2026-03-01T12:00:00Z",
          },
        ];
        await route.fulfill({
          status: 201,
          json: {
            id: "tok-race",
            token: "gestalt_race_secret",
            scopes: ["other-app"],
          },
        });
        return;
      }

      await route.continue();
    });
    await mockIntegrations(page, []);

    await page.goto("/authorization");
    await page.getByLabel("Token name").fill("race-token");
    await page.getByLabel("Scopes").fill("other-app");
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(page.getByText("Copy this token now")).toBeVisible();
    await expect(page.locator("tr", { hasText: "tok-race" })).toBeVisible();
    await expect(page.getByText("No API tokens yet.")).toBeHidden();
  });

  test("revokes a token by grant ID", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    let tokens = [...sampleTokens];
    await page.route("**/api/v2/identity/grants", (route, request) => {
      if (request.method() === "GET") {
        route.fulfill({
          json: { grantIds: tokens.map((token) => token.id) },
        });
      } else {
        route.continue();
      }
    });
    await page.route("**/api/v2/identity/grants/*", (route, request) => {
      const grantId = decodeURIComponent(
        request.url().split("/api/v2/identity/grants/")[1]?.split("?")[0] ?? "",
      );
      if (request.method() === "GET") {
        const token = tokens.find((entry) => entry.id === grantId);
        if (!token) {
          route.fulfill({ status: 404, json: { error: "grant not found" } });
          return;
        }
        route.fulfill({
          json: {
            scopes: (token.scopes ?? []).map((scope) => ({ scope, resource: [] })),
            createdAt: Math.floor(new Date(token.createdAt).getTime() / 1000),
            expiresAt: token.expiresAt
              ? Math.floor(new Date(token.expiresAt).getTime() / 1000)
              : 0,
          },
        });
        return;
      }
      if (request.method() === "DELETE") {
        tokens = tokens.filter((entry) => entry.id !== grantId);
        route.fulfill({ json: {} });
        return;
      }
      route.continue();
    });
    await mockIntegrations(page, []);

    await page.goto("/authorization");
    await expect(page.getByText("tok-1")).toBeVisible();

    await page.getByRole("button", { name: "Revoke" }).first().click();
    await expect(page.getByText("tok-1")).toBeHidden();
    await expect(page.getByText("tok-2")).toBeVisible();
  });
});
