import {
  test,
  expect,
  mockTokens,
  mockIntegrations,
  mockIntegrationOperations,
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

    await page.goto("/settings");
    await expect(page).toHaveURL(/\/settings/);
    await expect(
      page.getByRole("heading", { name: "Settings" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Your API Tokens" }),
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

    await page.goto("/settings");
    await expect(page.getByText("No API tokens yet.")).toBeVisible();
  });

  test("creates a scoped token and shows plaintext once", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let tokens: APIToken[] = [];
    await mockTokens(page, () => tokens);
    await page.route("**/api/v1/tokens", async (route, request) => {
      if (request.method() === "POST") {
        const body = request.postDataJSON() as {
          name?: string;
          scopes?: string;
          expiresIn?: number;
        };
        expect(body).toEqual({
          name: "audit-label",
          scopes: "my-app",
          expiresIn: 30 * 24 * 60 * 60,
        });
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
      await route.fallback();
    });
    await mockIntegrations(page, [
      { name: "my-app", displayName: "My App" },
      { name: "other-app", displayName: "Other App" },
    ]);
    await mockIntegrationOperations(page, {
      "my-app": [{ id: "read", title: "Read" }],
      "other-app": [{ id: "write", title: "Write" }],
    });

    await page.goto("/settings");
    await page.getByLabel("Token name").fill("audit-label");
    await page.getByRole("radio", { name: /Only select apps/ }).click();
    await page.getByRole("checkbox", { name: "Select My App" }).click();
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(page.getByLabel("API token")).toHaveValue(
      "gestalt_abc123secret",
    );
    await expect(
      page.getByText("We'll use this token for this example"),
    ).toBeVisible();
    await expect(page.locator("tr", { hasText: "tok-new" })).toBeVisible();
    await expect(page.getByText("my-app")).toBeVisible();
  });

  test("keeps the created token visible when stale list requests finish later", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let tokens: APIToken[] = [];
    let listCount = 0;

    await page.route(
      (url) => url.pathname.includes("/api/v2/identity/grants"),
      async (route, request) => {
      const url = new URL(request.url());
      if (
        request.method() === "GET" &&
        /\/api\/v2\/identity\/grants\/?$/.test(url.pathname)
      ) {
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

      const match = url.pathname.match(
        /\/api\/v2\/identity\/grants\/([^/]+)\/?$/,
      );
      if (request.method() === "GET" && match) {
        const id = decodeURIComponent(match[1]!);
        const token = tokens.find((item) => item.id === id);
        if (!token) {
          await route.fulfill({ status: 404, json: { error: "not found" } });
          return;
        }
        const createdMs = Date.parse(token.createdAt);
        await route.fulfill({
          json: {
            scopes: (token.scopes ?? []).map((scope) => ({
              scope,
              resource: [],
            })),
            createdAt: Number.isFinite(createdMs)
              ? String(Math.floor(createdMs / 1000))
              : "0",
            expiresAt: "0",
          },
        });
        return;
      }

      await route.fallback();
    },
    );

    await page.route("**/api/v1/tokens", async (route, request) => {
      if (request.method() === "POST") {
        const body = request.postDataJSON() as {
          name?: string;
          scopes?: string;
        };
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
      await route.fallback();
    });
    await mockIntegrations(page, [
      { name: "my-app", displayName: "My App" },
      { name: "other-app", displayName: "Other App" },
    ]);
    await mockIntegrationOperations(page, {
      "my-app": [{ id: "read", title: "Read" }],
      "other-app": [{ id: "write", title: "Write" }],
    });

    await page.goto("/settings");
    await page.getByLabel("Token name").fill("race-token");
    await page.getByRole("radio", { name: /Only select apps/ }).click();
    await page.getByRole("checkbox", { name: "Select Other App" }).click();
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(page.getByLabel("API token")).toHaveValue(
      "gestalt_race_secret",
    );
    await expect(page.locator("tr", { hasText: "tok-race" })).toBeVisible();
    await expect(page.getByText("No API tokens yet.")).toBeHidden();
  });

  test("revokes a token by grant ID", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    let tokens = [...sampleTokens];
    await mockTokens(page, () => tokens, {
      onRevoke: (id) => {
        tokens = tokens.filter((token) => token.id !== id);
      },
    });
    await mockIntegrations(page, []);

    await page.goto("/settings");
    await expect(page.getByText("tok-1")).toBeVisible();

    await page.getByRole("button", { name: "Revoke" }).first().click();
    await expect(page.getByText("tok-1")).toBeHidden();
    await expect(page.getByText("tok-2")).toBeVisible();
  });
});
