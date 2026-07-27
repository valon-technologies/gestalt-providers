import {
  test,
  expect,
  mockTokens,
  mockPersonalTokenCreate,
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
    const tokens = await mockTokens(page, []);
    await mockPersonalTokenCreate(page, tokens, async (body) => {
      expect(body).toEqual({
        name: "audit-label",
        scopes: "my-app",
        expiresIn: 30 * 24 * 60 * 60,
      });
      return {
        token: {
          id: "tok-new",
          scopes: body.scopes ? [body.scopes] : [],
          createdAt: "2026-03-01T12:00:00Z",
        },
        plaintext: "gestalt_abc123secret",
        expiresAt: "2027-03-01T12:00:00Z",
      };
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
    const tokens = await mockTokens(page, [], { delayFirstListMs: 250 });
    await mockPersonalTokenCreate(page, tokens, async (body) => {
      expect(body.scopes).toBe("other-app");
      return {
        token: {
          id: "tok-race",
          scopes: body.scopes ? [body.scopes] : [],
          createdAt: "2026-03-01T12:00:00Z",
        },
        plaintext: "gestalt_race_secret",
      };
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
    await mockTokens(page, sampleTokens);
    await mockIntegrations(page, []);

    await page.goto("/authorization");
    await expect(page.getByText("tok-1")).toBeVisible();

    await page.getByRole("button", { name: "Revoke" }).first().click();
    await expect(page.getByText("tok-1")).toBeHidden();
    await expect(page.getByText("tok-2")).toBeVisible();
  });
});
