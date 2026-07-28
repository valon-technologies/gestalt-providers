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

    await page.goto("/settings/tokens");
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

    await page.goto("/settings/tokens");
    await expect(page.getByText("No API tokens yet.")).toBeVisible();
  });

  test("creates a scoped token and shows plaintext once", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    const tokens = await mockTokens(page, []);
    await mockPersonalTokenCreate(page, tokens, async (body) => {
      expect(body.name).toBe("audit-label");
      expect(body.expiresIn).toBe(30 * 24 * 60 * 60);
      return {
        token: {
          id: "tok-new",
          scopes: body.scopes ? body.scopes.split(/\s+/) : [],
          createdAt: "2026-03-01T12:00:00Z",
        },
        plaintext: "gestalt_abc123secret",
        expiresAt: "2027-03-01T12:00:00Z",
      };
    });
    await mockIntegrations(page, []);

    await page.goto("/settings/tokens");
    await page.getByLabel("Token name").fill("audit-label");
    await page.getByRole("radio", { name: /all apps/i }).click();
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(page.getByRole("textbox", { name: "API token" })).toHaveValue(
      "gestalt_abc123secret",
    );
    await expect(page.locator("tr", { hasText: "tok-new" })).toBeVisible();
  });

  test("keeps the created token visible when stale list requests finish later", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    const tokens = await mockTokens(page, [], { delayFirstListMs: 250 });
    await mockPersonalTokenCreate(page, tokens, async (body) => {
      return {
        token: {
          id: "tok-race",
          scopes: body.scopes ? body.scopes.split(/\s+/) : [],
          createdAt: "2026-03-01T12:00:00Z",
        },
        plaintext: "gestalt_race_secret",
      };
    });
    await mockIntegrations(page, []);

    await page.goto("/settings/tokens");
    await page.getByLabel("Token name").fill("race-token");
    await page.getByRole("radio", { name: /all apps/i }).click();
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(page.getByRole("textbox", { name: "API token" })).toHaveValue(
      "gestalt_race_secret",
    );
    await expect(page.locator("tr", { hasText: "tok-race" })).toBeVisible();
    await expect(page.getByText("No API tokens yet.")).toBeHidden();
  });

  test("revokes a token by grant ID", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockTokens(page, sampleTokens);
    await mockIntegrations(page, []);

    await page.goto("/settings/tokens");
    await expect(page.getByText("tok-1")).toBeVisible();

    await page.getByRole("button", { name: "Revoke" }).first().click();
    const revokeDialog = page.getByRole("alertdialog", { name: "Revoke token" });
    await expect(revokeDialog).toBeVisible();
    await revokeDialog.getByRole("button", { name: "Revoke token" }).click();
    await expect(revokeDialog).toBeHidden();
    await expect(page.locator("tr", { hasText: "tok-1" })).toHaveCount(0);
    await expect(page.locator("tr", { hasText: "tok-2" })).toBeVisible();
  });
});
