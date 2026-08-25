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
    name: "CI pipeline",
    scopes: ["my-app"],
    createdAt: "2026-01-15T10:00:00Z",
  },
  {
    id: "tok-2",
    name: "Local CLI",
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
      page.getByRole("heading", { name: "API tokens" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Create token" }),
    ).toBeVisible();
    await expect(page.getByLabel("Token name")).toHaveCount(0);
    await expect(page.getByText("tok-1")).toBeVisible();
    await expect(page.getByText("tok-2")).toBeVisible();
    await expect(page.getByText("CI pipeline")).toBeVisible();
    await expect(page.getByText("Local CLI")).toBeVisible();
    await expect(page.getByText("my-app")).toBeVisible();
    await expect(page.getByText("other-app:read")).toBeVisible();
  });

  test("lists tokens newest created first and sorts from column headers", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockTokens(page, sampleTokens);
    await mockIntegrations(page, []);

    await page.goto("/settings/tokens");
    const rows = page.locator("table tbody tr");
    await expect(page.getByRole("columnheader", { name: "Created" })).toHaveAttribute(
      "aria-sort",
      "descending",
    );
    await expect(rows.nth(0)).toContainText("Local CLI");
    await expect(rows.nth(1)).toContainText("CI pipeline");

    await page.getByRole("columnheader", { name: "Name" }).getByRole("button").click();
    await expect(page.getByRole("columnheader", { name: "Name" })).toHaveAttribute(
      "aria-sort",
      "ascending",
    );
    await expect(rows.nth(0)).toContainText("CI pipeline");
    await expect(rows.nth(1)).toContainText("Local CLI");
  });

  test("collapses long scope lists behind a count", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockTokens(page, [
      {
        id: "tok-long",
        name: "Example app token",
        scopes: [
          "example-app:attachments.create",
          "example-app:contentRevisions.list",
          "example-app:customers.delete",
          "example-app:issues.list",
          "example-app:issues.update",
        ],
        createdAt: "2026-08-19T12:00:00Z",
      },
    ]);
    await mockIntegrations(page, []);

    await page.goto("/settings/tokens");
    await expect(page.getByText("example-app:attachments.create")).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Show 2 more scopes" }),
    ).toBeVisible();
    await expect(page.getByText("example-app:issues.update")).toBeHidden();

    await page.getByRole("button", { name: "Show 2 more scopes" }).click();
    await expect(page.getByRole("button", { name: "Show less" })).toBeVisible();
    await expect(page.getByText("example-app:issues.update")).toBeVisible();
  });

  test("shows empty state when no tokens", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockTokens(page, []);
    await mockIntegrations(page, []);

    await page.goto("/settings/tokens");
    await expect(page.getByText("No API tokens yet.")).toBeVisible();
    await expect(
      page.getByText(
        "Use Create token for scripts, MCP clients, and other tools.",
      ),
    ).toBeVisible();
    // Single CTA path: page header only (empty state is orientation, not a second button).
    await expect(page.getByRole("link", { name: "Create token" })).toHaveCount(1);
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

    await page.goto("/settings/tokens/new");
    await expect(page.getByRole("navigation", { name: "breadcrumb" })).toContainText(
      "Create token",
    );
    await expect(page.getByRole("link", { name: "Cancel" })).toBeVisible();

    await page.getByLabel("Token name").fill("audit-label");
    await page.getByRole("radio", { name: /all apps/i }).click();
    await page.getByRole("button", { name: "Create token" }).click();

    const secret = page.locator('[data-slot="copyable-code"]', {
      hasText: "gestalt_abc123secret",
    });
    await expect(secret).toBeVisible();
    await expect(page.getByRole("group", { name: "API token" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Copy token" })).toBeVisible();
    await expect(page.getByRole("textbox", { name: "API token" })).toHaveCount(0);
    await expect(
      page.getByText(/Copy this token now\. We won't show the full value again/),
    ).toBeVisible();
    await expect(
      page.getByText(
        "Your token is ready. Return to your tokens or choose your assistant in MCP Clients.",
      ),
    ).toBeVisible();
    await expect(page.getByRole("heading", { name: "Token created" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Cancel" })).toHaveCount(0);
    await expect(
      page.getByRole("link", { name: "MCP Clients" }),
    ).toHaveAttribute("href", "/docs/mcp");

    await page.getByRole("link", { name: "Back to tokens" }).click();
    await expect(page).toHaveURL(/\/settings\/tokens$/);
    await expect(page.locator("tr", { hasText: "tok-new" })).toBeVisible();
  });

  test("cancel returns to the token inventory without creating", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockTokens(page, []);
    await mockIntegrations(page, []);

    await page.goto("/settings/tokens/new");
    await page.getByLabel("Token name").fill("abandoned");
    await page.getByRole("link", { name: "Cancel" }).click();
    await expect(page).toHaveURL(/\/settings\/tokens$/);
    await expect(page.getByText("No API tokens yet.")).toBeVisible();
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

    // Prime the delayed first list request on the inventory page, then create.
    await page.goto("/settings/tokens");
    await expect(page.getByText("No API tokens yet.")).toBeVisible();
    await page.getByRole("link", { name: "Create token" }).click();
    await expect(page).toHaveURL(/\/settings\/tokens\/new$/);

    await page.getByLabel("Token name").fill("race-token");
    await page.getByRole("radio", { name: /all apps/i }).click();
    await page.getByRole("button", { name: "Create token" }).click();

    await expect(page.getByRole("group", { name: "API token" })).toBeVisible();
    await expect(page.getByText("gestalt_race_secret")).toBeVisible();
    await expect(page.getByRole("button", { name: "Copy token" })).toBeVisible();

    await page.getByRole("link", { name: "Back to tokens" }).click();
    await expect(page).toHaveURL(/\/settings\/tokens$/);
    await expect(page.locator("tr", { hasText: "tok-race" })).toBeVisible();
    await expect(page.getByText("No API tokens yet.")).toHaveCount(0);
  });

  test("revokes a token by grant ID", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockTokens(page, sampleTokens);
    await mockIntegrations(page, []);

    await page.goto("/settings/tokens");
    await expect(page.getByText("tok-1")).toBeVisible();

    await page
      .locator("tr", { hasText: "tok-1" })
      .getByRole("button", { name: "Revoke" })
      .click();
    const revokeDialog = page.getByRole("alertdialog", { name: "Revoke token" });
    await expect(revokeDialog).toBeVisible();
    await revokeDialog.getByRole("button", { name: "Revoke token" }).click();
    await expect(revokeDialog).toBeHidden();
    await expect(page.locator("tr", { hasText: "tok-1" })).toHaveCount(0);
    await expect(page.locator("tr", { hasText: "tok-2" })).toBeVisible();
  });
});
