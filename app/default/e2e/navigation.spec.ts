import {
  test,
  expect,
  mockAppAdminRegistry,
  mockAuthInfo,
  mockIntegrations,
  mockManagedIdentities,
  mockTokens,
  mockWorkflowRuns,
} from "./fixtures";

test.describe("Navigation", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(authenticatedPage, []);
    await mockIntegrations(authenticatedPage, [
      {
        name: "httpbin",
        displayName: "HTTPBin",
        description: "Development/testing-only HTTP request and response service. Do not use with production or sensitive data.",
        connected: true,
      },
    ]);
    await mockTokens(authenticatedPage, [
      {
        id: "tok_123",
        name: "Default token",
        scopes: ["api"],
        createdAt: "2026-04-13T00:00:00Z",
      },
    ]);
    await mockWorkflowRuns(authenticatedPage, [
      {
        id: "run_123",
        provider: "basic",
        status: "succeeded",
        target: {
          steps: [
            {
              id: "run",
              app: { name: "httpbin", operation: "get" },
            },
          ],
        },
        trigger: { kind: "schedule", activationId: "sched_123" },
        createdAt: "2026-04-13T00:00:00Z",
      },
    ]);
  });

  test("root redirects to apps", async ({ authenticatedPage: page }) => {
    await page.goto("/");
    await expect(page).toHaveURL(/\/apps/);
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
  });

  test("apps page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/apps");
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
  });

  test("identities page redirects into settings", async ({ authenticatedPage: page }) => {
    await page.goto("/identities");
    await expect(page).toHaveURL(/\/settings\/identities$/);
    await expect(
      page.getByRole("heading", { name: "Settings" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Managed identities" }),
    ).toBeVisible();
  });

  test("settings page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/settings");
    await expect(page).toHaveURL(/\/settings\/tokens$/);
    await expect(
      page.getByRole("heading", { name: "Settings" }),
    ).toBeVisible();
  });

  test("settings authorization hash lands on tokens anchor", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/settings#authorization");
    await expect(page).toHaveURL(/\/settings\/tokens#authorization$/);
    await expect(page.locator("#authorization")).toBeVisible();
  });

  test("workflows page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/workflows");
    await expect(
      page.getByRole("heading", { name: "Workflows" }),
    ).toBeVisible();
  });

  test("app admin workflows section renders", async ({ authenticatedPage: page }) => {
    await mockIntegrations(page, [
      {
        name: "slack",
        displayName: "Slack",
        managementPath: "/apps/slack/admin",
      },
    ]);
    await mockAppAdminRegistry(page, "slack", {
      app: "slack",
      registry: "toolshed",
      knownVersions: [],
      publishedVersions: [],
      selectionDisabled: false,
    });
    await mockWorkflowRuns(page, []);
    await page.goto("/apps/slack/admin/workflows");
    await expect(page.getByTestId("app-admin-nav-workflows")).toHaveClass(/font-medium/);
    await expect(page.getByTestId("app-workflow-ownership-note")).toBeVisible();
  });

  test("authorization redirects to settings tokens", async ({ authenticatedPage: page }) => {
    await page.goto("/authorization");
    await expect(page).toHaveURL(/\/settings\/tokens$/);
    await expect(
      page.getByRole("heading", { name: "Settings" }),
    ).toBeVisible();
  });

  test("tokens redirects to settings tokens", async ({ authenticatedPage: page }) => {
    await page.goto("/tokens");
    await expect(page).toHaveURL(/\/settings\/tokens$/);
    await expect(
      page.getByRole("heading", { name: "Settings" }),
    ).toBeVisible();
  });

  test("legacy workflows route redirects to apps", async ({ authenticatedPage: page }) => {
    await page.goto("/workflows");
    await expect(page).toHaveURL(/\/apps/);
  });

  test("docs page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/docs");
    await expect(page.getByRole("heading", { name: "Getting Started" })).toBeVisible();
  });

  test("docs subpages render", async ({ authenticatedPage: page }) => {
    await page.goto("/docs/getting-started");
    await expect(
      page.getByRole("heading", { name: "Getting Started" }),
    ).toBeVisible();

    await page.goto("/docs/workflows");
    await expect(
      page.getByRole("heading", { name: "Manage Workflows" }),
    ).toBeVisible();

    await page.goto("/docs/mcp");
    await expect(
      page.getByRole("heading", { name: "Use With MCP" }),
    ).toBeVisible();
  });

  test("nav links work", async ({ authenticatedPage: page }) => {
    await page.goto("/apps");
    await page.getByRole("button", { name: "Open user menu" }).click();
    await page.getByRole("menuitem", { name: "Settings" }).click();
    await expect(page).toHaveURL(/\/settings/);
    await expect(
      page.getByRole("heading", { name: "Settings" }),
    ).toBeVisible();
    await page.getByRole("link", { name: "Apps", exact: true }).click();
    await expect(page).toHaveURL(/\/apps/);
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
  });

  test("apps nav label uses the apps route", async ({ authenticatedPage: page }) => {
    await page.goto("/apps");
    await page.getByRole("link", { name: "Apps", exact: true }).click();
    await expect(page).toHaveURL(/\/apps/);
    await expect(page.getByRole("heading", { name: "Apps" })).toBeVisible();
  });
});
