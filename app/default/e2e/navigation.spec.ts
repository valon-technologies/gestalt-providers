import {
  test,
  expect,
  mockAppAdminRegistry,
  mockAuthInfo,
  mockIntegrations,
  mockTokens,
  mockWorkflowRuns,
  mockWorkflowDefinitions,
} from "./fixtures";

test.describe("Navigation", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
      features: { workflowDefaultProvider: "basic" },
    });
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

  test("legacy identities paths redirect to apps", async ({ authenticatedPage: page }) => {
    await page.goto("/identities");
    await expect(page).toHaveURL(/\/apps$/);

    await page.goto("/settings/identities");
    await expect(page).toHaveURL(/\/apps$/);
  });

  test("settings page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/settings");
    await expect(page).toHaveURL(/\/settings\/tokens$/);
    await expect(
      page.getByRole("heading", { name: "Settings" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Your API tokens" }),
    ).toBeVisible();
  });

  test("settings authorization hash lands on create token", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/settings#authorization");
    await expect(page).toHaveURL(/\/settings\/tokens\/new$/);
    await expect(
      page.getByRole("heading", { name: "Create token" }),
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
      registry: "example-registry",
      knownVersions: [],
      publishedVersions: [],
      selectionDisabled: false,
    });
    await mockWorkflowRuns(page, []);
    await mockWorkflowDefinitions(page, []);
    await page.goto("/apps/slack/admin/workflows");
    await expect(page.getByTestId("app-admin-nav-workflows")).toHaveClass(/font-medium/);
    await expect(page.getByRole("heading", { name: "Runs", exact: true })).toBeVisible();
  });

  test("authorization redirects to create token", async ({ authenticatedPage: page }) => {
    await page.goto("/authorization");
    await expect(page).toHaveURL(/\/settings\/tokens\/new$/);
    await expect(
      page.getByRole("heading", { name: "Create token" }),
    ).toBeVisible();
  });

  test("tokens redirects to settings tokens", async ({ authenticatedPage: page }) => {
    await page.goto("/tokens");
    await expect(page).toHaveURL(/\/settings\/tokens$/);
    await expect(
      page.getByRole("heading", { name: "Your API tokens" }),
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
      page.getByRole("heading", { name: "Inspect Workflows" }),
    ).toBeVisible();

    await page.goto("/docs/mcp");
    await expect(
      page.getByRole("heading", { name: "Use With MCP" }),
    ).toBeVisible();
  });

  test("nav links work", async ({ authenticatedPage: page }) => {
    await page.goto("/apps");
    await page.getByRole("button", { name: "Open user menu" }).click();
    await expect(page.getByRole("menuitem", { name: "Docs" })).toBeVisible();
    await page.getByRole("menuitem", { name: "Settings" }).click();
    await expect(page).toHaveURL(/\/settings/);
    await expect(
      page.getByRole("heading", { name: "Your API tokens" }),
    ).toBeVisible();
    await page.getByRole("link", { name: "Apps", exact: true }).click();
    await expect(page).toHaveURL(/\/apps/);
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
  });

  test("account menu exposes Docs for signed-in users", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/apps");
    await expect(
      page.getByRole("navigation", { name: "Primary" }).getByRole("link", {
        name: "Docs",
      }),
    ).toHaveCount(0);
    await page.getByRole("button", { name: "Open user menu" }).click();
    await page.getByRole("menuitem", { name: "Docs" }).click();
    await expect(page).toHaveURL(/\/docs/);
  });

  test("apps nav label uses the apps route", async ({ authenticatedPage: page }) => {
    await page.goto("/apps");
    await page.getByRole("link", { name: "Apps", exact: true }).click();
    await expect(page).toHaveURL(/\/apps/);
    await expect(page.getByRole("heading", { name: "Apps" })).toBeVisible();
  });
});
