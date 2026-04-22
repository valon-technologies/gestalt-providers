import {
  test,
  expect,
  mockAuthInfo,
  mockWorkflowEventTriggers,
  mockIntegrations,
  mockManagedIdentities,
  mockWorkflowSchedules,
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
        scopes: "api",
        createdAt: "2026-04-13T00:00:00Z",
      },
    ]);
    await mockWorkflowSchedules(authenticatedPage, []);
    await mockWorkflowEventTriggers(authenticatedPage, []);
    await mockWorkflowRuns(authenticatedPage, [
      {
        id: "run_123",
        provider: "basic",
        status: "succeeded",
        target: { plugin: "httpbin", operation: "get" },
        trigger: { kind: "schedule", scheduleId: "sched_123" },
        createdAt: "2026-04-13T00:00:00Z",
      },
    ]);
  });

  test("dashboard page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
    ).toBeVisible();
  });

  test("integrations page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/integrations");
    await expect(
      page.getByRole("heading", { name: "Plugins" }),
    ).toBeVisible();
  });

  test("identities page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/identities");
    await expect(
      page.getByRole("heading", { name: "Agent Identities" }),
    ).toBeVisible();
  });

  test("authorization page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/authorization");
    await expect(
      page.getByRole("heading", { name: "Authorization" }),
    ).toBeVisible();
  });

  test("workflows page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/workflows");
    await expect(
      page.getByRole("heading", { name: "Workflows" }),
    ).toBeVisible();
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
    await page.goto("/integrations");
    await page.getByRole("link", { name: "Authorization" }).click();
    await expect(page).toHaveURL(/authorization/);
    await expect(
      page.getByRole("heading", { name: "Authorization" }),
    ).toBeVisible();
    await page.getByRole("link", { name: "Workflows" }).click();
    await expect(page).toHaveURL(/workflows/);
    await expect(
      page.getByRole("heading", { name: "Workflows" }),
    ).toBeVisible();
  });

  test("plugins nav label keeps the integrations route", async ({ authenticatedPage: page }) => {
    await page.goto("/");
    await page.getByRole("link", { name: "Plugins", exact: true }).click();
    await expect(page).toHaveURL(/\/integrations/);
    await expect(page.getByRole("heading", { name: "Plugins" })).toBeVisible();
  });
});
