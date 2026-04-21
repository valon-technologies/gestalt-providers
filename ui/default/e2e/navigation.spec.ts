import {
  test,
  expect,
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
        scopes: "api",
        createdAt: "2026-04-13T00:00:00Z",
      },
    ]);
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

  test("tokens page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/tokens");
    await expect(
      page.getByRole("heading", { name: "API Tokens" }),
    ).toBeVisible();
  });

  test("workflows page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/workflows");
    await expect(
      page.getByRole("heading", { name: "Workflow Runs" }),
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

    await page.goto("/docs/mcp");
    await expect(
      page.getByRole("heading", { name: "Use With MCP" }),
    ).toBeVisible();
  });

  test("nav links work", async ({ authenticatedPage: page }) => {
    await page.goto("/integrations");
    await page.getByRole("link", { name: "Identities" }).click();
    await expect(page).toHaveURL(/identities/);
    await expect(
      page.getByRole("heading", { name: "Agent Identities" }),
    ).toBeVisible();
    await page.getByRole("link", { name: "API Tokens" }).click();
    await expect(page).toHaveURL(/tokens/);
    await expect(
      page.getByRole("heading", { name: "API Tokens" }),
    ).toBeVisible();
    await page.getByRole("link", { name: "Workflows" }).click();
    await expect(page).toHaveURL(/workflows/);
    await expect(
      page.getByRole("heading", { name: "Workflow Runs" }),
    ).toBeVisible();
  });

  test("plugins nav label keeps the integrations route", async ({ authenticatedPage: page }) => {
    await page.goto("/");
    await page.getByRole("link", { name: "Plugins", exact: true }).click();
    await expect(page).toHaveURL(/\/integrations/);
    await expect(page.getByRole("heading", { name: "Plugins" })).toBeVisible();
  });
});
