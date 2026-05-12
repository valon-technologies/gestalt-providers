import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrations,
  mockTokens,
} from "./fixtures";

test.describe("Navigation", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
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
        scopes: "api",
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

  test("authorization page renders", async ({ authenticatedPage: page }) => {
    await page.goto("/authorization");
    await expect(
      page.getByRole("heading", { name: "Authorization" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Manage identities" }),
    ).toHaveCount(0);
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
    await page.getByRole("link", { name: "Authorization" }).click();
    await expect(page).toHaveURL(/authorization/);
    await expect(
      page.getByRole("heading", { name: "Authorization" }),
    ).toBeVisible();
  });

  test("plugins nav label keeps the integrations route", async ({ authenticatedPage: page }) => {
    await page.goto("/");
    await page.getByRole("link", { name: "Plugins", exact: true }).click();
    await expect(page).toHaveURL(/\/integrations/);
    await expect(page.getByRole("heading", { name: "Plugins" })).toBeVisible();
  });
  test("stable UI does not expose alpha surfaces or call alpha APIs", async ({
    authenticatedPage: page,
  }) => {
    const alphaRequests: string[] = [];
    page.on("request", (request) => {
      const path = new URL(request.url()).pathname;
      if (
        path.startsWith("/api/v1/workflow/") ||
        path.startsWith("/api/v1/agent/") ||
        path.startsWith("/api/v1/authorization/subjects")
      ) {
        alphaRequests.push(path);
      }
    });

    await page.goto("/");

    await expect(page.getByRole("link", { name: "Workflows" })).toHaveCount(0);
    await expect(page.getByRole("link", { name: "Agents" })).toHaveCount(0);
    await expect(page.getByRole("link", { name: "Manage identities" })).toHaveCount(0);
    await expect(page.getByText("Workflows", { exact: true })).toHaveCount(0);
    await expect(page.getByText("View agent sessions")).toHaveCount(0);
    expect(alphaRequests).toEqual([]);
  });

  test("direct alpha routes do not render alpha UI", async ({
    authenticatedPage: page,
  }) => {
    const routes = [
      { path: "/workflows", heading: "Workflows" },
      { path: "/agents", heading: "Agent Sessions" },
      { path: "/identities", heading: "Agent Identities" },
      { path: "/docs/workflows", heading: "Manage Workflows" },
    ];

    for (const route of routes) {
      await page.goto(route.path);
      await expect(
        page.getByRole("heading", { name: route.heading }),
      ).toHaveCount(0);
    }
  });
});
