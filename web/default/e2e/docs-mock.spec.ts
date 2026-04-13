import { test, expect, mockAuthInfo } from "./fixtures";

const hasBackend = !!process.env.GESTALT_BASE_URL;

function trackPageErrors(page: import("@playwright/test").Page) {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  return errors;
}

test.describe("Docs page", () => {
  test.skip(
    hasBackend,
    "Docs page test uses mocked auth info and does not apply when running against a real server",
  );

  test("docs are reachable before login and cover the main user workflows", async ({
    page,
  }) => {
    const pageErrors = trackPageErrors(page);
    const expectedOrigin = process.env.PLAYWRIGHT_BASE_URL || "http://localhost:8080";
    const leftNav = page.locator("aside").first();
    await page.addInitScript(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });

    await page.goto("/login");
    await page.getByRole("link", { name: "documentation" }).click();

    await expect(page).toHaveURL(/\/docs/);
    await expect(
      page.getByRole("heading", {
        name: "Gestalt User Guide",
      }),
    ).toBeVisible();
    await expect(
      leftNav.getByRole("link", { name: "Overview" }),
    ).toHaveAttribute("href", "/docs/overview");
    await expect(
      leftNav.getByRole("link", { name: "Set Up The CLI" }),
    ).toHaveAttribute("href", "/docs/setup");
    await expect(
      leftNav.getByRole("link", { name: "Invoke Operations" }),
    ).toHaveAttribute("href", "/docs/invoke");
    await expect(
      leftNav.getByRole("link", { name: "Use With MCP" }),
    ).toHaveAttribute("href", "/docs/mcp");
    await expect(page.getByText("Base URL")).toBeVisible();
    await expect(page.getByText("Current Host")).toHaveCount(0);
    await expect(page.locator("article")).not.toContainText("gestaltd");

    await leftNav.getByRole("link", { name: "Set Up The CLI" }).click();
    await expect(page).toHaveURL(/\/docs\/setup/);
    await expect(
      page.getByRole("heading", { name: "Set Up The CLI" }),
    ).toBeVisible();
    await expect(
      page.getByRole("tab", { name: "gestalt init" }),
    ).toBeVisible();
    await page.getByRole("tab", { name: "gestalt config set url" }).click();
    await expect(
      page.locator("#setup-config-set-panel"),
    ).toContainText(`gestalt config set url ${expectedOrigin}`);

    await leftNav.getByRole("link", { name: "Invoke Operations" }).click();
    await expect(page).toHaveURL(/\/docs\/invoke/);
    await expect(
      page.getByRole("heading", { name: "Invoke Operations" }),
    ).toBeVisible();
    await expect(page.getByRole("tab", { name: "CLI" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "HTTP" })).toBeVisible();
    await page.getByRole("tab", { name: "HTTP" }).click();
    await expect(
      page.getByText("/api/v1/integrations").first(),
    ).toBeVisible();

    await leftNav.getByRole("link", { name: "Use With MCP" }).click();
    await expect(page).toHaveURL(/\/docs\/mcp/);
    await expect(
      page.getByRole("heading", { name: "Use With MCP" }),
    ).toBeVisible();
    await expect(
      page.getByText("claude mcp add --transport http").first(),
    ).toBeVisible();
    await expect(
      page.getByRole("tab", { name: "Claude Code" }),
    ).toBeVisible();
    await expect(page.getByRole("tab", { name: "Codex" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "Cursor" })).toBeVisible();
    await page.getByRole("tab", { name: "Codex" }).click();
    await expect(
      page.locator("#mcp-codex-panel"),
    ).toContainText(
      `codex mcp add gestalt --url ${expectedOrigin}/mcp --bearer-token-env-var GESTALT_API_KEY`,
    );
    await page.getByRole("tab", { name: "Cursor" }).click();
    await expect(
      page.locator("#mcp-cursor-panel"),
    ).toContainText(".cursor/mcp.json");
    await page.getByRole("tab", { name: "Other Clients" }).click();
    await expect(
      page.getByRole("cell", { name: `${expectedOrigin}/mcp` }).first(),
    ).toBeVisible();
    await expect(page.getByText("gestalt integrations list")).toHaveCount(0);
    expect(pageErrors).toEqual([]);
  });

  test("authenticated user can access docs without redirect", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    const pageErrors = trackPageErrors(page);
    await mockAuthInfo(page, {
      provider: "test-sso",
      display_name: "Test SSO",
    });

    await page.goto("/docs");
    await expect(page).toHaveURL(/\/docs/);
    await expect(
      page.getByRole("heading", { name: "Gestalt User Guide" }),
    ).toBeVisible();
    await expect(
      page.locator("aside").first().getByRole("link", { name: "Use With MCP" }),
    ).toHaveAttribute("href", "/docs/mcp");
    await expect(
      page.locator("nav").getByRole("link", { name: "Plugins", exact: true }),
    ).toBeVisible();
    await expect(page.getByText("test@gestalt.dev")).toBeVisible();
    expect(pageErrors).toEqual([]);
  });
});
