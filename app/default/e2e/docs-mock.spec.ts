import { test, expect, mockAuthInfo, mockAuthSession } from "./fixtures";

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

  test("unauthenticated user is redirected to server login from docs", async ({
    page,
  }) => {
    const pageErrors = trackPageErrors(page);
    await page.addInitScript(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await page.route("**/api/v1/auth/session", (route) => {
      route.fulfill({ status: 401, json: { error: "missing authorization" } });
    });

    await page.goto("/docs/getting-started");
    await expect(page).toHaveURL((url) => {
      return (
        url.pathname === "/api/v1/auth/login" &&
        url.searchParams.get("next") === "/docs/getting-started"
      );
    });
    expect(pageErrors).toEqual([]);
  });

  test("authenticated docs cover the main user workflows", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    const pageErrors = trackPageErrors(page);
    const expectedOrigin =
      process.env.PLAYWRIGHT_BASE_URL ||
      `http://localhost:${process.env.API_PORT || 8080}`;
    const leftNav = page.locator("aside").first();
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockAuthSession(page, {
      subjectId: "user:test@gestalt.dev",
      email: "test@gestalt.dev",
    });

    await page.goto("/docs");
    await expect(
      page.getByRole("heading", {
        name: "Getting Started",
      }),
    ).toBeVisible();
    await expect(
      leftNav.getByRole("link", { name: "Getting Started" }),
    ).toHaveAttribute("href", "/docs/getting-started");
    await expect(
      leftNav.getByRole("link", { name: "Invoke Operations" }),
    ).toHaveAttribute("href", "/docs/invoke");
    await expect(
      leftNav.getByRole("link", { name: "Grant Authorization" }),
    ).toHaveAttribute("href", "/docs/authorization");
    await expect(
      leftNav.getByRole("link", { name: "Use With MCP" }),
    ).toHaveAttribute("href", "/docs/mcp");
    await expect(page.getByText("Base URL", { exact: true })).toBeVisible();
    await expect(page.getByText("Current Host")).toHaveCount(0);
    await expect(page.locator("article")).not.toContainText("gestaltd --version");

    await leftNav.getByRole("link", { name: "Getting Started" }).click();
    await expect(page).toHaveURL(/\/docs\/getting-started/);
    await expect(
      page.getByRole("heading", { name: "Getting Started" }),
    ).toBeVisible();
    await expect(
      page.getByRole("tab", { name: "gestalt init" }),
    ).toBeVisible();
    await page.getByRole("tab", { name: "gestalt config set url" }).click();
    await expect(
      page.locator("#setup-config-set-panel"),
    ).toContainText(`gestalt config set url ${expectedOrigin}`);
    await expect(
      page.getByRole("tab", { name: "gestalt auth" }),
    ).toBeVisible();
    await expect(
      page.getByRole("tab", { name: "GESTALT_API_KEY" }),
    ).toBeVisible();
    await page.getByRole("tab", { name: "GESTALT_API_KEY" }).click();
    await expect(page.locator("#auth-token-panel")).toContainText(
      "export GESTALT_API_KEY=gst_api_your_token_here",
    );
    await expect(page.getByText("gestalt apps list", { exact: true })).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Grant authorization" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "gestalt authorization apps members set <app>",
    );
    await expect(page.locator("article")).toContainText(
      "gestalt authorization subjects grants set service_account:release-bot <app>",
    );
    await expect(
      page.getByRole("heading", { name: "Configure cloud environments" }),
    ).toBeVisible();
    const agentTabs = page.getByRole("tablist", {
      name: "Cloud environment configuration",
    });
    await expect(
      agentTabs.getByRole("tab", { name: "Claude Code web" }),
    ).toBeVisible();
    await expect(agentTabs.getByRole("tab", { name: "Codex Cloud" })).toBeVisible();
    await expect(
      agentTabs.getByRole("tab", { name: "Cursor Cloud Agents" }),
    ).toBeVisible();
    await expect(agentTabs.getByRole("tab")).toHaveText([
      "Claude Code web",
      "Codex Cloud",
      "Cursor Cloud Agents",
    ]);
    await expect(
      page.getByRole("link", { name: "claude.ai/code" }),
    ).toHaveAttribute("href", "https://claude.ai/code");
    await expect(
      page.getByAltText(
        "Claude Code web environment picker with the settings control highlighted",
      ),
    ).toBeVisible();
    await expect(page.locator("#agent-claude-code-panel")).toContainText(
      `GESTALT_URL=${expectedOrigin}`,
    );
    await expect(page.locator("#agent-claude-code-panel")).not.toContainText("BASE_URL");
    await expect(page.locator("#agent-claude-code-panel")).not.toContainText(
      "dedicated secrets store",
    );
    await agentTabs.getByRole("tab", { name: "Codex Cloud" }).click();
    await expect(
      page.getByRole("link", { name: "Codex environment settings" }),
    ).toHaveAttribute("href", "https://chatgpt.com/codex/settings/environments");
    await expect(page.locator("#agent-codex-panel")).toContainText(
      "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh",
    );
    await expect(page.locator("#agent-codex-panel")).not.toContainText("BASE_URL");
    await expect(page.locator("#agent-codex-panel")).toContainText(
      `GESTALT_URL=${expectedOrigin}`,
    );
    await expect(page.locator("#agent-codex-panel")).not.toContainText(
      "export GESTALT_API_KEY",
    );
    await expect(page.locator("#agent-codex-panel")).toContainText(
      "Codex secrets are only available during setup",
    );
    await agentTabs.getByRole("tab", { name: "Cursor Cloud Agents" }).click();
    await expect(
      page.getByRole("link", { name: "Cursor Cloud Agents settings" }),
    ).toHaveAttribute("href", "https://cursor.com/dashboard/cloud-agents#environments");
    await expect(page.locator("#agent-cursor-panel")).toContainText(
      ".cursor/environment.json",
    );
    await expect(page.locator("#agent-cursor-panel")).toContainText(
      '"install": "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh"',
    );
    await expect(page.locator("#agent-cursor-panel")).toContainText(
      `Set GESTALT_URL to ${expectedOrigin}`,
    );
    await expect(page.locator("#agent-cursor-panel")).toContainText(
      "GESTALT_API_KEY as a Cursor Cloud Agent secret",
    );
    await expect(page.locator("#agent-cursor-panel")).not.toContainText(
      "export GESTALT_API_KEY",
    );

    await leftNav.getByRole("link", { name: "Invoke Operations" }).click();
    await expect(page).toHaveURL(/\/docs\/invoke/);
    await expect(
      page.getByRole("heading", { name: "Invoke Operations" }),
    ).toBeVisible();
    await expect(page.getByRole("tab", { name: "CLI" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "HTTP" })).toBeVisible();
    await page.getByRole("tab", { name: "HTTP" }).click();
    await expect(
      page.getByText("/api/v1/apps").first(),
    ).toBeVisible();

    await leftNav.getByRole("link", { name: "Grant Authorization" }).click();
    await expect(page).toHaveURL(/\/docs\/authorization/);
    await expect(
      page.getByRole("heading", { name: "Grant Authorization" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "App admins can manage members for apps they administer",
    );
    await expect(page.locator("article")).toContainText("--url <management-url>");
    await expect(page.locator("article")).toContainText(
      "gestalt authorization apps members set <app>",
    );
    await expect(page.locator("article")).toContainText(
      "gestalt authorization subjects tokens create service_account:release-bot",
    );
    await expect(page.locator("article")).toContainText(
      "gestalt authorization admins members set",
    );

    await leftNav.getByRole("link", { name: "Manage Workflows" }).click();
    await expect(page).toHaveURL(/\/docs\/workflows/);
    await expect(
      page.getByRole("heading", { name: "Manage Workflows" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText("gestalt workflows --help");
    await expect(page.locator("article")).toContainText("gestalt workflows runs list");

    await leftNav.getByRole("link", { name: "Use With MCP" }).click();
    await expect(page).toHaveURL(/\/docs\/mcp/);
    await expect(
      page.getByRole("heading", { name: "Use With MCP" }),
    ).toBeVisible();
    await expect(
      page.getByText("claude mcp add --transport http").first(),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh",
    );
    await expect(
      page.getByRole("tab", { name: "Claude Code" }),
    ).toBeVisible();
    await expect(page.getByRole("tab", { name: "Codex" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "Cursor" })).toBeVisible();
    await page.getByRole("tab", { name: "Codex" }).click();
    await expect(
      page.locator("#mcp-codex-panel"),
    ).toContainText(
      'codex mcp add gestalt --url "$GESTALT_URL/mcp" --bearer-token-env-var GESTALT_API_KEY',
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
});
