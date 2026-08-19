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
    const leftNav = page.getByRole("navigation", { name: "Documentation" });
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
    await expect(page.getByText("Setup", { exact: true }).first()).toBeVisible();
    await expect(page.getByText("Assistants", { exact: true }).first()).toBeVisible();
    await expect(page.getByText("Terminal", { exact: true }).first()).toBeVisible();
    await expect(page.getByText("Administer", { exact: true }).first()).toBeVisible();
    await expect(page.getByText("Help", { exact: true }).first()).toBeVisible();
    await expect(
      leftNav.getByRole("link", { name: "Grant App Access" }),
    ).toHaveAttribute("href", "/docs/authorization");
    await expect(
      leftNav.getByRole("link", { name: "MCP Clients" }),
    ).toHaveAttribute("href", "/docs/mcp");
    await expect(page.getByText("Base URL", { exact: true })).toBeVisible();
    await expect(page.locator("article")).toContainText(expectedOrigin);
    await expect(page.getByText("Current Host")).toHaveCount(0);
    await expect(page.locator("article")).not.toContainText("gestaltd --version");

    await leftNav.getByRole("link", { name: "Getting Started" }).click();
    await expect(page).toHaveURL(/\/docs\/getting-started/);
    await expect(
      page.getByRole("heading", { name: "Getting Started" }),
    ).toBeVisible();
    await expect(page.getByRole("heading", { name: "Sign in" })).toHaveCount(0);
    await expect(page.locator("article")).toContainText(
      "Gestalt is an API proxy",
    );
    await expect(page.locator("article")).toContainText(
      "universal key for your tools",
    );
    await expect(
      page.getByRole("heading", { name: "Connect Apps" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Create an API token" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "pick Claude or ChatGPT",
    );
    await expect(page.getByText("How access works")).toBeVisible();
    const tokenWarning = page.getByText("This token is shown only once");
    await expect(tokenWarning).toBeVisible();
    const createTokenBox = await page
      .getByRole("heading", { name: "Create an API token" })
      .boundingBox();
    const tokenWarningBox = await tokenWarning.boundingBox();
    expect(tokenWarningBox?.y ?? 0).toBeGreaterThan(createTokenBox?.y ?? 0);
    await expect(page.locator("article")).not.toContainText(
      "Your API token is shown only once",
    );
    await expect(
      page.getByRole("heading", { name: "Next steps" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Gestalt CLI" }).first(),
    ).toHaveAttribute("href", "/docs/cli");
    await expect(
      page.getByRole("heading", { name: "Configure MCP" }),
    ).toHaveCount(0);
    await expect(
      page.getByRole("heading", { name: "Install the CLI" }),
    ).toHaveCount(0);
    await expect(
      page.getByRole("radiogroup", { name: "Install methods" }),
    ).toHaveCount(0);
    await expect(page.getByTestId("docs-journey-footer")).toBeVisible();
    await expect(
      leftNav.getByRole("link", { name: "Gestalt CLI" }),
    ).toHaveAttribute("href", "/docs/cli");

    await leftNav.getByRole("link", { name: "Gestalt CLI" }).click();
    await expect(page).toHaveURL(/\/docs\/cli/);
    await expect(
      page.getByRole("heading", { name: "Gestalt CLI" }),
    ).toBeVisible();
    await expect(
      page.getByRole("radiogroup", { name: "Install methods" }),
    ).toBeVisible();
    await expect(page.getByRole("radio", { name: "Installer" })).toBeVisible();
    await expect(page.getByRole("radio", { name: "Homebrew" })).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh",
    );
    await page.getByRole("radio", { name: "Homebrew" }).click();
    await expect(page.locator("article")).toContainText(
      "brew install valon-technologies/gestalt/gestalt",
    );
    await expect(
      page.getByRole("link", { name: "GitHub releases page" }),
    ).toHaveAttribute(
      "href",
      "https://github.com/valon-technologies/gestalt/releases",
    );
    await expect(
      page.getByRole("radio", { name: "gestalt init" }),
    ).toBeVisible();
    await page.getByRole("radio", { name: "gestalt config set url" }).click();
    await expect(page.locator("article")).toContainText(
      `gestalt config set url ${expectedOrigin}`,
    );
    await expect(
      page.getByRole("radio", { name: "gestalt auth" }),
    ).toBeVisible();
    await expect(
      page.getByRole("radio", { name: "GESTALT_API_KEY" }),
    ).toBeVisible();
    await page.getByRole("radio", { name: "GESTALT_API_KEY" }).click();
    await expect(page.locator("article")).toContainText(
      "export GESTALT_API_KEY=gst_api_your_token_here",
    );
    await expect(page.getByText("gestalt apps list", { exact: true })).toBeVisible();

    await leftNav.getByRole("link", { name: "Connect Apps" }).click();
    await expect(page).toHaveURL(/\/docs\/connect/);
    await expect(
      page.getByRole("heading", { name: "Connect from the terminal" }),
    ).toBeVisible();
    await expect(
      page.locator("article").getByRole("link", { name: "Apps" }).first(),
    ).toHaveAttribute("href", "/apps");
    await expect(
      page.locator("article").getByRole("link", { name: "Gestalt CLI" }),
    ).toHaveAttribute("href", "/docs/cli");

    await leftNav.getByRole("link", { name: "Invoke Operations" }).click();
    await expect(page).toHaveURL(/\/docs\/invoke/);
    await expect(
      page.getByRole("heading", { name: "Invoke Operations" }),
    ).toBeVisible();
    await expect(page.getByRole("radio", { name: "CLI" })).toBeVisible();
    await expect(page.getByRole("radio", { name: "HTTP" })).toBeVisible();
    await page.getByRole("radio", { name: "HTTP" }).click();
    await expect(page).toHaveURL(/\/docs\/invoke#invoke-http$/);
    await expect(
      page.getByText("/api/v1/apps").first(),
    ).toBeVisible();

    await page.goto("/docs/invoke#invoke-http");
    await expect(page.getByRole("radio", { name: "HTTP" })).toBeChecked();
    await expect(
      page.getByText("/api/v1/apps").first(),
    ).toBeVisible();

    await leftNav.getByRole("link", { name: "Grant App Access" }).click();
    await expect(page).toHaveURL(/\/docs\/authorization/);
    await expect(
      page.getByRole("heading", { name: "Grant App Access" }),
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
    await expect(page.locator("article")).toContainText("grant it an app role");
    await expect(
      page.getByRole("link", { name: "Settings → API tokens" }),
    ).toHaveAttribute("href", "/settings/tokens");

    await leftNav.getByRole("link", { name: "API Tokens" }).click();
    await expect(page).toHaveURL(/\/docs\/tokens/);
    await expect(
      page.getByRole("link", { name: "Settings → API tokens" }),
    ).toHaveAttribute("href", "/settings/tokens");
    await expect(page.locator("article")).toContainText("Create a token");
    await expect(
      page.getByRole("heading", { name: "What to do with the token" }),
    ).toBeVisible();
    const tokensDestSwitch = page.getByRole("radiogroup", {
      name: "Choose your assistant",
    });
    await expect(tokensDestSwitch.getByRole("radio", { name: "Claude" })).toBeChecked();
    await expect(tokensDestSwitch.getByRole("radio", { name: "ChatGPT" })).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "Request headers",
    );
    await tokensDestSwitch.getByRole("radio", { name: "ChatGPT" }).click();
    await expect(page).toHaveURL(/\/docs\/tokens#dest-chatgpt$/);
    await expect(page.locator("article")).toContainText("Developer mode");
    await expect(
      page.getByText("Place the token here").first(),
    ).toBeVisible();
    const settingsLinkBox = await page
      .locator("article")
      .getByRole("link", { name: "Settings → API tokens" })
      .boundingBox();
    const cliHeadingBox = await page
      .getByRole("heading", { name: "Create from the terminal" })
      .boundingBox();
    expect(cliHeadingBox?.y ?? 0).toBeGreaterThan(settingsLinkBox?.y ?? 0);
    await expect(page.locator("article")).not.toContainText(
      "created from Authorization",
    );

    await leftNav.getByRole("link", { name: "Inspect Workflows" }).click();
    await expect(page).toHaveURL(/\/docs\/workflows/);
    await expect(
      page.getByRole("heading", { name: "Inspect Workflows" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText("gestalt workflows --help");
    await expect(page.locator("article")).toContainText("gestalt workflows runs list");

    await leftNav.getByRole("link", { name: "MCP Clients" }).click();
    await expect(page).toHaveURL(/\/docs\/mcp/);
    await expect(
      page.getByRole("heading", { name: "MCP Clients" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "create a token, open the app",
    );
    await expect(
      page.getByRole("heading", { name: "One path per app" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "Gestalt MCP is the default for company workspace apps",
    );
    await expect(
      page.getByRole("heading", { name: "Choose your assistant" }),
    ).toBeVisible();
    const mcpDestSwitch = page.getByRole("radiogroup", {
      name: "Choose your assistant",
    });
    await expect(mcpDestSwitch.getByRole("radio", { name: "Claude" })).toBeChecked();
    await expect(mcpDestSwitch.getByRole("radio", { name: "ChatGPT" })).toBeVisible();
    await expect(page.locator("article video")).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Store the token on your computer" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "export GESTALT_API_KEY=gst_api_your_token_here",
    );
    await expect(page.locator("article")).toContainText(
      `export GESTALT_URL=${expectedOrigin}`,
    );
    await expect(
      page.getByText("claude mcp add --transport http").first(),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Settings → API tokens" }),
    ).toHaveAttribute("href", "/settings/tokens");
    await expect(
      page.getByRole("link", { name: "API Tokens" }),
    ).toHaveAttribute("href", "/docs/tokens");
    const mcpSwitch = page.getByRole("radiogroup", {
      name: "MCP client configuration",
    });
    const mcpPanel = mcpSwitch
      .locator("xpath=ancestor::*[@data-docs-option-switcher][1]")
      .locator(":scope > div")
      .last();
    await expect(
      mcpSwitch.getByRole("radio", { name: "Claude Code" }),
    ).toBeVisible();
    await expect(mcpSwitch.getByRole("radio", { name: "Codex" })).toBeVisible();
    await expect(mcpSwitch.getByRole("radio", { name: "Cursor" })).toBeVisible();
    await mcpSwitch.getByRole("radio", { name: "Codex" }).click();
    await expect(page).toHaveURL(/\/docs\/mcp#mcp-codex$/);
    await expect(mcpPanel).toContainText(
      'codex mcp add gestalt --url "$GESTALT_URL/mcp" --bearer-token-env-var GESTALT_API_KEY',
    );
    await page.goto("/docs/mcp#mcp-cursor");
    await expect(mcpSwitch.getByRole("radio", { name: "Cursor" })).toBeChecked();
    await expect(mcpPanel).toContainText(".cursor/mcp.json");
    await mcpSwitch.getByRole("radio", { name: "Other clients" }).click();
    await expect(page).toHaveURL(/\/docs\/mcp#mcp-other$/);
    await expect(
      page.getByRole("cell", { name: `${expectedOrigin}/mcp` }).first(),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Configure cloud environments" }),
    ).toBeVisible();
    const agentSwitch = page.getByRole("radiogroup", {
      name: "Cloud environment configuration",
    });
    const agentPanel = agentSwitch
      .locator("xpath=ancestor::*[@data-docs-option-switcher][1]")
      .locator(":scope > div")
      .last();
    await expect(
      agentSwitch.getByRole("radio", { name: "Claude Code web" }),
    ).toBeVisible();
    await expect(
      agentSwitch.getByRole("radio", { name: "Codex Cloud" }),
    ).toBeVisible();
    await expect(
      agentSwitch.getByRole("radio", { name: "Cursor Cloud Agents" }),
    ).toBeVisible();
    await expect(agentSwitch.getByRole("radio")).toHaveText([
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
    await expect(agentPanel).toContainText(`GESTALT_URL=${expectedOrigin}`);
    await expect(agentPanel).not.toContainText("BASE_URL");
    await expect(agentPanel).not.toContainText("dedicated secrets store");
    await agentSwitch.getByRole("radio", { name: "Codex Cloud" }).click();
    await expect(
      page.getByRole("link", { name: "Codex environment settings" }),
    ).toHaveAttribute("href", "https://chatgpt.com/codex/settings/environments");
    await expect(agentPanel).toContainText(
      "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh",
    );
    await expect(agentPanel).not.toContainText("BASE_URL");
    await expect(agentPanel).toContainText(`GESTALT_URL=${expectedOrigin}`);
    await expect(agentPanel).toContainText(
      "Codex secrets are only available during setup",
    );
    await agentSwitch.getByRole("radio", { name: "Cursor Cloud Agents" }).click();
    await expect(
      page.getByRole("link", { name: "Cursor Cloud Agents settings" }),
    ).toHaveAttribute("href", "https://cursor.com/dashboard/cloud-agents#environments");
    await expect(agentPanel).toContainText(".cursor/environment.json");
    await expect(agentPanel).toContainText(
      '"install": "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh"',
    );
    await expect(agentPanel).toContainText(
      `Set GESTALT_URL to ${expectedOrigin}`,
    );
    await expect(agentPanel).toContainText(
      "GESTALT_API_KEY as a Cursor Cloud Agent secret",
    );
    await expect(
      page.getByRole("heading", { name: "Verify your tools" }),
    ).toBeVisible();
    await expect(page.getByText("gestalt integrations list")).toHaveCount(0);
    expect(pageErrors).toEqual([]);
  });
});
