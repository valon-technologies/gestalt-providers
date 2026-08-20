import { test, expect, mockAuthInfo, mockAuthSession } from "./fixtures";
import { GESTALT_PUBLIC_ORIGIN_PLACEHOLDER } from "../src/lib/gestaltPublicOrigin";

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
    // Mock e2e is localhost. Docs show the public-origin placeholder, not the
    // Playwright base URL, so people never paste 127.0.0.1 into an assistant.
    const expectedOrigin = GESTALT_PUBLIC_ORIGIN_PLACEHOLDER;
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
      page.getByRole("heading", { name: "Connect apps", exact: true }),
    ).toBeVisible();
    const connectPermalink = page.getByRole("link", {
      name: "# Link to Connect apps",
    });
    await expect(connectPermalink).toHaveAttribute("href", "#connect-apps");
    await page
      .getByRole("heading", { name: "Connect apps", exact: true })
      .hover();
    await expect(connectPermalink).toBeVisible();
    await connectPermalink.click();
    await expect(page).toHaveURL(/#connect-apps$/);
    await expect(
      page.getByRole("heading", { name: "Create an API token" }),
    ).toBeVisible();
    await expect(page.locator("article")).toContainText(
      "pick your assistant",
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
    await expect(page.getByTestId("docs-journey-next")).toContainText(
      "API Tokens",
    );
    await expect(page.getByTestId("docs-journey-previous")).toHaveCount(0);
    await expect(
      leftNav.getByRole("link", { name: "Gestalt CLI" }),
    ).toHaveAttribute("href", "/docs/cli");

    await leftNav.getByRole("link", { name: "Gestalt CLI" }).click();
    await expect(page).toHaveURL(/\/docs\/cli/);
    await expect(
      page.getByRole("heading", { name: "Gestalt CLI" }),
    ).toBeVisible();
    await expect(page.getByTestId("docs-journey-previous")).toHaveCount(0);
    await expect(page.getByTestId("docs-journey-next")).toContainText(
      "Connect apps",
    );
    await expect(page.locator("article")).toContainText(
      "same capabilities as the browser",
    );
    await expect(page.locator("article")).toContainText(
      "JSON payloads",
    );
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

    await leftNav.getByRole("link", { name: "Connect apps" }).click();
    await expect(page).toHaveURL(/\/docs\/connect/);
    await expect(
      page.getByRole("heading", { name: "Connect apps in the browser" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Connect from the terminal" }),
    ).toBeVisible();
    await expect(
      page.locator("article p").getByRole("link", { name: "Apps", exact: true }),
    ).toHaveAttribute("href", "/apps");
    await expect(
      page.locator("article p").getByRole("link", { name: "Gestalt CLI" }),
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
      page.getByRole("heading", { name: "Grant App Access", level: 1 }),
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
    await expect(page.getByTestId("docs-journey-footer")).toHaveCount(0);
    await expect(
      page.getByRole("link", { name: "Settings → API tokens" }),
    ).toHaveAttribute("href", "/settings/tokens");

    await leftNav.getByRole("link", { name: "API Tokens" }).click();
    await expect(page).toHaveURL(/\/docs\/tokens/);
    await expect(
      page.getByRole("link", { name: "Settings → API tokens" }),
    ).toHaveAttribute("href", "/settings/tokens");
    await expect(page.locator("article")).toContainText("click Create token");
    await expect(
      page.getByRole("heading", { name: "What to do with the token" }),
    ).toBeVisible();
    await expect(
      page.getByRole("radiogroup", { name: "Choose your assistant" }),
    ).toHaveCount(0);
    await expect(
      page.locator("article p").getByRole("link", { name: "MCP Clients" }),
    ).toHaveAttribute("href", "/docs/mcp");
    await expect(page.getByTestId("docs-journey-next")).toContainText(
      "MCP Clients",
    );
    await expect(page.getByTestId("docs-journey-previous")).toContainText(
      "Getting Started",
    );
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
    await expect(page.getByTestId("docs-journey-next")).toHaveCount(0);

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
    await expect(page.getByTestId("docs-journey-previous")).toContainText(
      "API Tokens",
    );
    await expect(page.getByTestId("docs-journey-next")).toHaveCount(0);
    const mcpDestSwitch = page.getByRole("radiogroup", {
      name: "Choose your assistant",
    });
    await expect(
      mcpDestSwitch.getByRole("radio", { name: "Claude Code" }),
    ).toBeChecked();
    await expect(
      mcpDestSwitch.getByRole("radio", { name: "ChatGPT", exact: true }),
    ).toBeVisible();
    await expect(
      mcpDestSwitch.getByRole("radio", { name: "Codex", exact: true }),
    ).toBeVisible();
    await expect(
      mcpDestSwitch.getByRole("radio", { name: "Cursor", exact: true }),
    ).toBeVisible();
    await expect(
      mcpDestSwitch.getByRole("radio", { name: "Cursor Agent", exact: true }),
    ).toBeVisible();
    await expect(page.locator("article video")).toHaveCount(0);
    await expect(
      page
        .getByRole("heading", { name: "Choose your assistant" })
        .getByRole("link"),
    ).toHaveAttribute("href", "#mcp-connect");
    await expect(page.locator("article")).toContainText(
      "claude mcp add --transport http",
    );
    await mcpDestSwitch.getByRole("radio", { name: "ChatGPT", exact: true }).click();
    await expect(page).toHaveURL(/\/docs\/mcp#dest-chatgpt$/);
    await expect(page.locator("article")).toContainText("Streamable HTTP");
    await expect(page.locator("article video")).toBeVisible();
    await mcpDestSwitch.getByRole("radio", { name: "Codex", exact: true }).click();
    await expect(page).toHaveURL(/\/docs\/mcp#dest-codex$/);
    await expect(page.locator("article")).toContainText("Codex Desktop");
    await expect(page.locator("article video")).toHaveCount(0);
    await expect(page.locator("article")).toContainText(
      `codex mcp add gestalt --url "${expectedOrigin}/mcp" --bearer-token-env-var GESTALT_API_KEY`,
    );
    await expect(page.locator("article")).toContainText(
      "Cloud agents do not use local",
    );
    await page.goto("/docs/mcp#mcp-chatgpt");
    await expect(page).toHaveURL(/\/docs\/mcp#dest-chatgpt$/);
    await expect(
      mcpDestSwitch.getByRole("radio", { name: "ChatGPT", exact: true }),
    ).toBeChecked();
    await expect(page.locator("article")).toContainText("Streamable HTTP");
    await expect(page.locator("article video")).toBeVisible();
    await mcpDestSwitch.getByRole("radio", { name: "Cursor Agent", exact: true }).click();
    await expect(page).toHaveURL(/\/docs\/mcp#dest-cursor-agent$/);
    await expect(page.locator("article")).toContainText(
      "Cursor Agent reads MCP servers",
    );
    await page.goto("/docs/mcp#mcp-cursor");
    await expect(page).toHaveURL(/\/docs\/mcp#dest-cursor$/);
    await expect(
      mcpDestSwitch.getByRole("radio", { name: "Cursor", exact: true }),
    ).toBeChecked();
    await expect(page.getByTestId("docs-add-to-cursor")).toHaveAttribute(
      "href",
      /cursor:\/\/anysphere\.cursor-deeplink\/mcp\/install/,
    );
    await expect(page.getByTestId("docs-add-to-cursor")).toContainText(
      "Add in Cursor",
    );
    const destPanel = mcpDestSwitch
      .locator("xpath=ancestor::*[@data-docs-option-switcher][1]")
      .locator(":scope > div")
      .last();
    await expect(destPanel).not.toContainText(".cursor/mcp.json");
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
      page.getByRole("link", { name: "Settings → API tokens" }),
    ).toHaveAttribute("href", "/settings/tokens");
    await expect(
      page.locator("article p").getByRole("link", { name: "API Tokens", exact: true }),
    ).toHaveAttribute("href", "/docs/tokens");
    await expect(
      page.getByRole("radiogroup", { name: "MCP client configuration" }),
    ).toHaveCount(0);
    await page.goto("/docs/mcp#mcp-other");
    await expect(
      page.getByRole("heading", { name: "Other clients" }),
    ).toBeVisible();
    await expect(
      page
        .getByTestId("docs-info-table")
        .filter({ hasText: `${expectedOrigin}/mcp` })
        .first(),
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
    await expect(agentPanel).not.toContainText("export GESTALT_API_KEY");
    await expect(agentPanel).not.toContainText("BASE_URL");
    await expect(agentPanel).not.toContainText("dedicated secrets store");
    await agentSwitch.getByRole("radio", { name: "Codex Cloud" }).click();
    await expect(
      page.getByRole("link", { name: "Codex environment settings" }),
    ).toHaveAttribute("href", "https://chatgpt.com/codex/settings/environments");
    await expect(agentPanel).toContainText(
      "curl -fsSL https://gestaltd.ai/install-gestalt.sh | sh",
    );
    await expect(agentPanel).not.toContainText("export GESTALT_API_KEY");
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
