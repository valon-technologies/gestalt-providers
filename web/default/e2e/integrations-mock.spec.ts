import { test, expect, mockIntegrations, mockManualConnect, mockTokens } from "./fixtures";
import type { Integration } from "../src/lib/api";

const OAUTH_INTEGRATION: Integration = {
  name: "oauth-svc", displayName: "OAuth Service", description: "Example OAuth integration",
};

const MANUAL_INTEGRATION: Integration = {
  name: "manual-svc", displayName: "Manual Service", description: "Example manual integration", authTypes: ["manual"],
  credentialFields: [{ name: "token", label: "API Token" }],
};

const MANUAL_WITH_LINKED_DESC: Integration = {
  name: "linked-svc", displayName: "Linked Service", authTypes: ["manual"],
  credentialFields: [{ name: "api_key", label: "API Key", description: "Find yours in [Account Settings](https://example.com/settings)" }],
};

const MULTI_CONNECTION_DUAL_AUTH_INTEGRATION: Integration = {
  name: "workspace-svc",
  displayName: "Workspace Service",
  authTypes: ["oauth", "manual"],
  credentialFields: [{ name: "api_token", label: "API Token" }],
  connections: [
    {
      name: "workspace",
      authTypes: ["oauth", "manual"],
      credentialFields: [{ name: "api_token", label: "API Token" }],
    },
    {
      name: "personal",
      authTypes: ["manual"],
      credentialFields: [{ name: "personal_token", label: "Personal Token" }],
    },
  ],
};

const MULTI_CONNECTION_MULTI_OAUTH_INTEGRATION: Integration = {
  name: "team-svc",
  displayName: "Team Service",
  authTypes: ["oauth", "manual"],
  credentialFields: [{ name: "api_token", label: "API Token" }],
  connections: [
    {
      name: "workspace",
      authTypes: ["oauth", "manual"],
      credentialFields: [{ name: "workspace_token", label: "Workspace Token" }],
    },
    {
      name: "personal",
      authTypes: ["oauth", "manual"],
      credentialFields: [{ name: "personal_token", label: "Personal Token" }],
    },
  ],
};

const MULTI_CONNECTION_OAUTH_ONLY_INTEGRATION: Integration = {
  name: "dual-oauth-svc",
  displayName: "Dual OAuth Service",
  connections: [
    {
      name: "oauth",
      displayName: "OAuth",
      authTypes: ["oauth"],
    },
    {
      name: "mcp",
      displayName: "MCP",
      authTypes: ["oauth"],
    },
  ],
};

const MCP_PASSTHROUGH_INTEGRATION: Integration = {
  name: "mcp-passthrough-svc",
  displayName: "MCP Passthrough Service",
  connections: [
    {
      name: "MCP",
      displayName: "MCP",
      authTypes: [],
    },
  ],
};

const sampleIntegrations: Integration[] = [
  OAUTH_INTEGRATION,
  MANUAL_INTEGRATION,
  { name: "another-svc", displayName: "Another Service" },
];

const SVG_WITHOUT_XMLNS_INTEGRATION: Integration = {
  name: "svg-svc",
  displayName: "SVG Service",
  iconSvg: `<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>`,
};

const SVG_WITH_DEFS_AND_UNSAFE_CONTENT = `<svg viewBox="0 0 24 24" onload="window.__iconPwned = true">
  <script>window.__iconPwned = true</script>
  <foreignObject><div>bad</div></foreignObject>
  <defs>
    <clipPath id="clip-badge">
      <circle cx="12" cy="12" r="10"/>
    </clipPath>
  </defs>
  <g clip-path="url(#clip-badge)">
    <path fill="currentColor" d="M0 0h24v24H0z"/>
  </g>
  <image href="https://example.com/evil.png" width="24" height="24"/>
</svg>`;

const SVG_WITH_UNSAFE_CONTENT_INTEGRATIONS: Integration[] = [
  {
    name: "unsafe-svg-one",
    displayName: "Unsafe SVG One",
    iconSvg: SVG_WITH_DEFS_AND_UNSAFE_CONTENT,
  },
  {
    name: "unsafe-svg-two",
    displayName: "Unsafe SVG Two",
    iconSvg: SVG_WITH_DEFS_AND_UNSAFE_CONTENT,
  },
];

test.describe("Integrations", () => {
  test("displays integration cards and actions", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);
    await mockTokens(page, []);

    await page.goto("/integrations");
    await expect(
      page.getByRole("heading", { name: "Plugins" }),
    ).toBeVisible();
    await expect(
      page.getByRole("combobox", { name: "Search plugins" }),
    ).toBeVisible();
    await expect(page.getByText(OAUTH_INTEGRATION.displayName!)).toBeVisible();
    await expect(page.getByText(MANUAL_INTEGRATION.displayName!)).toBeVisible();
    await expect(page.getByText("Another Service")).toBeVisible();
    await expect(page.getByText(OAUTH_INTEGRATION.description!)).toBeVisible();
    await expect(page.getByText(MANUAL_INTEGRATION.description!)).toBeVisible();
    await expect(page.getByRole("button", { name: "OAuth Service settings" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Manual Service settings" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Another Service settings" })).toBeVisible();
  });

  test("renders svg icons even when the payload omits xmlns", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [SVG_WITHOUT_XMLNS_INTEGRATION]);
    await mockTokens(page, []);

    await page.goto("/integrations");

    const card = page
      .getByTestId("plugin-grid")
      .locator("div")
      .filter({ has: page.getByText("SVG Service", { exact: true }) })
      .first();
    const icon = card.locator("svg[aria-hidden='true']").first();

    await expect(icon).toBeVisible();
    await expect(icon.locator("circle")).toHaveCount(1);
  });

  test("sanitizes inline svg content and rewrites duplicate ids", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, SVG_WITH_UNSAFE_CONTENT_INTEGRATIONS);
    await mockTokens(page, []);

    await page.goto("/integrations");
    await expect(page.getByText("Unsafe SVG One")).toBeVisible();
    await expect(page.getByText("Unsafe SVG Two")).toBeVisible();

    const summary = await page.evaluate(() => {
      const grid = document.querySelector("[data-testid='plugin-grid']");
      if (!grid) {
        return null;
      }

      const ids = Array.from(grid.querySelectorAll("svg [id]"))
        .map((element) => element.getAttribute("id"))
        .filter((value): value is string => !!value);
      const clipPaths = Array.from(grid.querySelectorAll("svg [clip-path]"))
        .map((element) => element.getAttribute("clip-path"))
        .filter((value): value is string => !!value);

      return {
        html: grid.innerHTML,
        ids,
        clipPaths,
        iconCount: grid.querySelectorAll("svg[aria-hidden='true']").length,
      };
    });

    expect(summary).not.toBeNull();
    expect(summary!.iconCount).toBe(2);
    expect(summary!.ids).toHaveLength(2);
    expect(new Set(summary!.ids).size).toBe(summary!.ids.length);
    expect(summary!.ids).not.toContain("clip-badge");
    expect(summary!.clipPaths).toHaveLength(2);
    expect(summary!.clipPaths.every((value) => value.startsWith("url(#provider-icon-"))).toBe(true);
    expect(summary!.html).not.toContain("<script");
    expect(summary!.html).not.toContain("foreignObject");
    expect(summary!.html).not.toContain("onload=");
    expect(summary!.html).not.toContain("https://example.com/evil.png");
  });

  test("shows empty state when no integrations", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, []);
    await mockTokens(page, []);

    await page.goto("/integrations");
    await expect(
      page.getByText("No plugins registered."),
    ).toBeVisible();
  });

  test("filters plugins by display name", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/integrations");
    const search = page.getByRole("combobox", { name: "Search plugins" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("manual");

    await expect(grid.getByText("Manual Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("OAuth Service", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("Another Service", { exact: true })).toHaveCount(0);
  });

  test("filters plugins by plugin name", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/integrations");
    const search = page.getByRole("combobox", { name: "Search plugins" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("oauth-svc");

    await expect(grid.getByText("OAuth Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("Manual Service", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("Another Service", { exact: true })).toHaveCount(0);
  });

  test("filters plugins by description text", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/integrations");
    const search = page.getByRole("combobox", { name: "Search plugins" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("example oauth integration");

    await expect(grid.getByText("OAuth Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("Manual Service", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("Another Service", { exact: true })).toHaveCount(0);
  });

  test("shows a search empty state when no plugins match", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/integrations");
    const search = page.getByRole("combobox", { name: "Search plugins" });

    await search.fill("missing-plugin");

    await expect(page.getByText('No plugins match "missing-plugin".')).toBeVisible();
    await expect(page.getByTestId("plugin-grid")).toHaveCount(0);
  });

  test("supports keyboard selection from the search results", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/integrations");
    const search = page.getByRole("combobox", { name: "Search plugins" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("oauth");
    await search.press("ArrowDown");
    await search.press("Enter");

    await expect(search).toHaveValue("OAuth Service");
    await expect(grid.getByText("OAuth Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("Manual Service", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("Another Service", { exact: true })).toHaveCount(0);
  });

  test("clearing the search restores the full grid and keeps focus", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/integrations");
    const search = page.getByRole("combobox", { name: "Search plugins" });
    const clearButton = page.locator('button[aria-label="Clear plugin search"]');
    const grid = page.getByTestId("plugin-grid");

    await search.fill("manual");
    await expect(grid.getByText("Manual Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("OAuth Service", { exact: true })).toHaveCount(0);

    await clearButton.click();

    await expect(search).toHaveValue("");
    await expect(search).toBeFocused();
    await expect(grid.getByText("OAuth Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("Manual Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("Another Service", { exact: true })).toBeVisible();
  });

  test("connected integration shows check icon and settings gear", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { ...OAUTH_INTEGRATION, connected: true, instances: [{ name: "default" }] },
      MANUAL_INTEGRATION,
    ]);

    await page.goto("/integrations");
    await expect(page.getByText(OAUTH_INTEGRATION.displayName!)).toBeVisible();
    await expect(page.getByText(MANUAL_INTEGRATION.displayName!)).toBeVisible();
    await expect(page.getByRole("button", { name: "OAuth Service settings" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Manual Service settings" })).toBeVisible();

    // Connected integration's settings shows Reconnect/Disconnect
    await page.getByRole("button", { name: "OAuth Service settings" }).click();
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByText("default")).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Add Connection" })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Disconnect" })).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();

    // Non-connected integration's settings shows Connect
    await page.getByRole("button", { name: "Manual Service settings" }).click();
    await expect(page.getByRole("dialog").getByRole("button", { name: "Connect" })).toBeVisible();
  });

  test("disconnect confirmation shows warning and allows cancel", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      { ...OAUTH_INTEGRATION, connected: true, instances: [{ name: "default" }] },
    ]);

    await page.goto("/integrations");
    await page.getByRole("button", { name: "OAuth Service settings" }).click();

    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Disconnect" }).click();

    await expect(dialog.getByText("Disconnect OAuth Service?")).toBeVisible();
    await expect(
      dialog.getByText(
        "This will remove your connection to OAuth Service. You can reconnect at any time.",
      ),
    ).toBeVisible();

    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(dialog.getByRole("button", { name: "Add Connection" })).toBeVisible();
  });

  test("disconnect calls API and refreshes list", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let disconnected = false;

    const connectedList = [{ ...OAUTH_INTEGRATION, connected: true, instances: [{ name: "default" }] }];
    const disconnectedList = [{ ...OAUTH_INTEGRATION, connected: false }];

    await mockIntegrations(page, connectedList, {
      onDisconnect: () => { disconnected = true; },
    });

    await page.goto("/integrations");
    await expect(page.getByRole("button", { name: "OAuth Service settings" })).toBeVisible();

    // Re-mock so GET returns disconnected state after DELETE fires
    await page.route("**/api/v1/integrations", (route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: disconnected ? disconnectedList : connectedList });
      } else {
        route.fallback();
      }
    });

    await page.getByRole("button", { name: "OAuth Service settings" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Disconnect" }).click();
    // Confirm the disconnect
    await dialog.getByRole("button", { name: "Disconnect" }).click();

    await expect(page.getByRole("dialog")).not.toBeVisible();
    // Settings gear is still visible (always shown), but integration is now disconnected
    await expect(page.getByRole("button", { name: "OAuth Service settings" })).toBeVisible();
    await page.getByRole("button", { name: "OAuth Service settings" }).click();
    await expect(page.getByRole("dialog").getByText("Not connected")).toBeVisible();
  });

  test("manual auth submits credential and refreshes", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let connected = false;
    let receivedCredential = "";

    const disconnectedList: Integration[] = [MANUAL_INTEGRATION];
    const connectedList: Integration[] = [{ ...MANUAL_INTEGRATION, connected: true }];

    await mockIntegrations(page, disconnectedList);
    await mockManualConnect(page, {
      onConnect: (_name, cred) => {
        connected = true;
        receivedCredential = cred;
      },
    });

    await page.goto("/integrations");
    await page.getByRole("button", { name: "Manual Service settings" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Connect" }).click();
    await dialog.getByLabel(/API token/i).fill("test-api-key-123");

    await page.route("**/api/v1/integrations", (route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: connected ? connectedList : disconnectedList });
      } else {
        route.fallback();
      }
    });

    await dialog.getByRole("button", { name: "Submit" }).click();
    await expect(page.getByRole("button", { name: "Manual Service settings" })).toBeVisible();
    expect(receivedCredential).toBe("test-api-key-123");
  });

  test("manual auth Cancel hides the form", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MANUAL_INTEGRATION]);

    await page.goto("/integrations");
    await page.getByRole("button", { name: "Manual Service settings" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Connect" }).click();
    await expect(dialog.getByLabel(/API token/i)).toBeVisible();
    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(dialog.getByText("Not connected")).toBeVisible();
  });

  test("multi-connection dual auth renders actions per connection", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MULTI_CONNECTION_DUAL_AUTH_INTEGRATION]);

    await page.goto("/integrations");
    await page.getByRole("button", { name: "Workspace Service settings" }).click();
    const dialog = page.getByRole("dialog");

    await expect(dialog.getByRole("button", { name: "Connect with workspace" })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Use API Token for workspace" })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Connect with personal" })).toBeVisible();
  });

  test("multi-connection oauth-only renders an action for MCP auth", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let requestBody: {
      integration: string;
      connection?: string;
      instance?: string;
    } | undefined;

    await mockIntegrations(page, [MULTI_CONNECTION_OAUTH_ONLY_INTEGRATION]);
    await page.route("**/api/v1/auth/start-oauth", async (route) => {
      requestBody = route.request().postDataJSON() as {
        integration: string;
        connection?: string;
        instance?: string;
      };
      await route.fulfill({
        json: { url: "about:blank", state: "state-123" },
      });
    });

    await page.goto("/integrations");
    await page.getByRole("button", { name: "Dual OAuth Service settings" }).click();
    const dialog = page.getByRole("dialog");

    await expect(dialog.getByRole("button", { name: "Connect with OAuth" })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Connect with MCP" })).toBeVisible();
    await expect(dialog.getByText("MCP passthrough", { exact: true })).toHaveCount(0);

    await dialog.getByRole("button", { name: "Connect with MCP" }).click();
    await page.waitForURL("about:blank");

    expect(requestBody).toMatchObject({
      integration: "dual-oauth-svc",
      connection: "mcp",
    });
  });

  test("no-auth MCP connections are shown as passive passthroughs", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MCP_PASSTHROUGH_INTEGRATION]);

    await page.goto("/integrations");
    await page.getByRole("button", { name: "MCP Passthrough Service settings" }).click();
    const dialog = page.getByRole("dialog");

    await expect(dialog.getByText("MCP", { exact: true })).toBeVisible();
    await expect(dialog.getByText("MCP passthrough", { exact: true })).toBeVisible();
    await expect(dialog.getByRole("button", { name: /connect/i })).toHaveCount(0);
  });

  test("multi-connection loading state stays on the clicked oauth action", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let releaseOAuthRequest: (() => void) | undefined;
    const oauthRequestReleased = new Promise<void>((resolve) => {
      releaseOAuthRequest = resolve;
    });

    await mockIntegrations(page, [MULTI_CONNECTION_MULTI_OAUTH_INTEGRATION]);
    await page.route("**/api/v1/auth/start-oauth", async (route) => {
      await oauthRequestReleased;
      await route.fulfill({ status: 500, body: "oauth failed" });
    });

    await page.goto("/integrations");
    await page.getByRole("button", { name: "Team Service settings" }).click();
    const dialog = page.getByRole("dialog");

    await dialog.getByRole("button", { name: "Connect with personal" }).click();
    await expect(dialog.getByRole("button", { name: "Connecting..." })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Connect with workspace" })).toBeVisible();

    releaseOAuthRequest?.();
    await expect(dialog.getByText("oauth failed")).toBeVisible();
  });

  test("manual auth reconnect opens token form via settings modal", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [{ ...MANUAL_INTEGRATION, connected: true, instances: [{ name: "default" }] }]);

    await page.goto("/integrations");
    await page.getByRole("button", { name: "Manual Service settings" }).click();
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByText("default")).toBeVisible();
    await dialog.getByRole("button", { name: "Add Connection" }).click();
    await expect(dialog.getByLabel("Connection name")).toBeVisible();
    await dialog.getByLabel("Connection name").fill("second");
    await dialog.getByRole("button", { name: "Continue" }).click();
    await expect(dialog.getByLabel(/API token/i)).toBeVisible();
  });

  test("credential field description renders inline links", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MANUAL_WITH_LINKED_DESC]);
    await mockTokens(page, []);

    await page.goto("/integrations");
    await page.getByRole("button", { name: "Linked Service settings" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Connect" }).click();

    const link = dialog.getByRole("link", { name: "Account Settings" });
    await expect(link).toBeVisible();
    await expect(link).toHaveAttribute("href", "https://example.com/settings");
    await expect(link).toHaveAttribute("target", "_blank");
    await expect(dialog.getByText("Find yours in")).toBeVisible();
  });
});
