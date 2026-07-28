import { test, expect, mockIntegrations, mockManualConnect, mockTokens } from "./fixtures";
import type { Page } from "@playwright/test";
import type { Integration } from "../src/lib/api";

async function openAppConnection(page: Page, appName: string) {
  await page.goto(`/apps/${appName}/connection`);
  await expect(page.getByTestId("app-admin-connection")).toBeVisible();
  return page.getByTestId(`integration-connection-${appName}`);
}

const OAUTH_INTEGRATION: Integration = {
  name: "oauth-svc",
  displayName: "OAuth Service",
  description: "Example OAuth integration",
  connections: [{ name: "plugin", authTypes: ["oauth"] }],
};

const MANUAL_INTEGRATION: Integration = {
  name: "manual-svc",
  displayName: "Manual Service",
  description: "Example manual integration",
  connections: [{
    name: "plugin",
    authTypes: ["manual"],
    credentialFields: [{ name: "token", label: "API Token" }],
  }],
};

const MANUAL_WITH_LINKED_DESC: Integration = {
  name: "linked-svc",
  displayName: "Linked Service",
  connections: [{
    name: "plugin",
    authTypes: ["manual"],
    credentialFields: [{ name: "api_key", label: "API Key", description: "Find yours in [Account Settings](https://example.com/settings)" }],
  }],
};

const MULTI_CONNECTION_DUAL_AUTH_INTEGRATION: Integration = {
  name: "workspace-svc",
  displayName: "Workspace Service",
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
      credentialMode: "none",
      credentialState: "not_required",
      status: "ready",
      healthState: "not_applicable",
      mcpPassthrough: true,
    },
  ],
};

const NO_AUTH_WITH_USER_INTEGRATION: Integration = {
  name: "no-auth-svc",
  displayName: "No Auth Service",
  connections: [
    {
      name: "webhook",
      displayName: "Webhook",
      credentialMode: "none",
      ownerKind: "none",
      credentialState: "not_required",
      healthState: "not_applicable",
      status: "ready",
      actions: [],
    },
    {
      name: "workspace",
      displayName: "Workspace",
      credentialMode: "subject",
      ownerKind: "current_user",
      credentialState: "missing",
      healthState: "unknown",
      status: "needs_user_connection",
      actions: ["connect"],
      authTypes: ["oauth"],
    },
  ],
};

const USER_CONNECTION_ACTIONS_INTEGRATION: Integration = {
  name: "user-actions-svc",
  displayName: "User Actions Service",
  status: "ready",
  credentialState: "connected",
  connections: [
    {
      name: "workspace",
      displayName: "Workspace",
      credentialMode: "subject",
      ownerKind: "current_user",
      credentialState: "connected",
      healthState: "healthy",
      status: "ready",
      actions: ["add_instance", "reconnect", "disconnect"],
      authTypes: ["manual"],
      credentialFields: [{ name: "token", label: "Workspace Token" }],
      instances: [
        { name: "prod", connection: "workspace" },
        { name: "staging", connection: "workspace" },
      ],
    },
  ],
};

const SELECT_INSTANCE_INTEGRATION: Integration = {
  name: "select-instance-svc",
  displayName: "Select Instance Service",
  status: "needs_instance_selection",
  credentialState: "connected",
  connections: [
    {
      name: "workspace",
      displayName: "Workspace",
      credentialMode: "subject",
      ownerKind: "current_user",
      credentialState: "connected",
      healthState: "healthy",
      status: "needs_instance_selection",
      actions: ["select_instance"],
      authTypes: ["oauth"],
      instances: [
        { name: "alpha", connection: "workspace" },
        { name: "beta", connection: "workspace" },
      ],
    },
  ],
};

const MOUNTED_UI_INTEGRATION: Integration = {
  name: "mounted-ui-svc",
  displayName: "Mounted UI Service",
  description: "Example mounted plugin UI",
  mountedPath: "/mounted-ui",
};

const MOUNTED_UI_WITH_SETTINGS_INTEGRATION: Integration = {
  name: "mounted-ui-settings-svc",
  displayName: "Mounted UI With Settings",
  description: "Mounted UI with a connectable plugin entry",
  mountedPath: "/mounted-settings-ui",
  connections: [{ name: "plugin", authTypes: ["oauth"] }],
};

const sampleIntegrations: Integration[] = [
  OAUTH_INTEGRATION,
  MANUAL_INTEGRATION,
  { name: "another-svc", displayName: "Another Service" },
];

function withConnectedConnection(
  integration: Integration,
  connectionName = integration.connections?.[0]?.name ?? "plugin",
  instanceName = "default",
): Integration {
  return {
    ...integration,
    status: "ready",
    credentialState: "connected",
    healthState: "not_checked",
    connections: integration.connections?.map((connection) =>
      connection.name === connectionName
        ? {
            ...connection,
            status: "ready",
            credentialState: "connected",
            healthState: "not_checked",
            actions: ["add_instance", "disconnect"],
            instances: [{ name: instanceName, connection: connectionName }],
          }
        : connection,
    ),
  };
}

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

    await page.goto("/apps");
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
    await expect(
      page.getByRole("searchbox", { name: "Search apps" }),
    ).toBeVisible();
    await expect(page.getByText(OAUTH_INTEGRATION.displayName!)).toBeVisible();
    await expect(page.getByText(MANUAL_INTEGRATION.displayName!)).toBeVisible();
    await expect(page.getByText("Another Service")).toBeVisible();
    await expect(page.getByText(OAUTH_INTEGRATION.description!)).toBeVisible();
    await expect(page.getByText(MANUAL_INTEGRATION.description!)).toBeVisible();
    await expect(page.getByRole("button", { name: "Add OAuth Service" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Add Manual Service" })).toBeVisible();
    await expect(page.getByTestId("integration-card-another-svc")).toBeVisible();
  });

  test("renders svg icons even when the payload omits xmlns", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [SVG_WITHOUT_XMLNS_INTEGRATION]);
    await mockTokens(page, []);

    await page.goto("/apps");

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

    await page.goto("/apps");
    await expect(page.getByText("Unsafe SVG One")).toBeVisible();
    await expect(page.getByText("Unsafe SVG Two")).toBeVisible();

    const summary = await page.evaluate(() => {
      const grid = document.querySelector("[data-testid='plugin-grid']");
      if (!grid) {
        return null;
      }

      const iconRoots = grid.querySelectorAll(
        "[data-testid^='integration-card-'] .size-14",
      );
      const ids = Array.from(iconRoots).flatMap((root) =>
        Array.from(root.querySelectorAll("svg [id]"))
          .map((element) => element.getAttribute("id"))
          .filter((value): value is string => !!value),
      );
      const clipPaths = Array.from(iconRoots).flatMap((root) =>
        Array.from(root.querySelectorAll("svg [clip-path]"))
          .map((element) => element.getAttribute("clip-path"))
          .filter((value): value is string => !!value),
      );

      return {
        html: grid.innerHTML,
        ids,
        clipPaths,
        iconCount: iconRoots.length,
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

  test("shows empty state when no apps", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, []);
    await mockTokens(page, []);

    await page.goto("/apps");
    await expect(
      page.getByText("No apps registered."),
    ).toBeVisible();
  });

  test("mounted ui cards navigate to app detail", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MOUNTED_UI_INTEGRATION]);
    await mockTokens(page, []);

    await page.goto("/apps");
    await page.getByTestId("integration-card-mounted-ui-svc").click();

    await page.waitForURL("**/apps/mounted-ui-svc");
    await expect(
      page.getByRole("heading", { level: 1, name: "Mounted UI Service" }),
    ).toBeVisible();
  });

  test("open app button launches mounted ui", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MOUNTED_UI_INTEGRATION]);
    await mockTokens(page, []);

    await page.goto("/apps");
    await page.getByTestId("open-app-mounted-ui-svc").click();

    await page.waitForURL("**/mounted-ui");
  });

  test("card click opens app detail page", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    const integration = withConnectedConnection(MOUNTED_UI_WITH_SETTINGS_INTEGRATION);
    await mockIntegrations(page, [integration]);
    await mockTokens(page, []);

    await page.goto("/apps");
    await page.getByTestId("integration-card-mounted-ui-settings-svc").click();

    await page.waitForURL("**/apps/mounted-ui-settings-svc");
    await expect(
      page.getByRole("heading", { level: 1, name: "Mounted UI With Settings" }),
    ).toBeVisible();
  });

  test("catalog more menu only lists remove app", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    const integration = withConnectedConnection(MOUNTED_UI_WITH_SETTINGS_INTEGRATION);
    await mockIntegrations(page, [integration]);
    await mockTokens(page, []);

    await page.goto("/apps");
    await page.getByRole("button", { name: "Mounted UI With Settings options" }).click();
    await expect(page.getByRole("menuitem", { name: "Settings" })).toHaveCount(0);
    await expect(page.getByRole("menuitem", { name: "App details" })).toHaveCount(0);
    await expect(page.getByRole("menuitem", { name: "Manage app" })).toHaveCount(0);
    await expect(page.getByRole("menuitem", { name: "Remove app" })).toBeVisible();
  });

  test("filters apps by display name", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/apps");
    const search = page.getByRole("searchbox", { name: "Search apps" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("manual");

    await expect(grid.getByText("Manual Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("OAuth Service", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("Another Service", { exact: true })).toHaveCount(0);
  });

  test("filters apps by plugin name", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/apps");
    const search = page.getByRole("searchbox", { name: "Search apps" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("oauth-svc");

    await expect(grid.getByText("OAuth Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("Manual Service", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("Another Service", { exact: true })).toHaveCount(0);
  });

  test("filters apps by description text", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/apps");
    const search = page.getByRole("searchbox", { name: "Search apps" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("example oauth integration");

    await expect(grid.getByText("OAuth Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("Manual Service", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("Another Service", { exact: true })).toHaveCount(0);
  });

  test("shows a search empty state when no apps match", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/apps");
    const search = page.getByRole("searchbox", { name: "Search apps" });

    await search.fill("missing-plugin");

    await expect(page.getByText('No apps match "missing-plugin".')).toBeVisible();
    await expect(page.getByTestId("plugin-grid")).toHaveCount(0);
  });

  test("does not render a suggestion list over the grid", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/apps");
    const search = page.getByRole("searchbox", { name: "Search apps" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("oauth");
    await expect(page.getByRole("listbox")).toHaveCount(0);
    await expect(page.getByRole("option")).toHaveCount(0);
    await expect(grid.getByText("OAuth Service", { exact: true })).toBeVisible();
    await expect(grid.getByText("Manual Service", { exact: true })).toHaveCount(0);
    await expect(grid.getByText("Another Service", { exact: true })).toHaveCount(0);
  });

  test("clearing the search restores the full grid and keeps focus", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/apps");
    const search = page.getByRole("searchbox", { name: "Search apps" });
    const clearButton = page.locator('button[aria-label="Clear app search"]');
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

  test("connected integration shows checkmark and connection page actions", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      withConnectedConnection(OAUTH_INTEGRATION),
      MANUAL_INTEGRATION,
    ]);

    await page.goto("/apps");
    await expect(page.getByText(OAUTH_INTEGRATION.displayName!)).toBeVisible();
    await expect(page.getByText(MANUAL_INTEGRATION.displayName!)).toBeVisible();
    await expect(page.getByTestId("integration-card-oauth-svc").getByLabel("Installed")).toBeVisible();
    await expect(page.getByRole("button", { name: "OAuth Service options" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Add Manual Service" })).toBeVisible();

    const oauthPanel = await openAppConnection(page, "oauth-svc");
    await expect(oauthPanel.getByText("default")).toBeVisible();
    await expect(oauthPanel.getByRole("button", { name: "Add connection" })).toBeVisible();
    await expect(oauthPanel.getByRole("button", { name: "Disconnect" })).toBeVisible();

    const manualPanel = await openAppConnection(page, "manual-svc");
    await expect(manualPanel.getByRole("button", { name: "Connect" })).toBeVisible();
  });

  test("disconnect confirmation shows warning and allows cancel", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      withConnectedConnection(OAUTH_INTEGRATION),
    ]);

    const panel = await openAppConnection(page, "oauth-svc");
    await panel.getByRole("button", { name: "Disconnect" }).click();

    await expect(panel.getByText("Disconnect OAuth Service?")).toBeVisible();
    await expect(
      panel.getByText(
        "This will remove your connection to OAuth Service. You can reconnect at any time.",
      ),
    ).toBeVisible();

    await panel.getByRole("button", { name: "Cancel" }).click();
    await expect(panel.getByRole("button", { name: "Add connection" })).toBeVisible();
  });

  test("disconnect calls API and refreshes list", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let disconnected = false;
    let disconnectURL: URL | undefined;

    const oauthConnectionIntegration: Integration = {
      ...OAUTH_INTEGRATION,
      connections: [{ name: "oauth", displayName: "OAuth", authTypes: ["oauth"] }],
    };
    const connectedList = [withConnectedConnection(oauthConnectionIntegration, "oauth", "prod")];
    const disconnectedList = [oauthConnectionIntegration];

    await mockIntegrations(page, connectedList, {
      onDisconnect: (_name, url) => {
        disconnected = true;
        disconnectURL = url;
      },
    });

    await page.route("**/api/v1/apps", (route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: disconnected ? disconnectedList : connectedList });
      } else {
        route.fallback();
      }
    });

    const panel = await openAppConnection(page, "oauth-svc");
    await panel.getByRole("button", { name: "Disconnect" }).click();
    await panel.getByRole("button", { name: "Disconnect" }).click();

    await expect.poll(() => disconnected).toBe(true);
    const refreshedPanel = await openAppConnection(page, "oauth-svc");
    await expect(refreshedPanel.getByText("Not connected")).toHaveCount(0);
    await expect(refreshedPanel.getByRole("button", { name: "Connect" })).toBeVisible();
    expect(disconnectURL?.searchParams.get("_instance")).toBe("prod");
    expect(disconnectURL?.searchParams.get("_connection")).toBe("oauth");
    expect(disconnectURL?.searchParams.has("instance")).toBe(false);
    expect(disconnectURL?.searchParams.has("connection")).toBe(false);
  });

  test("manual auth submits credential and refreshes", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    let connected = false;
    let receivedCredential = "";

    const disconnectedList: Integration[] = [MANUAL_INTEGRATION];
    const connectedList: Integration[] = [withConnectedConnection(MANUAL_INTEGRATION)];

    await mockIntegrations(page, disconnectedList);
    await mockManualConnect(page, {
      onConnect: (_name, cred) => {
        connected = true;
        receivedCredential = cred;
      },
    });

    await page.route("**/api/v1/apps", (route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: connected ? connectedList : disconnectedList });
      } else {
        route.fallback();
      }
    });

    const panel = await openAppConnection(page, "manual-svc");
    await panel.getByRole("button", { name: "Connect" }).click();
    await panel.getByLabel(/API token/i).fill("test-api-key-123");
    await panel.getByRole("button", { name: "Submit" }).click();
    await expect.poll(() => connected).toBe(true);
    expect(receivedCredential).toBe("test-api-key-123");
  });

  test("manual auth Cancel hides the form", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MANUAL_INTEGRATION]);

    const panel = await openAppConnection(page, "manual-svc");
    await panel.getByRole("button", { name: "Connect" }).click();
    await expect(panel.getByLabel(/API token/i)).toBeVisible();
    await panel.getByRole("button", { name: "Cancel" }).click();
    await expect(panel.getByText("Not connected")).toHaveCount(0);
    await expect(panel.getByRole("button", { name: "Connect" })).toBeVisible();
  });

  test("multi-connection dual auth renders actions per connection", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MULTI_CONNECTION_DUAL_AUTH_INTEGRATION]);

    const panel = await openAppConnection(page, "workspace-svc");

    await expect(panel.getByRole("button", { name: "Connect with workspace" })).toBeVisible();
    await expect(panel.getByRole("button", { name: "Use API token for workspace" })).toBeVisible();
    await expect(panel.getByRole("button", { name: "Connect with personal" })).toBeVisible();
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

    const panel = await openAppConnection(page, "dual-oauth-svc");

    await expect(panel.getByRole("button", { name: "Connect with OAuth" })).toBeVisible();
    await expect(panel.getByRole("button", { name: "Connect with MCP" })).toBeVisible();
    await expect(panel.getByText("MCP passthrough", { exact: true })).toHaveCount(0);

    await panel.getByRole("button", { name: "Connect with MCP" }).click();
    await page.waitForURL("about:blank");

    expect(requestBody).toMatchObject({
      integration: "dual-oauth-svc",
      connection: "mcp",
    });
  });

  test("no-auth connections are not labeled MCP passthrough by default", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [NO_AUTH_WITH_USER_INTEGRATION]);

    await page.goto("/apps");
    const card = page.getByTestId("integration-card-no-auth-svc");
    await expect(card.getByLabel("Connected")).toHaveCount(0);
    await expect(card.getByText("Not connected")).toHaveCount(0);
    const panel = await openAppConnection(page, "no-auth-svc");

    await expect(panel.getByText("Webhook", { exact: true })).toBeVisible();
    await expect(panel.getByText("No credentials required", { exact: true })).toHaveCount(0);
    await expect(panel.getByText("MCP passthrough", { exact: true })).toHaveCount(0);
    await expect(panel.getByRole("button", { name: "Connect" })).toBeVisible();
  });

  test("explicit MCP passthrough connections keep the passthrough label", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MCP_PASSTHROUGH_INTEGRATION]);

    const panel = await openAppConnection(page, "mcp-passthrough-svc");

    await expect(panel.getByText("MCP", { exact: true })).toBeVisible();
    await expect(panel.getByText("Uses a shared connection", { exact: true })).toBeVisible();
    await expect(panel.getByRole("button", { name: /connect/i })).toHaveCount(0);
  });

  test("subject-owned connection rows expose server actions and grouped instances", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [USER_CONNECTION_ACTIONS_INTEGRATION]);

    await page.goto("/apps");
    await expect(page.getByTestId("integration-card-user-actions-svc").getByLabel("Installed")).toBeVisible();
    const panel = await openAppConnection(page, "user-actions-svc");

    await expect(panel.getByText("Workspace", { exact: true })).toBeVisible();
    await expect(panel.getByText("User credentials connected")).toHaveCount(0);
    await expect(panel.getByText("prod", { exact: true })).toBeVisible();
    await expect(panel.getByText("staging", { exact: true })).toBeVisible();
    await expect(panel.getByRole("button", { name: "Add connection" })).toBeVisible();
    await expect(panel.getByRole("button", { name: "Reconnect" })).toBeVisible();
    await expect(panel.getByRole("button", { name: "Disconnect" })).toHaveCount(2);
  });

  test("select-instance status stays on connection page without starting auth", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [SELECT_INSTANCE_INTEGRATION]);

    await page.goto("/apps");
    await expect(page.getByTestId("integration-card-select-instance-svc")).toBeVisible();
    const panel = await openAppConnection(page, "select-instance-svc");

    await expect(panel.getByText("Choose an account").first()).toBeVisible();
    await expect(panel.getByText("alpha", { exact: true })).toBeVisible();
    await expect(panel.getByText("beta", { exact: true })).toBeVisible();
    await expect(panel.getByRole("button", { name: "Select connection" })).toHaveCount(0);
    await expect(panel.getByRole("button", { name: /connect|reconnect|add connection/i })).toHaveCount(0);
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

    const panel = await openAppConnection(page, "team-svc");

    await panel.getByRole("button", { name: "Connect with personal" }).click();
    await expect(panel.getByRole("button", { name: "Connecting..." })).toBeVisible();
    await expect(panel.getByRole("button", { name: "Connect with workspace" })).toBeVisible();

    releaseOAuthRequest?.();
    await expect(panel.getByText("oauth failed")).toBeVisible();
  });

  test("manual auth reconnect opens token form on connection page", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [withConnectedConnection(MANUAL_INTEGRATION)]);

    const panel = await openAppConnection(page, "manual-svc");
    await expect(panel.getByText("default")).toBeVisible();
    await panel.getByRole("button", { name: "Add connection" }).click();
    await expect(panel.getByLabel("Connection name")).toBeVisible();
    await panel.getByLabel("Connection name").fill("second");
    await panel.getByRole("button", { name: "Continue" }).click();
    await expect(panel.getByLabel(/API token/i)).toBeVisible();
  });

  test("credential field description renders inline links", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MANUAL_WITH_LINKED_DESC]);
    await mockTokens(page, []);

    const panel = await openAppConnection(page, "linked-svc");
    await panel.getByRole("button", { name: "Connect" }).click();

    const link = panel.getByRole("link", { name: "Account Settings" });
    await expect(link).toBeVisible();
    await expect(link).toHaveAttribute("href", "https://example.com/settings");
    await expect(link).toHaveAttribute("target", "_blank");
    await expect(panel.getByText("Find yours in")).toBeVisible();
  });
});
