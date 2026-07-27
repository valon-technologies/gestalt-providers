import { test, expect, mockIntegrations, mockManualConnect, mockTokens } from "./fixtures";
import type { Integration } from "../src/lib/api";

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

async function openAppManage(
  page: import("@playwright/test").Page,
  label: string,
) {
  const add = page.getByRole("button", { name: `Add ${label}` });
  if ((await add.count()) > 0) {
    await add.click();
    return;
  }
  await page.getByRole("button", { name: `${label} options` }).click();
  await page.getByRole("menuitem", { name: "Manage" }).click();
}

async function openAppUninstall(
  page: import("@playwright/test").Page,
  label: string,
) {
  await page.getByRole("button", { name: `${label} options` }).click();
  await page.getByRole("menuitem", { name: "Uninstall" }).click();
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
    await expect(page.getByRole("button", { name: "Add Another Service" })).toBeVisible();
    await expect(page.getByRole("button", { name: "OAuth Service options" })).toHaveCount(0);
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

  test("shows empty state when no apps", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, []);
    await mockTokens(page, []);

    await page.goto("/apps");
    await expect(
      page.getByText(
        "No apps are available yet. Ask your admin if you expected to see ones here.",
      ),
    ).toBeVisible();
  });

  test("mounted ui cards navigate to app admin", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MOUNTED_UI_INTEGRATION]);
    await mockTokens(page, []);

    await page.goto("/apps");
    await expect(page.getByRole("button", { name: "Add Mounted UI Service" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Mounted UI Service options" })).toHaveCount(0);
    await expect(
      page.getByTestId("integration-card-mounted-ui-svc").getByText("App"),
    ).toBeVisible();

    await page.getByTestId("integration-card-mounted-ui-svc").click();

    await page.waitForURL("**/apps/mounted-ui-svc");
    await expect(
      page.getByRole("heading", { level: 1, name: "Mounted UI Service" }),
    ).toBeVisible();
    await expect(page.getByRole("navigation", { name: "breadcrumb" })).toBeVisible();
    await expect(
      page.getByRole("navigation", { name: "breadcrumb" }).getByRole("link", { name: "Apps" }),
    ).toBeVisible();
    await expect(
      page
        .getByRole("navigation", { name: "breadcrumb" })
        .getByRole("link", { name: "Mounted UI Service" }),
    ).toHaveAttribute("aria-current", "page");
  });

  test("mounted ui options menu does not trigger navigation", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MOUNTED_UI_WITH_SETTINGS_INTEGRATION]);
    await mockTokens(page, []);

    await page.goto("/apps");
    await openAppManage(page, "Mounted UI With Settings");

    await expect(page.getByRole("dialog")).toBeVisible();
    await expect(page).toHaveURL(/\/apps$/);
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

    await expect(page.getByText('No apps match "missing-plugin". Try a different search, or clear it.')).toBeVisible();
    await expect(page.getByRole("button", { name: "Clear search" })).toBeVisible();
    await expect(page.getByTestId("plugin-grid")).toHaveCount(0);
  });

  test("attention apps sort first with a callout; all setup states remain visible", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      MANUAL_INTEGRATION,
      withConnectedConnection(OAUTH_INTEGRATION),
      SELECT_INSTANCE_INTEGRATION,
    ]);

    await page.goto("/apps");

    await expect(
      page.getByRole("radiogroup", { name: "Filter by connection status" }),
    ).toHaveCount(0);
    await expect(page.getByRole("radio", { name: "All" })).toHaveCount(0);
    await expect(page.getByRole("radio", { name: "To connect" })).toHaveCount(0);
    await expect(page.getByRole("radio", { name: "Ready" })).toHaveCount(0);
    await expect(page.getByRole("radio", { name: "Needs fix" })).toHaveCount(0);

    await expect(page.getByTestId("apps-needs-attention-callout")).toBeVisible();
    await expect(page.getByTestId("apps-needs-attention-callout")).toContainText(
      "1 app needs attention",
    );

    await expect(page.getByTestId("catalog-bucket-installed")).toBeVisible();
    await expect(
      page.getByTestId("catalog-bucket-installed").getByText("OAuth Service", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByTestId("integration-card-select-instance-svc"),
    ).toBeVisible();
    await expect(page.getByTestId("integration-card-manual-svc")).toBeVisible();

    await page.getByTestId("integration-card-manual-svc").click();
    await expect(page.getByTestId("app-listing-detail-manual-svc")).toBeVisible();
    await expect(
      page.getByTestId("app-listing-detail-manual-svc").getByRole("button", { name: "Connect" }),
    ).toBeVisible();
  });

  test("filters the grid and highlights matching tokens", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, sampleIntegrations);

    await page.goto("/apps");
    const search = page.getByRole("searchbox", { name: "Search apps" });
    const grid = page.getByTestId("plugin-grid");

    await search.fill("oauth");

    await expect(search).toHaveValue("oauth");
    await expect(grid.getByText("OAuth Service", { exact: true })).toBeVisible();
    await expect(grid.locator("mark", { hasText: "OAuth" })).toBeVisible();
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

  test("connected integration shows installed check and options menu", async ({
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
    await expect(
      page.getByTestId("integration-card-oauth-svc").getByLabel("Installed"),
    ).toBeVisible();
    await expect(
      page.getByTestId("integration-card-manual-svc").getByLabel("Installed"),
    ).toHaveCount(0);
    await expect(page.getByRole("button", { name: "OAuth Service options" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Add Manual Service" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Manual Service options" })).toHaveCount(0);
    await expect(
      page.getByRole("button", { name: "Open for OAuth Service" }),
    ).toHaveCount(0);
    await expect(
      page.getByRole("button", { name: "View details for Manual Service" }),
    ).toHaveCount(0);

    // Manage opens settings with Reconnect/Disconnect
    await openAppManage(page, "OAuth Service");
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByText("default")).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Add Instance" })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Disconnect" })).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();

    // Card click opens listing, then Connect opens settings
    await page.getByTestId("integration-card-manual-svc").click();
    const listing = page.getByTestId("app-listing-detail-manual-svc");
    await expect(listing).toBeVisible();
    await listing.getByRole("button", { name: "Connect" }).click();
    await expect(page.getByRole("dialog").getByRole("button", { name: "Connect" })).toBeVisible();
  });

  test("disconnect confirmation shows warning and allows cancel", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [
      withConnectedConnection(OAUTH_INTEGRATION),
    ]);

    await page.goto("/apps");
    await openAppManage(page, "OAuth Service");

    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Disconnect" }).click();

    await expect(dialog.getByText("Disconnect OAuth Service?")).toBeVisible();
    await expect(
      dialog.getByText(
        "This will remove your connection to OAuth Service. You can reconnect at any time.",
      ),
    ).toBeVisible();

    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(dialog.getByRole("button", { name: "Add Instance" })).toBeVisible();
  });

  test("uninstall from options menu calls API and refreshes list", async ({
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

    await page.goto("/apps");
    await expect(page.getByRole("button", { name: "OAuth Service options" })).toBeVisible();

    // Re-mock so GET returns disconnected state after DELETE fires
    await page.route("**/api/v1/apps", (route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: disconnected ? disconnectedList : connectedList });
      } else {
        route.fallback();
      }
    });

    await openAppUninstall(page, "OAuth Service");
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByText("Uninstall OAuth Service?")).toBeVisible();
    await dialog.getByRole("button", { name: "Uninstall" }).click();

    await expect(page.getByRole("dialog")).not.toBeVisible();
    await expect(page.getByRole("button", { name: "Add OAuth Service" })).toBeVisible();
    await expect(page.getByRole("button", { name: "OAuth Service options" })).toHaveCount(0);
    await openAppManage(page, "OAuth Service");
    await expect(page.getByRole("dialog").getByText("Not connected")).toHaveCount(0);
    await expect(page.getByRole("dialog").getByRole("button", { name: "Connect" })).toBeVisible();
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

    await page.goto("/apps");
    await openAppManage(page, "Manual Service");
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Connect" }).click();
    await dialog.getByLabel(/API token/i).fill("test-api-key-123");

    await page.route("**/api/v1/apps", (route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ json: connected ? connectedList : disconnectedList });
      } else {
        route.fallback();
      }
    });

    await dialog.getByRole("button", { name: "Submit" }).click();
    await expect(page.getByRole("button", { name: "Manual Service options" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Add Manual Service" })).toHaveCount(0);
    expect(receivedCredential).toBe("test-api-key-123");
  });

  test("manual auth Cancel hides the form", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MANUAL_INTEGRATION]);

    await page.goto("/apps");
    await openAppManage(page, "Manual Service");
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Connect" }).click();
    await expect(dialog.getByLabel(/API token/i)).toBeVisible();
    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(dialog.getByText("Not connected")).toHaveCount(0);
    await expect(dialog.getByRole("button", { name: "Connect" })).toBeVisible();
  });

  test("multi-connection dual auth renders actions per connection", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MULTI_CONNECTION_DUAL_AUTH_INTEGRATION]);

    await page.goto("/apps");
    await openAppManage(page, "Workspace Service");
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

    await page.goto("/apps");
    await openAppManage(page, "Dual OAuth Service");
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

  test("no-auth connections are not labeled MCP passthrough by default", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [NO_AUTH_WITH_USER_INTEGRATION]);

    await page.goto("/apps");
    const card = page.getByTestId("integration-card-no-auth-svc");
    await expect(card.getByLabel("Ready", { exact: true })).toHaveCount(0);
    await expect(card.getByText("Not connected")).toHaveCount(0);
    await expect(card.getByLabel("Installed")).toHaveCount(0);
    await openAppManage(page, "No Auth Service");
    const dialog = page.getByRole("dialog");

    await expect(dialog.getByText("Webhook", { exact: true })).toBeVisible();
    await expect(dialog.getByText("No credentials required", { exact: true })).toHaveCount(0);
    await expect(dialog.getByText("MCP passthrough", { exact: true })).toHaveCount(0);
    await expect(dialog.getByText("Uses a shared connection", { exact: true })).toHaveCount(0);
    await expect(dialog.getByRole("button", { name: "Connect" })).toBeVisible();
  });

  test("explicit MCP passthrough connections keep the shared-connection label", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MCP_PASSTHROUGH_INTEGRATION]);

    await page.goto("/apps");
    await openAppManage(page, "MCP Passthrough Service");
    const dialog = page.getByRole("dialog");

    await expect(dialog.getByText("MCP", { exact: true })).toBeVisible();
    await expect(dialog.getByText("Uses a shared connection", { exact: true })).toBeVisible();
    await expect(dialog.getByRole("button", { name: /connect/i })).toHaveCount(0);
  });

  test("subject-owned connection rows expose server actions and grouped instances", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [USER_CONNECTION_ACTIONS_INTEGRATION]);

    await page.goto("/apps");
    await expect(
      page.getByTestId("integration-card-user-actions-svc").getByLabel("Installed"),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "View details for User Actions Service" }),
    ).toHaveCount(0);
    await openAppManage(page, "User Actions Service");
    const dialog = page.getByRole("dialog");

    await expect(dialog.getByText("Workspace", { exact: true })).toBeVisible();
    await expect(dialog.getByText("User credentials connected")).toHaveCount(0);
    await expect(dialog.getByText("prod", { exact: true })).toBeVisible();
    await expect(dialog.getByText("staging", { exact: true })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Add Instance" })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Reconnect" })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Disconnect" })).toHaveCount(2);
  });

  test("select-instance status stays in settings without starting auth", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [SELECT_INSTANCE_INTEGRATION]);

    await page.goto("/apps");
    const card = page.getByTestId("integration-card-select-instance-svc");
    await expect(card.getByText("Choose an account")).toBeVisible();
    await openAppManage(page, "Select Instance Service");
    const dialog = page.getByRole("dialog");

    await expect(dialog.getByText("Choose an account").first()).toBeVisible();
    await expect(dialog.getByText("alpha", { exact: true })).toBeVisible();
    await expect(dialog.getByText("beta", { exact: true })).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Select Instance" })).toHaveCount(0);
    await expect(dialog.getByRole("button", { name: /connect|reconnect|add instance/i })).toHaveCount(0);
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

    await page.goto("/apps");
    await openAppManage(page, "Team Service");
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
    await mockIntegrations(page, [withConnectedConnection(MANUAL_INTEGRATION)]);

    await page.goto("/apps");
    await openAppManage(page, "Manual Service");
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByText("default")).toBeVisible();
    await dialog.getByRole("button", { name: "Add Instance" }).click();
    await expect(dialog.getByLabel("Connection name")).toBeVisible();
    await dialog.getByLabel("Connection name").fill("second");
    await dialog.getByRole("button", { name: "Continue" }).click();
    await expect(dialog.getByLabel(/API token/i)).toBeVisible();
  });

  test("credential field description renders inline links", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await mockIntegrations(page, [MANUAL_WITH_LINKED_DESC]);
    await mockTokens(page, []);

    await page.goto("/apps");
    await openAppManage(page, "Linked Service");
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Connect" }).click();

    const link = dialog.getByRole("link", { name: "Account Settings" });
    await expect(link).toBeVisible();
    await expect(link).toHaveAttribute("href", "https://example.com/settings");
    await expect(link).toHaveAttribute("target", "_blank");
    await expect(dialog.getByText("Find yours in")).toBeVisible();
  });
});
