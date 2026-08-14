import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrations,
  mockIntegrationOperations,
  mockPersonalTokenCreate,
  mockTokens,
  enableSetupActivationPrompt,
  seedSetupSession,
} from "./fixtures";
import type { Page } from "@playwright/test";

const catalogFixtures = [
  {
    name: "slack",
    displayName: "Slack",
    description:
      "Read public and private conversations, DMs, and group DMs; send messages; and manage channels.",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>',
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "pagerduty",
    displayName: "PagerDuty",
    description: "Manage incidents, services, and on-call schedules.",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>',
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "linear",
    displayName: "Linear",
    description: "Manage issues, projects, and teams.",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M4 4h16v16H4z"/></svg>',
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "ashby",
    displayName: "Ashby",
    description:
      "Candidates, applications, jobs, offers, interviews, departments, locations, users, and reports.",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="8"/></svg>',
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "intercom",
    displayName: "Intercom",
    description: "Read and update contacts, companies, conversations, and notes.",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="5" y="5" width="14" height="14" rx="2"/></svg>',
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "gmail",
    displayName: "Gmail",
    description: "Read, send, and manage Gmail messages, threads, drafts, and labels.",
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "github",
    displayName: "GitHub",
    description:
      "Repository, issue, pull request, workflow, and code search operations.",
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "notion",
    displayName: "Notion",
    description: "Current Notion REST operations plus the official Notion MCP tool surface.",
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "figma",
    displayName: "Figma",
    description: "Access files, components, comments, and team projects.",
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "jira",
    displayName: "Jira",
    description: "Atlassian Jira Cloud project and issue management",
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "looker",
    displayName: "Looker",
    description:
      "Run queries, manage dashboards, looks, folders, users, groups, schedules, projects, content validation, and instance configuration in Looker.",
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "zendesk",
    displayName: "Zendesk",
    description:
      "Support tickets, users, organizations, macros, automations, triggers, and SLA policies.",
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "aiSpendTracker",
    displayName: "AI Spend Tracker",
    description: "Personal AI spend",
    mountedPath: "/ai-spend",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="8"/></svg>',
    credentialState: "connected" as const,
    status: "ready" as const,
  },
  {
    name: "oncall",
    displayName: "Oncall",
    description: "Oncall",
    mountedPath: "/oncall",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M2 6l10 7 10-7v12H2z"/></svg>',
    credentialState: "connected" as const,
    status: "ready" as const,
  },
  {
    name: "servicingQuiz",
    displayName: "Servicing Quiz",
    description: "SATs",
    mountedPath: "/servicing-quiz",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12"/></svg>',
    credentialState: "connected" as const,
    status: "ready" as const,
  },
];

const defaultToken = {
  id: "tok_123",
  name: "Default token",
  scopes: ["api"],
  createdAt: "2026-04-13T00:00:00Z",
};

function withConnectedConnection<T extends { name: string }>(item: T) {
  return {
    ...item,
    credentialState: "connected" as const,
    status: "ready" as const,
    connections: [
      {
        name: item.name,
        connected: true,
        credentialState: "connected" as const,
        status: "ready" as const,
      },
    ],
  };
}

async function expectSetupStepper(page: Page) {
  const stepper = page.locator('[data-slot="stepper"]');
  await expect(stepper).toHaveAttribute("data-orientation", "horizontal");
  await expect(stepper).toHaveAttribute("data-completed-chrome", "outcome");
  await expect(stepper).toHaveAttribute("data-size", "default");
}

test.describe("Setup page", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(authenticatedPage, catalogFixtures);
    await mockTokens(authenticatedPage, []);
  });

  test("redirects /setup to welcome storytelling", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/setup");

    await expect(page).toHaveURL(/\/setup\/welcome$/);
    await expect(
      page.getByRole("heading", {
        name: "Your AI assistant, wired into your work",
        exact: true,
      }),
    ).toBeVisible();
    await expect(
      page.locator('[data-slot="page-header"] [data-slot="eyebrow"]'),
    ).toHaveCount(0);
    await expect(
      page
        .getByTestId("build-welcome")
        .locator('[data-slot="page-header-content"]'),
    ).toHaveAttribute("data-size", "md");

    await expect(page.getByTestId("build-welcome")).toBeVisible();
    await expect(page.getByText(/About 5 minutes/)).toBeVisible();
    await expect(page.getByText(/you choose which apps/i)).toBeVisible();
    await expect(page.getByText(/\bMCP\b/)).toHaveCount(0);

    await expect(page.getByTestId("build-nav-welcome")).toBeVisible();
    await expect(page.getByTestId("build-step-nav")).toHaveAttribute(
      "data-orientation",
      "horizontal",
    );
    await expectSetupStepper(page);
    await expect(page.getByTestId("build-nav-assistant")).toBeVisible();
    await expect(page.getByTestId("build-nav-token")).toBeVisible();
    await expect(page.getByTestId("build-nav-install")).toBeVisible();
    await expect(page.getByTestId("build-nav-install")).toContainText(
      "Add Gestalt",
    );
    await expect(page.getByTestId("build-nav-install")).not.toContainText(
      "to your assistant",
    );
    await expect(page.getByTestId("build-nav-apps")).toBeVisible();
    await expect(page.getByTestId("build-nav-try")).toBeVisible();

    await expect(page.getByLabel("Token name")).toHaveCount(0);
  });

  test("welcome maps token load failures to an alert instead of rpc text", async ({
    authenticatedPage: page,
  }) => {
    await page.route("**/api/v2/identity/grants", async (route) => {
      if (route.request().method() === "GET") {
        await route.fulfill({
          status: 500,
          json: {
            error:
              "list grants: rpc error: code = Unknown desc = list grants: oidc auth: caller bearer token is required",
          },
        });
        return;
      }
      await route.fallback();
    });

    await page.goto("/setup/welcome");
    const notice = page.getByTestId("error-notice");
    await expect(notice).toBeVisible();
    await expect(notice).toContainText("Couldn't load this workspace");
    await expect(notice).not.toContainText("rpc error");
    await expect(notice).not.toContainText("bearer token");
  });

  test("legacy /build redirects to /setup", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/build");
    await expect(page).toHaveURL(/\/setup\/welcome$/);
  });

  test("continue from welcome opens assistant picker", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/setup/welcome");
    await page.getByTestId("build-welcome-continue").click();
    await expect(page).toHaveURL(/\/setup\/assistant$/);
    await expect(page.getByTestId("build-install-radio")).toBeVisible();
    await expect(page.getByTestId("build-step-next")).toBeDisabled();
    await page.getByTestId("build-install-card-cursor").click();
    await expect(page.getByTestId("build-step-next")).toBeEnabled();
    await expect(page.getByTestId("build-step-next")).toContainText(
      "Create an API token",
    );
  });

  test("resume after welcome opens assistant when none is chosen", async ({
    authenticatedPage: page,
  }) => {
    await seedSetupSession(page, { introSeen: true });
    await page.goto("/setup");
    await expect(page).toHaveURL(/\/setup\/assistant$/);
    await expect(page.getByTestId("build-install-radio")).toBeVisible();
    await expect(page.getByTestId("build-step-next")).toBeDisabled();
  });

  test("token and install are separate steps", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await seedSetupSession(page, {
      introSeen: true,
      installAgent: "cursor",
      selectedTokenId: "tok_123",
    });

    await page.goto("/setup");

    await expect(page).toHaveURL(/\/setup\/install$/);
    await expect(page.getByTestId("build-token-radio")).toHaveCount(0);
    await expect(page.getByTestId("build-mcp-install-single")).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Add Gestalt in Cursor" }),
    ).toBeVisible();
    await expect(
      page.getByText("Connect Cursor so it can use your Gestalt apps."),
    ).toBeVisible();
    await expect(page.getByTestId("build-install-cursor-method")).toBeVisible();
    await expect(page.getByRole("radio", { name: "Open Cursor" })).toBeChecked();
    await expect(page.getByTestId("build-add-to-cursor")).toBeEnabled();
    await expect(page.getByTestId("build-add-to-cursor")).toHaveAttribute(
      "href",
      /cursor:\/\/anysphere\.cursor-deeplink\/mcp\/install/,
    );
    await expect(page.getByTestId("build-add-to-cursor")).toHaveText(
      "Add in Cursor",
    );
    await page.getByText("Paste the config yourself").click();
    await expect(page.getByText(".cursor/mcp.json")).toBeVisible();
    await expect(page.getByRole("link", { name: "setup notes" })).toBeVisible();

    await page.goto("/setup/token");
    await expect(page.getByTestId("build-token-radio")).toBeVisible();
    await expect(page.getByTestId("build-mcp-install-single")).toHaveCount(0);
    await expect(
      page.getByRole("heading", { name: "Create an API token" }),
    ).toBeVisible();
  });

  test("legacy /setup/connect redirects to the token step", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await seedSetupSession(page, {
      introSeen: true,
      installAgent: "cursor",
    });

    await page.goto("/setup/connect");
    await expect(page).toHaveURL(/\/setup\/token$/);
    await expect(page.getByTestId("build-token-radio")).toBeVisible();
  });

  test("MCP snippets reflect assistant choice on install", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await seedSetupSession(page, {
      introSeen: true,
      selectedTokenId: "tok_123",
    });

    await page.goto("/setup/assistant");
    await page.getByTestId("build-install-card-claude").click();
    await page.getByTestId("build-step-next").click();
    await expect(page).toHaveURL(/\/setup\/token$/);
    await page.getByTestId("build-step-next").click();
    await expect(page).toHaveURL(/\/setup\/install$/);
    await expect(
      page.getByRole("heading", { name: "Add Gestalt in Claude Code" }),
    ).toBeVisible();
    await expect(page.getByTestId("build-install-claude-snippet")).toBeVisible();

    await page.goto("/setup/assistant");
    await page.getByTestId("build-install-card-codex").click();
    await page.getByTestId("build-step-next").click();
    await expect(page).toHaveURL(/\/setup\/token$/);
    await page.getByTestId("build-step-next").click();
    await expect(page.getByRole("heading", { name: "Add Gestalt in Codex" })).toBeVisible();
    await expect(page.getByTestId("build-install-codex-snippet")).toBeVisible();
  });

  test("step pager advances from token create to install", async ({
    authenticatedPage: page,
  }) => {
    await seedSetupSession(page, {
      introSeen: true,
      installAgent: "cursor",
    });

    const tokens = await mockTokens(page, []);
    await mockPersonalTokenCreate(page, tokens, async (body) => {
      expect(body).toEqual({
        name: "ci-pipeline",
        scopes: "",
        expiresIn: 30 * 24 * 60 * 60,
      });
      return {
        token: {
          id: "tok_new",
          name: body.name,
          scopes: [],
          createdAt: "2026-07-21T00:00:00Z",
        },
        plaintext: "gst_api_created_once_secret",
      };
    });

    await page.goto("/setup/token");

    await expect(
      page.locator("label").filter({ hasText: "Use existing token" }),
    ).toHaveCount(0);
    await expect(page.getByLabel("Token name")).toBeVisible();
    await expect(page.getByLabel("Token name")).toHaveValue(
      "Workspace assistant",
    );
    await page.getByLabel("Token name").fill("ci-pipeline");
    await expect(page.getByRole("button", { name: "Create token" })).toBeVisible();
    await expect(page.getByTestId("build-step-next")).toBeDisabled();
    await page.getByRole("button", { name: "Create token" }).click();
    await expect(page.getByText("Token created.")).toBeVisible();
    await expect(page.getByRole("radio", { name: "Use existing token" })).toBeChecked();
    await expect(page.getByTestId("build-existing-token-list")).toBeVisible();
    await expect(
      page.getByRole("radio", { name: /ci-pipeline \(tok_new\)/ }),
    ).toBeChecked();
    await expect(page.getByTestId("build-step-next")).toBeEnabled();
    await expect(page.getByTestId("build-step-next")).toContainText(
      "Add Gestalt in Cursor",
    );
    await page.getByTestId("build-step-next").click();

    await expect(page).toHaveURL(/\/setup\/install$/);
    await expect(page.getByTestId("build-step-prev")).toContainText(
      "Create an API token",
    );
    await expect(page.getByTestId("build-mcp-install-single")).toBeVisible();
    await page.getByTestId("build-step-next").click();
    await expect(page).toHaveURL(/\/setup\/apps$/);
    await expect(page.getByTestId("build-step-prev")).toContainText(
      "Add Gestalt in Cursor",
    );
    await expect(page.getByTestId("build-connect-apps")).toBeVisible();
    await expect(page.getByTestId("build-step-next")).toBeDisabled();
  });

  test("creating a token selects it under Use existing when the server list omits it", async ({
    authenticatedPage: page,
  }) => {
    await seedSetupSession(page, {
      introSeen: true,
      installAgent: "cursor",
    });

    const hello = {
      id: "grant-legacy-3e5276ee-671b-42e0-9093-971216489eaf",
      name: "Hello",
      scopes: ["api"],
      createdAt: "2026-04-13T00:00:00Z",
    };
    const tokens = await mockTokens(page, [hello]);
    await mockPersonalTokenCreate(
      page,
      tokens,
      async (body) => ({
        token: {
          id: "tok_new",
          name: body.name ?? "created",
          scopes: [],
          createdAt: "2026-08-14T00:00:00Z",
        },
        plaintext: "gst_api_created_once_secret",
      }),
      { listCreated: false },
    );

    await page.goto("/setup/token");
    await page.locator("label").filter({ hasText: "Create new token" }).click();
    await page.getByLabel("Token name").fill("Workspace assistant");
    await expect(page.getByTestId("build-step-next")).toBeDisabled();
    await page.getByRole("button", { name: "Create token" }).click();
    await expect(page.getByText("Token created.")).toBeVisible();

    await expect(
      page.getByRole("radio", { name: "Use existing token" }),
    ).toBeChecked();
    await expect(page.getByTestId("build-existing-token-list")).toBeVisible();
    await expect(
      page.getByRole("radio", { name: "Workspace assistant (tok_new)" }),
    ).toBeChecked();
    await expect(
      page.getByRole("radio", {
        name: "Hello (grant-legacy-3e5276ee-671b-42e0-9093-971216489eaf)",
      }),
    ).not.toBeChecked();
    await expect(page.getByTestId("build-step-next")).toBeEnabled();
    await expect(page.getByTestId("build-step-next")).toContainText(
      "Add Gestalt in Cursor",
    );
  });

  test("apps step prompts to connect catalog apps", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await seedSetupSession(page, {
      introSeen: true,
      selectedTokenId: "tok_123",
      mcpInstalled: true,
      installAgent: "claude",
      activeExemplarId: "oncall",
    });

    await page.goto("/setup");

    await expect(page).toHaveURL(/\/setup\/apps$/);
    await expect(
      page.getByRole("heading", { name: "Connect apps" }),
    ).toBeVisible();
    await expect(
      page.locator('[data-slot="page-header"] [data-slot="eyebrow"]'),
    ).toHaveCount(0);
    await expect(page.getByTestId("build-connect-apps")).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Suggested" }),
    ).toBeVisible();
    await expect(page.getByTestId("build-connect-app-pagerduty")).toBeVisible();
    await expect(page.getByTestId("build-connect-app-linear")).toBeVisible();
    await expect(page.getByTestId("build-connect-app-slack")).toBeVisible();
    await expect(page.getByTestId("build-connect-app-slack")).toContainText(
      "Read public and private conversations, DMs, and group DMs; send messages; and manage channels.",
    );
    await expect(page.getByTestId("build-apps-category-chips")).toBeVisible();
    await expect(page.getByTestId("build-apps-category-all")).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "More apps" }),
    ).toBeVisible();
    await expect(page.getByTestId("build-connect-app-ashby")).toBeVisible();
    await expect(page.getByTestId("build-connect-app-gmail")).toBeVisible();
    await expect(page.getByTestId("build-connect-app-zendesk")).toHaveCount(0);
    await expect(page.getByTestId("build-see-more-apps")).toContainText(
      "See Zendesk",
    );
    await expect(page.getByTestId("build-connect-app-oncall")).toHaveCount(0);
    await expect(
      page.getByTestId("build-connect-app-aiSpendTracker"),
    ).toHaveCount(0);
    await expect(
      page.getByTestId("build-connect-app-servicingQuiz"),
    ).toHaveCount(0);
    await expect(page.getByTestId("build-step-next")).toBeDisabled();

    await page.getByTestId("build-apps-category-communication").click();
    await expect(
      page.getByRole("heading", { name: "Communication" }),
    ).toBeVisible();
    await expect(page.getByTestId("build-connect-app-gmail")).toBeVisible();
    await expect(page.getByTestId("build-connect-app-ashby")).toHaveCount(0);
    await expect(page.getByTestId("build-connect-app-slack")).toBeVisible();
    await expect(page.getByTestId("build-see-more-apps")).toHaveCount(0);

    await page.getByTestId("build-apps-category-all").click();
    await expect(
      page.getByRole("heading", { name: "More apps" }),
    ).toBeVisible();
    await expect(page.getByTestId("build-see-more-apps")).toContainText(
      "See Zendesk",
    );
    await page.getByTestId("build-see-more-apps").click();
    await expect(page.getByTestId("build-connect-app-zendesk")).toBeVisible();
    await expect(page.getByTestId("build-see-more-apps")).toHaveCount(0);
  });

  test("failed catalog does not skip the apps step", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await page.route("**/api/v1/apps", async (route) => {
      if (route.request().method() === "GET") {
        await route.fulfill({
          status: 500,
          json: { error: "catalog unavailable" },
        });
        return;
      }
      await route.fallback();
    });
    await seedSetupSession(page, {
      introSeen: true,
      installAgent: "cursor",
      selectedTokenId: "tok_123",
      mcpInstalled: true,
    });

    await page.goto("/setup");

    await expect(page).toHaveURL(/\/setup\/apps$/);
    await expect(page.getByTestId("error-notice")).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Connect apps" }),
    ).toBeVisible();
  });

  test("single-option connect starts OAuth; multiple options open the chooser", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await mockIntegrations(
      page,
      catalogFixtures.map((item) => {
        if (item.name === "slack") {
          return {
            ...item,
            connections: [
              {
                name: "default",
                authTypes: ["oauth" as const],
                credentialState: "missing" as const,
                status: "needs_user_connection" as const,
              },
            ],
          };
        }
        if (item.name === "github") {
          return {
            ...item,
            connections: [
              {
                name: "oauth",
                displayName: "OAuth",
                authTypes: ["oauth" as const],
              },
              {
                name: "mcp",
                displayName: "MCP",
                authTypes: ["oauth" as const],
              },
            ],
          };
        }
        return item;
      }),
    );
    let oauthBody:
      | { integration: string; connection?: string }
      | undefined;
    await page.route("**/api/v1/auth/start-oauth", async (route) => {
      oauthBody = route.request().postDataJSON() as {
        integration: string;
        connection?: string;
      };
      await route.fulfill({
        json: { url: "about:blank", state: "state-123" },
      });
    });
    await seedSetupSession(page, {
      introSeen: true,
      selectedTokenId: "tok_123",
      mcpInstalled: true,
      installAgent: "claude",
      activeExemplarId: "oncall",
    });

    await page.goto("/setup/apps");

    await page.getByRole("button", { name: "Connect GitHub" }).click();
    const chooser = page.getByTestId("integration-connection-github");
    await expect(chooser).toBeVisible();
    await expect(
      chooser.getByRole("button", { name: "Connect with OAuth" }),
    ).toBeVisible();
    await expect(
      chooser.getByRole("button", { name: "Connect with MCP" }),
    ).toBeVisible();
    await chooser.getByRole("button", { name: "Close" }).click();
    await expect(chooser).toHaveCount(0);

    await page.getByRole("button", { name: "Connect Slack" }).click();
    await expect(
      page.getByTestId("integration-connection-slack"),
    ).toHaveCount(0);
    await page.waitForURL("about:blank");
    expect(oauthBody).toMatchObject({
      integration: "slack",
      connection: "default",
    });
  });

  test("try step shows proof for Oncall exemplar", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await seedSetupSession(page, {
      introSeen: true,
      selectedTokenId: "tok_123",
      mcpInstalled: true,
      installAgent: "claude",
      activeExemplarId: "oncall",
    });

    await page.goto("/setup/try");

    await expect(page.getByTestId("build-golden-prompt")).toBeVisible();
    await expect(page.getByTestId("build-agent-console-reply")).toHaveAttribute(
      "data-agent-skin",
      "claude",
    );
    await expect(page.getByTestId("build-connect-apps")).toHaveCount(0);
  });

  test("try step terminal matches Codex when Codex was chosen", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await seedSetupSession(page, {
      introSeen: true,
      selectedTokenId: "tok_123",
      mcpInstalled: true,
      installAgent: "codex",
      activeExemplarId: "oncall",
    });

    await page.goto("/setup/try");

    await expect(page.getByTestId("build-agent-console-reply")).toHaveAttribute(
      "data-agent-skin",
      "codex",
    );
    await expect(
      page.locator('[data-slot="agent-console-window-title"]'),
    ).toHaveText("codex");
  });

  test("shows try invoke proof after companions connected for AI Spend Tracker", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [
      {
        ...catalogFixtures.find((item) => item.name === "slack")!,
        credentialState: "connected",
        status: "ready",
      },
      ...catalogFixtures.filter((item) => item.name !== "slack"),
    ]);
    await mockTokens(page, [defaultToken]);
    await mockIntegrationOperations(page, {
      aiSpendTracker: [
        {
          id: "getMyUsage",
          title: "Get my usage",
          description: "Returns personal AI coding spend for a time window.",
          readOnly: true,
        },
      ],
    });
    await seedSetupSession(page, {
      introSeen: true,
      selectedTokenId: "tok_123",
      mcpInstalled: true,
      installAgent: "cursor",
      activeExemplarId: "aiSpendTracker",
    });

    await page.goto("/setup/try");

    await expect(page.getByTestId("build-golden-prompt")).toBeVisible();
    await expect(
      page.getByText(/Prompt your favorite LLM with/),
    ).toBeVisible();
    await expect(page.getByTestId("build-invoke-operation")).toContainText(
      "aiSpendTracker.getMyUsage",
    );
    await expect(page.getByTestId("build-agent-console-reply")).toBeVisible();
    await expect(page.getByTestId("build-agent-console-reply")).toHaveAttribute(
      "data-agent-skin",
      "cursor",
    );
    await expect(page.getByTestId("build-open-exemplar")).toHaveAttribute(
      "href",
      "/ai-spend",
    );
    await expect(page.getByTestId("build-step-next")).toHaveAttribute(
      "href",
      "/apps",
    );
    await expect(page.getByTestId("build-step-next")).toContainText(
      "Browse apps",
    );
  });

  test("token step lists tokens as radios and supports create new", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [
      {
        ...defaultToken,
        createdAt: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
      },
    ]);
    await seedSetupSession(page, {
      introSeen: true,
      installAgent: "cursor",
    });

    await page.goto("/setup/token");

    await expect(page.getByTestId("build-token-radio")).toBeVisible();
    await page.locator("label").filter({ hasText: "Use existing token" }).click();
    await expect(page.getByTestId("build-existing-token-list")).toBeVisible();
    await expect(page.getByText("Default token")).toBeVisible();
    await expect(page.getByText("tok_123")).toBeVisible();
    await page.locator("label").filter({ hasText: "Create new token" }).click();
    await expect(page.getByLabel("Token name")).toBeVisible();
    await expect(page.getByLabel("Token name")).toHaveValue(
      "Workspace assistant",
    );
    await expect(page.getByText("Expiration", { exact: true })).toBeVisible();
    await expect(page.getByText("App access", { exact: true })).toBeVisible();
    await expect(page.getByRole("button", { name: "Create token" })).toBeVisible();
    await expect(page.getByTestId("build-step-next")).toBeDisabled();
  });

  test("create-token draft does not inherit a grant id", async ({
    authenticatedPage: page,
  }) => {
    const grantId = "grant-legacy-3e5276ee-671b-42e0-9093-971216489eaf";
    await mockTokens(page, [
      {
        id: grantId,
        createdAt: "2026-04-13T00:00:00Z",
      },
    ]);
    await seedSetupSession(page, {
      introSeen: true,
      installAgent: "cursor",
      selectedTokenId: grantId,
      tokenName: grantId,
      bindCredential: false,
    });

    await page.goto("/setup/token");
    await page.locator("label").filter({ hasText: "Create new token" }).click();
    await expect(page.getByLabel("Token name")).toHaveValue(
      "Workspace assistant",
    );
    await expect(page.getByLabel("Token name")).not.toHaveValue(grantId);
  });

  test("skip for now stays on apps and does not auto-prompt again", async ({
    authenticatedPage: page,
  }) => {
    await enableSetupActivationPrompt(page);
    await page.goto("/setup/welcome");
    await page.getByTestId("build-welcome-skip").click();
    await expect(page).toHaveURL(/\/apps/);
    await expect(page.getByRole("heading", { name: "Apps" })).toBeVisible();
    await expect(page.getByTestId("setup-resume-banner")).toBeVisible();
    await page.goto("/apps");
    await expect(page).toHaveURL(/\/apps/);
    await expect(page.getByRole("heading", { name: "Apps" })).toBeVisible();
  });

  test("empty workspace is soft-forced to setup welcome once", async ({
    authenticatedPage: page,
  }) => {
    await enableSetupActivationPrompt(page);
    await page.goto("/apps");
    await expect(page).toHaveURL(/\/setup\/welcome$/);
    await expect(page.getByTestId("build-welcome")).toBeVisible();
  });

  test("warm workspace stays on apps without setup redirect", async ({
    authenticatedPage: page,
  }) => {
    await enableSetupActivationPrompt(page);
    await mockTokens(page, [defaultToken]);
    await page.goto("/apps");
    await expect(page).toHaveURL(/\/apps/);
    await expect(page.getByRole("heading", { name: "Apps" })).toBeVisible();
    await expect(page.getByTestId("setup-resume-banner")).toHaveCount(0);
  });

  test("completed setup shows overview instead of welcome", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await seedSetupSession(page, {
      introSeen: true,
      installAgent: "cursor",
      mcpInstalled: true,
      selectedTokenId: "tok_123",
      activeExemplarId: "aiSpendTracker",
      trySeen: true,
    });
    await mockIntegrations(
      page,
      catalogFixtures.map((item) =>
        item.name === "slack" ? withConnectedConnection(item) : item,
      ),
    );

    await page.goto("/setup");
    await expect(page.getByTestId("build-setup-overview")).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "You're all set" }),
    ).toBeVisible();
    await expectSetupStepper(page);
    await expect(page.getByTestId("build-overview-welcome")).toBeVisible();
    await expect(page.getByTestId("build-overview-try")).toBeVisible();
    await expect(page.getByRole("link", { name: /Run setup again/ })).toHaveAttribute(
      "href",
      "/setup/welcome",
    );
    await expect(page.getByRole("link", { name: /Browse apps/ })).toHaveAttribute(
      "href",
      "/apps",
    );
  });
});
