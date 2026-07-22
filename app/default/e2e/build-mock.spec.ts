import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrations,
  mockManagedIdentities,
  mockTokens,
} from "./fixtures";

const catalogFixtures = [
  {
    name: "slack",
    displayName: "Slack",
    description: "Slack",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>',
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "pagerduty",
    displayName: "PagerDuty",
    description: "PagerDuty",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>',
    credentialState: "missing" as const,
    status: "needs_user_connection" as const,
  },
  {
    name: "linear",
    displayName: "Linear",
    description: "Linear",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M4 4h16v16H4z"/></svg>',
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
    name: "valonLearn",
    displayName: "Valon Learn",
    description: "Learn",
    mountedPath: "/learn",
    iconSvg:
      '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="6"/></svg>',
    credentialState: "connected" as const,
    status: "ready" as const,
  },
  {
    name: "valonSats",
    displayName: "Valon SATs",
    description: "SATs",
    mountedPath: "/valon-sats",
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

test.describe("Build page", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(authenticatedPage, []);
    await mockIntegrations(authenticatedPage, catalogFixtures);
    await mockTokens(authenticatedPage, []);
  });

  test("redirects /build to intro; outcome toggle drives AgentConsole", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/build");

    await expect(page).toHaveURL(/\/build\/intro$/);
    await expect(
      page.getByRole("heading", { name: "Build", exact: true }),
    ).toBeVisible();

    await expect(page.getByTestId("build-intro")).toBeVisible();
    await expect(page.getByTestId("build-outcome-toggle")).toBeVisible();
    await expect(page.getByTestId("build-outcome-card-aiSpendTracker")).toBeVisible();
    await expect(page.getByTestId("build-outcome-card-oncall")).toBeVisible();
    await expect(page.getByTestId("build-outcome-card-valonLearn")).toBeVisible();
    await expect(page.getByTestId("build-outcome-card-valonSats")).toBeVisible();

    await expect(
      page.getByTestId("build-outcome-card-aiSpendTracker").getByText("Engineering"),
    ).toBeVisible();
    await expect(
      page.getByRole("radio", { name: "Monitor spending" }),
    ).toBeChecked();
    await expect(page.getByText("Already built at Valon")).toHaveCount(0);
    await expect(page.getByRole("radio", { name: "AI Spend Tracker" })).toHaveCount(
      0,
    );

    await expect(page.getByTestId("build-agent-console")).toBeVisible();
    await expect(page.getByTestId("build-agent-console")).toContainText(
      /Cursor and Claude spend|Engineering overall/i,
    );

    await page.getByRole("radio", { name: "Practice servicing knowledge" }).click();
    await expect(page.getByTestId("build-outcome-department")).toHaveText(
      "Default Servicing",
    );
    await expect(page.getByTestId("build-agent-console")).toContainText(
      /servicing knowledge|assessment/i,
    );

    await page.getByRole("radio", { name: "Check oncall schedule" }).click();
    await expect(
      page.getByTestId("build-outcome-card-oncall").getByText("Engineering"),
    ).toBeVisible();
    await expect(page.getByTestId("build-outcome-department")).toHaveText(
      "Engineering",
    );

    await expect(page.getByTestId("build-nav-intro")).toBeVisible();
    await expect(page.getByTestId("build-nav-authorize")).toBeVisible();
    await expect(page.getByTestId("build-nav-install")).toBeVisible();
    await expect(page.getByTestId("build-nav-connect")).toBeVisible();
    await expect(page.getByTestId("build-nav-invoke")).toBeVisible();

    await expect(page.getByTestId("build-showcase")).toHaveCount(0);
    await expect(page.getByLabel("Token name")).toHaveCount(0);
  });

  test("continue from intro opens authorize", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/build/intro");
    await page.getByTestId("build-intro-continue").click();
    await expect(page).toHaveURL(/\/build\/authorize$/);
    await expect(page.getByLabel("Token name")).toBeVisible();
    await expect(page.getByText("Expiration", { exact: true })).toBeVisible();
    await expect(page.getByTestId("build-step-next")).toContainText(
      "Install Gestalt",
    );
  });

  test("paste + Add to Cursor on install after tokens exist", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await page.addInitScript(() => {
      sessionStorage.setItem("gestalt.build.introSeen", "1");
    });

    await page.goto("/build");

    await expect(page).toHaveURL(/\/build\/install$/);
    await expect(page.getByTestId("build-add-to-cursor")).toBeDisabled();
    await expect(page.getByTestId("build-mark-mcp-installed")).toBeVisible();

    await page
      .getByLabel("Paste an API token secret")
      .fill("gst_api_test_token_for_install");
    await expect(page.getByTestId("build-add-to-cursor")).toBeEnabled();
    await expect(page.getByTestId("build-add-to-cursor")).toHaveAttribute(
      "href",
      /cursor:\/\/anysphere\.cursor-deeplink\/mcp\/install/,
    );

    await page.getByRole("radio", { name: "Claude Code" }).click();
    await expect(page.getByText(/claude mcp add --transport http/)).toBeVisible();
    await page.getByRole("radio", { name: "Codex" }).click();
    await expect(page.getByText(/codex mcp add gestalt/)).toBeVisible();
  });

  test("step pager advances from authorize to install after create", async ({
    authenticatedPage: page,
  }) => {
    await page.addInitScript(() => {
      sessionStorage.setItem("gestalt.build.introSeen", "1");
    });

    await page.route("**/api/v1/tokens", async (route, request) => {
      if (request.method() === "GET") {
        await route.fulfill({ json: [] });
        return;
      }
      if (request.method() === "POST") {
        const body = request.postDataJSON() as {
          name?: string;
          scopes?: string;
          expiresIn?: number;
        };
        expect(body).toEqual({
          name: "ci-pipeline",
          scopes: "",
          expiresIn: 30 * 24 * 60 * 60,
        });
        await route.fulfill({
          json: {
            id: "tok_new",
            name: "ci-pipeline",
            scopes: [],
            createdAt: "2026-07-21T00:00:00Z",
            token: "gst_api_created_once_secret",
          },
        });
        return;
      }
      await route.fallback();
    });

    await page.goto("/build/authorize");

    await page.getByLabel("Token name").fill("ci-pipeline");
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(
      page.getByText("Copy this token now. It will not be shown again."),
    ).toBeVisible();
    await expect(page.getByText("gst_api_created_once_secret")).toBeVisible();
    await expect(page.getByTestId("build-step-next")).toBeVisible();

    await page.getByTestId("build-step-next").click();

    await expect(page).toHaveURL(/\/build\/install$/);
    await expect(page.getByTestId("build-add-to-cursor")).toBeEnabled();
    await expect(page.getByLabel("API token secret")).toHaveValue(
      "gst_api_created_once_secret",
    );
    await expect(page.getByTestId("build-step-prev")).toContainText(
      "Create a token",
    );
  });

  test("connect shows companions for Oncall exemplar", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await page.addInitScript(() => {
      sessionStorage.setItem("gestalt.build.introSeen", "1");
      sessionStorage.setItem("gestalt.build.mcpInstalled", "1");
      sessionStorage.setItem("gestalt.build.activeExemplarId", "oncall");
    });

    await page.goto("/build");

    await expect(page).toHaveURL(/\/build\/connect$/);
    await expect(page.getByTestId("build-connect-app-pagerduty")).toBeVisible();
    await expect(page.getByTestId("build-connect-app-linear")).toBeVisible();
    await expect(page.getByTestId("build-connect-app-slack")).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Connect PagerDuty" }),
    ).toBeVisible();
  });

  test("self-contained exemplar skips companion connect", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await page.addInitScript(() => {
      sessionStorage.setItem("gestalt.build.introSeen", "1");
      sessionStorage.setItem("gestalt.build.mcpInstalled", "1");
      sessionStorage.setItem("gestalt.build.activeExemplarId", "valonLearn");
    });

    await page.goto("/build");

    await expect(page).toHaveURL(/\/build\/invoke$/);
    await expect(page.getByTestId("build-golden-prompt")).toBeVisible();
    await expect(page.getByText("valonLearn.listMyProgress")).toBeVisible();
    await expect(page.getByTestId("build-exemplar-cta")).toBeVisible();
    await expect(page.getByTestId("build-open-exemplar")).toHaveAttribute(
      "href",
      "/learn",
    );
    await expect(
      page.getByText(/Kaitlyn Schiffhauer|already shipped/i),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Browse app store" }),
    ).toBeVisible();
  });

  test("shows invoke after companions connected for AI Spend Tracker", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [
      {
        ...catalogFixtures[0],
        credentialState: "connected",
        status: "ready",
      },
      catalogFixtures[1],
      catalogFixtures[2],
      catalogFixtures[3],
      catalogFixtures[4],
      catalogFixtures[5],
      catalogFixtures[6],
    ]);
    await mockTokens(page, [defaultToken]);
    await page.addInitScript(() => {
      sessionStorage.setItem("gestalt.build.introSeen", "1");
      sessionStorage.setItem("gestalt.build.mcpInstalled", "1");
      sessionStorage.setItem(
        "gestalt.build.activeExemplarId",
        "aiSpendTracker",
      );
    });

    await page.goto("/build");

    await expect(page).toHaveURL(/\/build\/invoke$/);
    await expect(page.getByTestId("build-golden-prompt")).toBeVisible();
    await expect(page.getByText("aiSpendTracker.getMyUsage")).toBeVisible();
    await expect(page.getByTestId("build-open-exemplar")).toHaveAttribute(
      "href",
      "/ai-spend",
    );
    await expect(page.getByTestId("build-step-panel")).toBeVisible();
    await expect(page.getByTestId("build-nav-intro")).toHaveAttribute(
      "data-state",
      "completed",
    );
  });

  test("authorize step lists tokens and supports add another", async ({
    authenticatedPage: page,
  }) => {
    await mockTokens(page, [defaultToken]);
    await page.addInitScript(() => {
      sessionStorage.setItem("gestalt.build.introSeen", "1");
      sessionStorage.setItem("gestalt.build.mcpInstalled", "1");
      sessionStorage.setItem("gestalt.build.activeExemplarId", "valonSats");
    });

    await page.goto("/build/authorize");

    await expect(page.getByTestId("build-token-list")).toBeVisible();
    await expect(page.getByText("Default token")).toBeVisible();
    await expect(page.getByTestId("build-add-another-token")).toBeVisible();
    await expect(page.getByLabel("Token name")).toHaveCount(0);
    await page.getByTestId("build-add-another-token").click();
    await expect(page.getByLabel("Token name")).toBeVisible();
    await expect(page.getByRole("button", { name: "Create Token" })).toHaveCount(
      1,
    );
  });
});
