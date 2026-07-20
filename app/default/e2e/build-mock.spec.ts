import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrations,
  mockManagedIdentities,
  mockTokens,
} from "./fixtures";

test.describe("Build page", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(authenticatedPage, []);
    // Catalog entries (icons) without connection — connect step stays incomplete.
    await mockIntegrations(authenticatedPage, [
      {
        name: "google_calendar",
        displayName: "Google Calendar",
        description: "Calendar",
        iconSvg:
          '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>',
        credentialState: "missing",
        status: "needs_user_connection",
      },
      {
        name: "slack",
        displayName: "Slack",
        description: "Slack",
        iconSvg:
          '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>',
        credentialState: "missing",
        status: "needs_user_connection",
      },
      {
        name: "notion",
        displayName: "Notion",
        description: "Notion",
        iconSvg:
          '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M4 4h16v16H4z"/></svg>',
        credentialState: "missing",
        status: "needs_user_connection",
      },
    ]);
    await mockTokens(authenticatedPage, []);
  });

  test("renders connect checklist and primary Calendar aha", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/build");

    await expect(
      page.getByRole("heading", { name: "Build", exact: true }),
    ).toBeVisible();
    // Focus + progress: all step labels visible; only current body expands.
    await expect(
      page.getByRole("heading", { name: "Connect an app" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Grant access" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Make your first call" }),
    ).toBeVisible();

    await expect(
      page.getByRole("radio", { name: /Google Calendar\s+See what.s next/ }),
    ).toBeVisible();
    await expect(
      page.getByRole("radio", { name: /Slack\s+Post a hello/ }),
    ).toBeVisible();
    await expect(
      page.getByRole("radio", { name: /Notion\s+Find a page/ }),
    ).toBeVisible();

    await expect(
      page.getByRole("radio", { name: /Google Calendar\s+See what.s next/ }),
    ).toBeChecked();

    await expect(
      page.getByRole("button", { name: "Connect Google Calendar" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "See all apps" }),
    ).toBeVisible();

    // Upcoming steps are labels only — no muted bodies or invoke recipe yet.
    await expect(
      page.getByRole("link", { name: "Open Authorization" }),
    ).toHaveCount(0);
    await expect(page.getByLabel("Token name")).toHaveCount(0);
    await expect(
      page.getByText(/gestalt apps invoke google_calendar events\.list/),
    ).toHaveCount(0);

    await expect(
      page.getByRole("link", { name: "Build", exact: true }),
    ).toBeVisible();
  });

  test("switches Connect CTA when selecting Slack", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/build");
    await page.getByRole("radio", { name: /Slack\s+Post a hello/ }).click();
    await expect(
      page.getByRole("radio", { name: /Slack\s+Post a hello/ }),
    ).toBeChecked();
    await expect(
      page.getByRole("button", { name: "Connect Slack" }),
    ).toBeVisible();
  });

  test("shows invoke recipe after apps and tokens; supports revisit", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [
      {
        name: "google_calendar",
        displayName: "Google Calendar",
        description: "Calendar",
        iconSvg:
          '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>',
        credentialState: "connected",
        status: "ready",
      },
      {
        name: "slack",
        displayName: "Slack",
        description: "Slack",
        iconSvg:
          '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>',
        credentialState: "missing",
        status: "needs_user_connection",
      },
      {
        name: "notion",
        displayName: "Notion",
        description: "Notion",
        iconSvg:
          '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M4 4h16v16H4z"/></svg>',
        credentialState: "missing",
        status: "needs_user_connection",
      },
    ]);
    await mockTokens(page, [
      {
        id: "tok_123",
        name: "Default token",
        scopes: ["api"],
        createdAt: "2026-04-13T00:00:00Z",
      },
    ]);

    await page.goto("/build");

    // Prerequisites met → invoke guide + congrats (no confirm gate).
    await expect(page.getByText(/Add Gestalt as an MCP server/)).toBeVisible();
    await expect(page.getByText(/\.cursor\/mcp\.json/)).toBeVisible();
    await expect(
      page.getByText(
        /Using Gestalt, list the next 5 events on my primary Google Calendar/,
      ),
    ).toBeVisible();
    await expect(
      page.getByText(/Congratulations — you're ready to build/),
    ).toBeVisible();
    await expect(
      page.getByText(/You should see something like this/),
    ).toBeVisible();
    await expect(
      page.getByText(/Design review — Tomorrow 10:00–10:30 AM/),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Browse apps" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Agent identities" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Read the docs" }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "I saw a result" }),
    ).toHaveCount(0);
    await expect(
      page.locator('[data-slot="timeline-steps-item"][data-status="completed"]'),
    ).toHaveCount(3);

    await page.getByRole("radio", { name: "Claude Code" }).click();
    await expect(page.getByText(/\.mcp\.json/)).toBeVisible();
    await page.getByRole("radio", { name: "Codex" }).click();
    await expect(page.getByText(/codex mcp add gestalt/)).toBeVisible();

    // Completed connect is revisitable.
    await expect(
      page.getByText(/Google Calendar is connected/),
    ).toBeVisible();
    await page.getByRole("button", { name: "Change app" }).click();
    await expect(
      page.getByRole("radio", { name: /Google Calendar\s+See what.s next/ }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Back to current step" }).click();
    await expect(
      page.getByText(/Congratulations — you're ready to build/),
    ).toBeVisible();
  });

  test("grant access embeds token create instead of dumping to Authorization", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [
      {
        name: "google_calendar",
        displayName: "Google Calendar",
        description: "Calendar",
        iconSvg:
          '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>',
        credentialState: "connected",
        status: "ready",
      },
    ]);
    await mockTokens(page, []);

    await page.goto("/build");

    await expect(
      page.getByRole("heading", { name: "Grant access" }),
    ).toBeVisible();
    await expect(page.getByLabel("Token name")).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Create Token" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Manage all tokens" }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Open Authorization" }),
    ).toHaveCount(0);
  });
});
