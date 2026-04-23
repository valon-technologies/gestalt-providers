import {
  test,
  expect,
  mockAgentRuns,
  mockAuthInfo,
  mockIntegrationOperations,
  mockIntegrations,
} from "./fixtures";

test.describe("Agents", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(authenticatedPage, [
      {
        name: "github",
        displayName: "GitHub",
        description: "Repository operations",
        connected: true,
      },
    ]);
    await mockIntegrationOperations(authenticatedPage, {
      github: [
        {
          id: "pull_requests.list",
          title: "List pull requests",
        },
      ],
    });
  });

  test("shows an empty state when no runs exist", async ({ authenticatedPage: page }) => {
    await mockAgentRuns(page, []);
    await page.goto("/agents");
    await expect(page.getByText("No agent runs yet.")).toBeVisible();
  });

  test("shows run details for the selected run", async ({ authenticatedPage: page }) => {
    await mockAgentRuns(page, [
      {
        id: "agent_run_123",
        provider: "simple",
        model: "fast",
        status: "succeeded",
        messages: [
          { role: "user", text: "Summarize open incidents." },
          { role: "assistant", text: "Two incidents are open." },
        ],
        outputText: "Two incidents are open.",
        structuredOutput: { count: 2 },
        statusMessage: "completed",
        sessionRef: "session_123",
        createdBy: {
          subjectId: "user:123",
          displayName: "Ada",
        },
        createdAt: "2026-04-23T00:00:00Z",
        startedAt: "2026-04-23T00:01:00Z",
        completedAt: "2026-04-23T00:02:00Z",
        executionRef: "agent_run_123",
      },
      {
        id: "agent_run_456",
        provider: "simple",
        model: "deep",
        status: "failed",
        messages: [{ role: "user", text: "Review support escalations." }],
        statusMessage: "model request failed",
        createdAt: "2026-04-22T00:00:00Z",
      },
    ]);

    await page.goto("/agents");
    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });

    await expect(page.getByRole("heading", { name: "Agents" })).toBeVisible();
    await expect(
      page.getByRole("button", { name: /Summarize open incidents/i }),
    ).toBeVisible();
    await expect(detailPanel.getByText("Two incidents are open.").first()).toBeVisible();
    await expect(detailPanel.getByText("session_123")).toBeVisible();
    await expect(detailPanel.getByText("\"count\": 2")).toBeVisible();

    await page.getByRole("button", { name: /Review support escalations/i }).click();
    await expect(detailPanel.getByText("agent_run_456")).toBeVisible();
    await expect(detailPanel.getByText("model request failed")).toBeVisible();
  });

  test("starts a run with explicit plugin tools", async ({ authenticatedPage: page }) => {
    let createBody: Record<string, unknown> | null = null;
    await mockAgentRuns(page, [], {
      onCreate(body) {
        createBody = body;
        return {
          id: "agent_run_new",
          provider: typeof body.provider === "string" ? body.provider : "simple",
          model: typeof body.model === "string" ? body.model : "fast",
          status: "running",
          messages: body.messages as never,
          createdAt: "2026-04-23T00:00:00Z",
          startedAt: "2026-04-23T00:00:00Z",
        };
      },
    });

    await page.goto("/agents");
    await page.getByRole("button", { name: "New run" }).first().click();
    await page.getByRole("textbox", { name: "Provider", exact: true }).fill("simple");
    await page.getByRole("textbox", { name: "Model", exact: true }).fill("fast");
    await page.getByLabel("User message").fill("Summarize the latest open PRs.");
    await page.getByLabel("Tools").selectOption("explicit");
    await page.getByLabel("Plugin").selectOption("github");
    await page.getByLabel("Operation").selectOption("pull_requests.list");
    await page.getByRole("button", { name: "Start run" }).click();

    await expect(page.getByRole("button", { name: /Summarize the latest open PRs/i })).toBeVisible();
    expect(createBody?.provider).toBe("simple");
    expect(createBody?.model).toBe("fast");
    expect(createBody?.toolSource).toBe("explicit");
    expect(createBody?.toolRefs).toEqual([
      {
        pluginName: "github",
        operation: "pull_requests.list",
      },
    ]);
  });

  test("cancels an active run from the detail panel", async ({ authenticatedPage: page }) => {
    await mockAgentRuns(page, [
      {
        id: "agent_run_active",
        provider: "simple",
        model: "fast",
        status: "running",
        messages: [{ role: "user", text: "Draft the launch notes." }],
        createdAt: "2026-04-23T00:00:00Z",
        startedAt: "2026-04-23T00:01:00Z",
      },
    ]);

    await page.goto("/agents");
    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });

    await page.getByRole("button", { name: "Cancel run" }).click();
    await expect(detailPanel.getByText(/^canceled$/i)).toBeVisible();
    await expect(detailPanel.getByText("Run canceled.")).toBeVisible();
    await expect(page.getByRole("button", { name: "Cancel run" })).toHaveCount(0);
  });
});
