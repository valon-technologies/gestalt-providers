import {
  test,
  expect,
  mockAgentSessions,
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

  test("shows an empty state when no sessions exist", async ({
    authenticatedPage: page,
  }) => {
    await mockAgentSessions(page, { sessions: [], turns: {} });

    await page.goto("/agents");

    await expect(page.getByText("No agent sessions yet.")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Agent Sessions" })).toBeVisible();
  });

  test("opens a deep-linked session console with transcript and public events", async ({
    authenticatedPage: page,
  }) => {
    await mockAgentSessions(page, {
      providers: [
        {
          name: "simple",
          default: true,
          capabilities: { supportedToolSources: ["mcp_catalog"] },
        },
      ],
      sessions: [
        {
          id: "agent_session_123",
          provider: "simple",
          model: "fast",
          clientRef: "triage-session",
          state: "active",
          createdAt: "2026-04-23T00:00:00Z",
          updatedAt: "2026-04-23T00:00:00Z",
          lastTurnAt: "2026-04-23T00:02:00Z",
        },
      ],
      turns: {
        agent_session_123: [
          {
            id: "agent_turn_123",
            sessionId: "agent_session_123",
            provider: "simple",
            model: "fast",
            status: "succeeded",
            messages: [{ role: "user", text: "Summarize open incidents." }],
            outputText: "Two incidents are open.",
            structuredOutput: { count: 2 },
            createdAt: "2026-04-23T00:00:00Z",
            completedAt: "2026-04-23T00:02:00Z",
          },
        ],
      },
      events: {
        agent_turn_123: [
          {
            id: "evt_1",
            turnId: "agent_turn_123",
            seq: 1,
            type: "assistant.message",
            visibility: "public",
            data: { text: "Two incidents are open." },
            createdAt: "2026-04-23T00:01:00Z",
          },
          {
            id: "evt_2",
            turnId: "agent_turn_123",
            seq: 2,
            type: "tool.started",
            visibility: "public",
            data: {
              tool_id: "linear-list",
              status: "started",
              arguments: { query: "Ada" },
            },
            createdAt: "2026-04-23T00:01:20Z",
          },
          {
            id: "evt_3",
            turnId: "agent_turn_123",
            seq: 3,
            type: "tool.completed",
            visibility: "public",
            data: { toolName: "github", status: 200, output: { count: 2 } },
            display: {
              kind: "tool",
              phase: "completed",
              label: "GitHub",
              output: { count: 2 },
            },
            createdAt: "2026-04-23T00:01:30Z",
          },
          {
            id: "evt_4",
            turnId: "agent_turn_123",
            seq: 4,
            type: "private.secret",
            visibility: "private",
            data: { text: "do not show" },
            display: { kind: "text", text: "do not show" },
            createdAt: "2026-04-23T00:01:40Z",
          },
          {
            id: "evt_5",
            turnId: "agent_turn_123",
            seq: 5,
            type: "custom.public",
            visibility: "public",
            data: { note: "visible fallback" },
            createdAt: "2026-04-23T00:01:50Z",
          },
        ],
      },
    });

    await page.goto("/agents?session=agent_session_123&turn=agent_turn_123");
    const activityPanel = page.locator("aside").filter({
      has: page.getByRole("heading", { name: "Activity", exact: true }),
    });
    const startedTool = activityPanel.locator("details").filter({
      hasText: "linear-list",
    });
    const inlineStartedTool = page.locator("article").filter({
      hasText: "linear-list",
    });
    const inlineCompletedTool = page.locator("article").filter({
      hasText: "GitHub",
    });
    const customEvent = activityPanel.locator("details").filter({
      hasText: "custom.public",
    });

    await expect(page.getByRole("heading", { name: "triage-session" })).toBeVisible();
    await expect(
      activityPanel.getByRole("heading", { name: "Activity", exact: true }),
    ).toBeVisible();
    await expect(
      activityPanel.getByRole("heading", { name: "Public Activity" }),
    ).toBeVisible();
    await expect(page.getByText("Summarize open incidents.").first()).toBeVisible();
    await expect(page.getByText("Two incidents").first()).toBeVisible();
    await expect(inlineStartedTool.getByText("linear-list").first()).toBeVisible();
    await expect(inlineStartedTool.getByText("#2 started").first()).toBeVisible();
    await inlineStartedTool.locator("summary").filter({ hasText: "Input" }).click();
    await expect(inlineStartedTool.getByText('"query": "Ada"').first()).toBeVisible();
    await expect(inlineCompletedTool.getByText("GitHub").first()).toBeVisible();
    await inlineCompletedTool
      .locator("summary")
      .filter({ hasText: "Output" })
      .click();
    await expect(inlineCompletedTool.getByText('"count": 2').first()).toBeVisible();
    await expect(startedTool.getByText("linear-list").first()).toBeVisible();
    await expect(startedTool.getByText("started").first()).toBeVisible();
    await expect(activityPanel.getByText("GitHub", { exact: true })).toBeVisible();
    await expect(
      customEvent.getByText("custom.public", { exact: true }).first(),
    ).toBeVisible();
    await startedTool.locator("summary").click();
    await expect(startedTool.getByText('"query": "Ada"').first()).toBeVisible();
    await customEvent.locator("summary").click();
    await expect(customEvent.getByText("visible fallback").first()).toBeVisible();
    await expect(page.getByText("do not show")).toHaveCount(0);
  });

  test("keeps the composer visible with a long transcript", async ({
    authenticatedPage: page,
  }) => {
    await page.setViewportSize({ width: 1280, height: 720 });
    await mockAgentSessions(page, {
      sessions: [
        {
          id: "agent_session_long",
          provider: "simple",
          model: "fast",
          clientRef: "long-session",
          state: "active",
          createdAt: "2026-04-23T00:00:00Z",
          updatedAt: "2026-04-23T00:00:00Z",
          lastTurnAt: "2026-04-23T00:30:00Z",
        },
      ],
      turns: {
        agent_session_long: [
          {
            id: "agent_turn_long",
            sessionId: "agent_session_long",
            provider: "simple",
            model: "fast",
            status: "succeeded",
            messages: [
              { role: "user", text: "Review the long transcript." },
              ...Array.from({ length: 35 }, (_, index) => ({
                role: "assistant",
                text: `Transcript line ${index + 1}`,
              })),
            ],
            createdAt: "2026-04-23T00:00:00Z",
            completedAt: "2026-04-23T00:30:00Z",
          },
        ],
      },
    });

    await page.goto("/agents?session=agent_session_long&turn=agent_turn_long");

    const composer = page.getByLabel("User message");
    const latestMessage = page.getByText("Transcript line 35").first();
    await expect(composer).toBeVisible();
    await expect(latestMessage).toBeVisible();

    const composerBox = await composer.boundingBox();
    const latestBox = await latestMessage.boundingBox();
    expect(composerBox).not.toBeNull();
    expect(latestBox).not.toBeNull();
    expect(composerBox!.y + composerBox!.height).toBeLessThanOrEqual(720);
    expect(latestBox!.y).toBeGreaterThanOrEqual(0);
    expect(latestBox!.y).toBeLessThanOrEqual(720);
  });

  test("opens a deep-linked turn outside the session summary page", async ({
    authenticatedPage: page,
  }) => {
    const turns = Array.from({ length: 25 }, (_, index) => ({
      id: `agent_turn_${index + 1}`,
      sessionId: "agent_session_deep",
      provider: "simple",
      model: "fast",
      status: "succeeded",
      messages: [
        {
          role: "user",
          text:
            index === 24
              ? "Older turn message outside the first page."
              : `Recent turn ${index + 1}.`,
        },
      ],
      createdAt: `2026-04-23T00:${String(59 - index).padStart(2, "0")}:00Z`,
      completedAt: `2026-04-23T00:${String(59 - index).padStart(2, "0")}:30Z`,
    }));
    await mockAgentSessions(page, {
      sessions: [
        {
          id: "agent_session_deep",
          provider: "simple",
          model: "fast",
          state: "active",
          createdAt: "2026-04-23T00:00:00Z",
          updatedAt: "2026-04-23T01:00:00Z",
          lastTurnAt: "2026-04-23T01:00:00Z",
        },
      ],
      turns: { agent_session_deep: turns },
    });

    await page.goto("/agents?session=agent_session_deep&turn=agent_turn_25");

    await expect(
      page.getByText("Older turn message outside the first page.").first(),
    ).toBeVisible();
  });

  test("creates a new session and turn with the agent default", async ({
    authenticatedPage: page,
  }) => {
    let createTurnBody: Record<string, unknown> | null = null;
    await mockAgentSessions(
      page,
      { sessions: [], turns: {} },
      {
        onCreateTurn(session, body) {
          createTurnBody = body;
          return {
            id: "agent_turn_new",
            sessionId: session.id,
            provider: session.provider,
            model: session.model,
            status: "running",
            messages: body.messages as never,
            createdAt: "2026-04-23T00:00:00Z",
            startedAt: "2026-04-23T00:00:00Z",
          };
        },
      },
    );

    await page.goto("/agents");
    await page.getByLabel("User message").fill("Draft the launch notes.");
    await page.getByRole("button", { name: /create session/i }).click();

    await expect(page.getByText("Agent turn started.")).toBeVisible();
    expect(createTurnBody?.toolRefs).toBeUndefined();
    expect(createTurnBody?.toolSource).toBeUndefined();
  });

  test("uses a compact chat composer for selected sessions", async ({
    authenticatedPage: page,
  }) => {
    let createTurnBody: Record<string, unknown> | null = null;
    await mockAgentSessions(
      page,
      {
        sessions: [
          {
            id: "agent_session_chat",
            provider: "simple",
            model: "fast",
            state: "active",
            createdAt: "2026-04-23T00:00:00Z",
            updatedAt: "2026-04-23T00:00:00Z",
          },
        ],
        turns: { agent_session_chat: [] },
      },
      {
        onCreateTurn(session, body) {
          createTurnBody = body;
          return {
            id: "agent_turn_chat",
            sessionId: session.id,
            provider: session.provider,
            model: "fast",
            status: "running",
            messages: body.messages as never,
            createdAt: "2026-04-23T00:00:00Z",
            startedAt: "2026-04-23T00:00:00Z",
          };
        },
      },
    );

    await page.goto("/agents?session=agent_session_chat");

    await expect(page.getByLabel("User message")).toBeVisible();
    await expect(page.getByRole("button", { name: /send turn/i })).toBeVisible();

    await page.getByLabel("User message").fill("Check the latest rollout.");
    await page.getByLabel("User message").press("Control+Enter");

    await expect(page.getByText("Agent turn started.")).toBeVisible();
    expect(createTurnBody?.messages).toEqual([
      { role: "user", text: "Check the latest rollout." },
    ]);
    expect(createTurnBody?.toolRefs).toBeUndefined();
  });

  test.skip("starts a selected-tool turn using the mcp_catalog wire contract", async ({
    authenticatedPage: page,
  }) => {
    let createTurnBody: Record<string, unknown> | null = null;
    await mockAgentSessions(
      page,
      {
        sessions: [
          {
            id: "agent_session_tools",
            provider: "simple",
            model: "fast",
            state: "active",
            createdAt: "2026-04-23T00:00:00Z",
            updatedAt: "2026-04-23T00:00:00Z",
          },
        ],
        turns: { agent_session_tools: [] },
      },
      {
        onCreateTurn(session, body) {
          createTurnBody = body;
          return {
            id: "agent_turn_tools",
            sessionId: session.id,
            provider: session.provider,
            model: "fast",
            status: "running",
            messages: body.messages as never,
            createdAt: "2026-04-23T00:00:00Z",
            startedAt: "2026-04-23T00:00:00Z",
          };
        },
      },
    );

    await page.goto("/agents?session=agent_session_tools");
    await page.getByLabel("User message").fill("Summarize the latest open PRs.");
    await page.getByText("Turn options").click();
    await page.getByLabel("Tools", { exact: true }).selectOption("selected");
    await page.getByLabel("Plugin").selectOption("github");
    await page.getByLabel("Operation").selectOption("pull_requests.list");
    await page.getByRole("button", { name: "Send turn" }).click();

    await expect(page.getByText("Agent turn started.")).toBeVisible();
    expect(createTurnBody?.toolSource).toBe("mcp_catalog");
    expect(createTurnBody?.toolRefs).toEqual([
      {
        plugin: "github",
        operation: "pull_requests.list",
      },
    ]);
  });

  test("handles waiting_for_input turns with cancel and approval resolution", async ({
    authenticatedPage: page,
  }) => {
    let resolution: Record<string, unknown> | null = null;
    await mockAgentSessions(
      page,
      {
        sessions: [
          {
            id: "agent_session_waiting",
            provider: "simple",
            model: "fast",
            state: "active",
            createdAt: "2026-04-23T00:00:00Z",
            updatedAt: "2026-04-23T00:00:00Z",
            lastTurnAt: "2026-04-23T00:01:00Z",
          },
        ],
        turns: {
          agent_session_waiting: [
            ...Array.from({ length: 20 }, (_, index) => ({
              id: `agent_turn_recent_${index + 1}`,
              sessionId: "agent_session_waiting",
              provider: "simple",
              model: "fast",
              status: "succeeded",
              messages: [{ role: "user", text: `Recent turn ${index + 1}.` }],
              createdAt: `2026-04-23T00:${String(59 - index).padStart(2, "0")}:00Z`,
              completedAt: `2026-04-23T00:${String(59 - index).padStart(2, "0")}:30Z`,
            })),
            {
              id: "agent_turn_waiting",
              sessionId: "agent_session_waiting",
              provider: "simple",
              model: "fast",
              status: "waiting_for_input",
              messages: [{ role: "user", text: "Deploy the change." }],
              createdAt: "2026-04-23T00:00:00Z",
            },
          ],
        },
        events: {
          agent_turn_waiting: [
            {
              id: "evt_waiting",
              turnId: "agent_turn_waiting",
              seq: 1,
              type: "interaction.requested",
              visibility: "public",
              data: { interactionId: "interaction_1" },
            },
          ],
        },
        interactions: {
          agent_turn_waiting: [
            {
              id: "interaction_1",
              turnId: "agent_turn_waiting",
              type: "approval",
              state: "pending",
              title: "Approve deployment",
              prompt: "Deploy to production?",
              request: {},
            },
          ],
        },
      },
      {
        onResolveInteraction(_interaction, body) {
          resolution = body;
          return undefined;
        },
      },
    );

    await page.goto("/agents?session=agent_session_waiting&turn=agent_turn_waiting");
    await expect(page.getByText("Waiting For Input")).toBeVisible();
    await page.getByRole("button", { name: "Approve" }).click();

    await expect(page.getByText("Interaction resolved.")).toBeVisible();
    expect(resolution).toEqual({ approved: true });

    await page.getByRole("button", { name: "Cancel turn" }).click();
    await expect(page.getByText("Agent turn canceled.")).toBeVisible();
    await expect(page.getByText(/^canceled$/i)).toBeVisible();
  });
});
