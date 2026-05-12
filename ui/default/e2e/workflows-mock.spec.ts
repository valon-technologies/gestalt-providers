import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrationOperations,
  mockIntegrations,
  mockWorkflowEventTriggers,
  mockWorkflowRuns,
  mockWorkflowSchedules,
} from "./fixtures";
import type { WorkflowEventTrigger, WorkflowRun, WorkflowSchedule } from "../src/lib/api";

test.describe("Workflows", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockWorkflowSchedules(authenticatedPage, []);
    await mockWorkflowEventTriggers(authenticatedPage, []);
  });

  test("shows an empty state when no runs exist", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, []);
    await page.goto("/workflows");
    await expect(page.getByText("No workflow runs yet.")).toBeVisible();
  });

  test("refreshes the list without clearing the current page state", async ({ authenticatedPage: page }) => {
    const workflowRuns = await mockWorkflowRuns(page, [
      {
        id: "run_initial",
        provider: "basic",
        status: "succeeded",
        target: {
          plugin: "slack",
          operation: "chat.postMessage",
        } as unknown as WorkflowRun["target"],
        trigger: {
          kind: "manual",
        },
        createdAt: "2026-04-20T00:00:00Z",
        completedAt: "2026-04-20T00:02:00Z",
      },
    ]);

    await page.goto("/workflows");
    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toBeVisible();

    workflowRuns.setRuns([
      {
        id: "run_refreshed",
        provider: "basic",
        status: "failed",
        target: {
          plugin: {
            name: "github",
            operation: "issues.create",
          },
        },
        trigger: {
          kind: "schedule",
          scheduleId: "sched_456",
        },
        createdAt: "2026-04-21T00:00:00Z",
      },
    ]);

    await page.getByRole("button", { name: "Refresh" }).click();
    await expect(page.getByRole("button", { name: /github\.issues\.create/i })).toBeVisible();
    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toHaveCount(0);
  });

  test("shows run details for the selected run", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_123",
        provider: "basic",
        status: "succeeded",
        target: {
          plugin: {
            name: "slack",
            operation: "chat.postMessage",
            connection: "workspace",
            input: { channel: "C123", text: "hello" },
          },
        },
        trigger: {
          kind: "schedule",
          scheduleId: "sched_123",
        },
        createdBy: {
          subjectId: "user:123",
          displayName: "Ada",
        },
        createdAt: "2026-04-20T00:00:00Z",
        startedAt: "2026-04-20T00:01:00Z",
        completedAt: "2026-04-20T00:02:00Z",
        statusMessage: "completed",
        resultBody: "{\"ok\":true}",
      },
      {
        id: "run_456",
        provider: "advanced",
        status: "failed",
        target: {
          plugin: {
            name: "github",
            operation: "issues.create",
          },
        },
        trigger: {
          kind: "event",
          triggerId: "evt_456",
        },
        createdAt: "2026-04-19T00:00:00Z",
      },
    ]);

    await page.goto("/workflows");
    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });
    await expect(page.getByRole("heading", { name: "Workflows" })).toBeVisible();
    await expect(
      page.getByRole("button", { name: /slack\.chat\.postMessage/i }),
    ).toBeVisible();
    await expect(detailPanel.getByText(/^completed$/)).toBeVisible();
    await expect(detailPanel.getByText("schedule:sched_123")).toBeVisible();
    await expect(detailPanel.getByText("slack.chat.postMessage")).toBeVisible();

    await page.getByRole("button", { name: /github\.issues\.create/i }).click();
    await expect(detailPanel.getByText("run_456")).toBeVisible();
    await expect(detailPanel.getByText("trigger:evt_456")).toBeVisible();
    await expect(detailPanel.getByText("github.issues.create")).toBeVisible();
  });

  test("cancels an in-flight run from the detail panel", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_inflight",
        provider: "basic",
        status: "pending",
        target: {
          plugin: {
            name: "slack",
            operation: "chat.postMessage",
          },
        },
        trigger: {
          kind: "manual",
        },
        createdAt: "2026-04-20T00:00:00Z",
        startedAt: "2026-04-20T00:01:00Z",
      },
    ]);

    await page.goto("/workflows");
    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });

    await page.getByRole("button", { name: "Cancel run" }).click();
    await expect(detailPanel.getByText(/^canceled$/i)).toBeVisible();
    await expect(detailPanel.getByText("Run canceled.")).toBeVisible();
    await expect(page.getByRole("button", { name: "Cancel run" })).toHaveCount(0);
  });

  test("shows cancel errors without clearing the selected run", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(
      page,
      [
        {
          id: "run_pending",
          provider: "basic",
          status: "pending",
          target: {
            plugin: {
              name: "slack",
              operation: "chat.postMessage",
            },
          },
          trigger: {
            kind: "manual",
          },
          createdAt: "2026-04-20T00:00:00Z",
        },
      ],
      {
        onCancel() {
          return {
            status: 412,
            json: { error: "workflow run cannot be canceled once it has started" },
          };
        },
      },
    );

    await page.goto("/workflows");
    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });

    await page.getByRole("button", { name: "Cancel run" }).click();
    await expect(detailPanel.getByText("workflow run cannot be canceled once it has started")).toBeVisible();
    await expect(detailPanel.getByText("run_pending")).toBeVisible();
    await expect(page.getByRole("button", { name: "Cancel run" })).toBeVisible();
  });

  test("does not offer cancel for a running run", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_running",
        provider: "basic",
        status: "running",
        target: {
          plugin: {
            name: "slack",
            operation: "chat.postMessage",
          },
        },
        trigger: {
          kind: "manual",
        },
        createdAt: "2026-04-20T00:00:00Z",
        startedAt: "2026-04-20T00:01:00Z",
      },
    ]);

    await page.goto("/workflows");
    await expect(page.getByRole("button", { name: "Cancel run" })).toHaveCount(0);
  });

  test("shows schedule details in the schedules tab", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, []);
    await mockWorkflowSchedules(page, [
      {
        id: "sched_123",
        provider: "basic",
        cron: "0 */5 * * *",
        timezone: "UTC",
        target: {
          plugin: "slack",
          operation: "chat.postMessage",
          connection: "workspace",
        } as unknown as WorkflowSchedule["target"],
        nextRunAt: "2026-04-21T00:05:00Z",
        createdAt: "2026-04-20T00:00:00Z",
        updatedAt: "2026-04-20T01:00:00Z",
      },
      {
        id: "sched_456",
        provider: "advanced",
        cron: "0 9 * * 1-5",
        paused: true,
        target: {
          plugin: {
            name: "github",
            operation: "issues.create",
          },
        },
        createdAt: "2026-04-19T00:00:00Z",
      },
    ]);

    await page.goto("/workflows");
    await page.getByRole("tab", { name: "Schedules" }).click();

    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Schedule Details" }),
    });

    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toBeVisible();
    await expect(detailPanel.getByText("sched_123")).toBeVisible();
    await expect(detailPanel.getByText("0 */5 * * *")).toBeVisible();
    await expect(detailPanel.getByText("UTC")).toBeVisible();

    await page.getByRole("button", { name: /github\.issues\.create/i }).click();
    await expect(detailPanel.getByText("sched_456")).toBeVisible();
    await expect(page.getByRole("button", { name: "Resume" })).toBeVisible();
  });

  test("writes canonical workflow targets from schedule and trigger forms", async ({ authenticatedPage: page }) => {
    const createBodies: Array<Record<string, unknown>> = [];
    const updateBodies: Array<Record<string, unknown>> = [];
    const triggerCreateBodies: Array<Record<string, unknown>> = [];
    const triggerUpdateBodies: Array<Record<string, unknown>> = [];
    await mockWorkflowRuns(page, []);
    await mockIntegrations(page, [
      {
        name: "slack",
        displayName: "Slack",
        connected: true,
      },
    ]);
    await mockIntegrationOperations(page, {
      slack: [{ id: "chat.postMessage", title: "Post Message" }],
    });
    await mockWorkflowSchedules(page, [], {
      onCreate(body) {
        createBodies.push(body);
        return {
          id: "sched_created",
          provider: typeof body.provider === "string" ? body.provider : "basic",
          cron: typeof body.cron === "string" ? body.cron : "0 * * * *",
          timezone: typeof body.timezone === "string" ? body.timezone : "UTC",
          target: body.target as WorkflowSchedule["target"],
          paused: Boolean(body.paused),
          createdAt: "2026-04-21T00:00:00Z",
          updatedAt: "2026-04-21T00:00:00Z",
        };
      },
      onUpdate(current, body) {
        updateBodies.push(body);
        return {
          ...current,
          target: body.target as WorkflowSchedule["target"],
          updatedAt: "2026-04-21T01:00:00Z",
        };
      },
    });
    await mockWorkflowEventTriggers(page, [], {
      onCreate(body) {
        triggerCreateBodies.push(body);
        return {
          id: "trg_created",
          provider: typeof body.provider === "string" ? body.provider : "basic",
          match: body.match as WorkflowEventTrigger["match"],
          target: body.target as WorkflowEventTrigger["target"],
          paused: Boolean(body.paused),
          createdAt: "2026-04-21T00:00:00Z",
          updatedAt: "2026-04-21T00:00:00Z",
        };
      },
      onUpdate(current, body) {
        triggerUpdateBodies.push(body);
        return {
          ...current,
          match: body.match as WorkflowEventTrigger["match"],
          target: body.target as WorkflowEventTrigger["target"],
          updatedAt: "2026-04-21T01:00:00Z",
        };
      },
    });

    await page.goto("/workflows");
    await page.getByRole("tab", { name: "Schedules" }).click();
    await page.getByRole("button", { name: "New schedule" }).click();
    await page.getByLabel("Plugin").selectOption("slack");
    await expect(page.getByLabel("Operation")).toContainText("Post Message");
    await page.getByLabel("Operation").selectOption("chat.postMessage");
    await page.getByLabel("Input JSON").fill('{"channel":"C123"}');
    await page.getByRole("button", { name: "Create schedule" }).click();

    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toBeVisible();
    expect(createBodies[0]).toMatchObject({
      target: {
        plugin: {
          name: "slack",
          operation: "chat.postMessage",
          input: { channel: "C123" },
        },
      },
    });
    expect((createBodies[0].target as Record<string, unknown>).operation).toBeUndefined();

    await page.getByRole("button", { name: "Edit" }).click();
    await page.getByLabel("Input JSON").fill('{"channel":"C456"}');
    await page.getByRole("button", { name: "Save schedule" }).click();
    expect(updateBodies[0]).toMatchObject({
      target: {
        plugin: {
          name: "slack",
          operation: "chat.postMessage",
          input: { channel: "C456" },
        },
      },
    });
    expect((updateBodies[0].target as Record<string, unknown>).operation).toBeUndefined();

    await page.getByRole("tab", { name: "Triggers" }).click();
    await page.getByRole("button", { name: "New trigger" }).click();
    await page.getByLabel("Event type").fill("slack.message.created");
    await page.getByLabel("Plugin").selectOption("slack");
    await expect(page.getByLabel("Operation")).toContainText("Post Message");
    await page.getByLabel("Operation").selectOption("chat.postMessage");
    await page.getByLabel("Input JSON").fill('{"channel":"C789"}');
    await page.getByRole("button", { name: "Create trigger" }).click();

    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toBeVisible();
    expect(triggerCreateBodies[0]).toMatchObject({
      target: {
        plugin: {
          name: "slack",
          operation: "chat.postMessage",
          input: { channel: "C789" },
        },
      },
    });
    expect((triggerCreateBodies[0].target as Record<string, unknown>).operation).toBeUndefined();

    await page.getByRole("button", { name: "Edit" }).click();
    await page.getByLabel("Input JSON").fill('{"channel":"C999"}');
    await page.getByRole("button", { name: "Save trigger" }).click();
    expect(triggerUpdateBodies[0]).toMatchObject({
      target: {
        plugin: {
          name: "slack",
          operation: "chat.postMessage",
          input: { channel: "C999" },
        },
      },
    });
    expect((triggerUpdateBodies[0].target as Record<string, unknown>).operation).toBeUndefined();
  });

  test("shows event trigger details in the triggers tab", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, []);
    await mockWorkflowEventTriggers(page, [
      {
        id: "evt_123",
        provider: "basic",
        match: {
          type: "github.pull_request.opened",
          source: "github",
          subject: "repo:valon/gestalt",
        },
        target: {
          plugin: "slack",
          operation: "chat.postMessage",
          connection: "workspace",
        } as unknown as WorkflowEventTrigger["target"],
        createdAt: "2026-04-20T00:00:00Z",
        updatedAt: "2026-04-20T01:00:00Z",
      },
      {
        id: "evt_456",
        provider: "advanced",
        paused: true,
        match: {
          type: "linear.issue.created",
        },
        target: {
          plugin: {
            name: "github",
            operation: "issues.create",
          },
        },
        createdAt: "2026-04-19T00:00:00Z",
      },
    ]);

    await page.goto("/workflows");
    await page.getByRole("tab", { name: "Triggers" }).click();

    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Trigger Details" }),
    });

    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toBeVisible();
    await expect(detailPanel.getByText("evt_123")).toBeVisible();
    await expect(
      detailPanel.getByText("github.pull_request.opened").first(),
    ).toBeVisible();
    await expect(
      detailPanel.getByText("Source: github · Subject: repo:valon/gestalt"),
    ).toBeVisible();

    await page.getByRole("button", { name: /github\.issues\.create/i }).click();
    await expect(detailPanel.getByText("evt_456")).toBeVisible();
    await expect(page.getByRole("button", { name: "Resume" })).toBeVisible();
  });
});
