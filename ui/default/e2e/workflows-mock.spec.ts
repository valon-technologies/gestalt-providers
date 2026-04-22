import {
  test,
  expect,
  mockAuthInfo,
  mockWorkflowEventTriggers,
  mockWorkflowRuns,
  mockWorkflowSchedules,
} from "./fixtures";
import type { WorkflowEventTrigger, WorkflowRun, WorkflowSchedule } from "../src/lib/api";

async function mockWorkflowState(
  page: Parameters<typeof mockWorkflowRuns>[0],
  {
    runs = [],
    schedules = [],
    triggers = [],
  }: {
    runs?: WorkflowRun[];
    schedules?: WorkflowSchedule[];
    triggers?: WorkflowEventTrigger[];
  },
) {
  const runController = await mockWorkflowRuns(page, runs);
  const scheduleController = await mockWorkflowSchedules(page, schedules);
  const triggerController = await mockWorkflowEventTriggers(page, triggers);
  return { runController, scheduleController, triggerController };
}

test.describe("Workflows", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
  });

  test("shows an empty runs state when no workflow activity exists", async ({
    authenticatedPage: page,
  }) => {
    await mockWorkflowState(page, {});
    await page.goto("/workflows");
    await expect(page.getByRole("heading", { name: "Workflows" })).toBeVisible();
    await expect(page.getByText("No workflow runs yet.")).toBeVisible();
  });

  test("refreshes the run list without clearing the selected surface", async ({
    authenticatedPage: page,
  }) => {
    const { runController } = await mockWorkflowState(page, {
      runs: [
        {
          id: "run_initial",
          provider: "basic",
          status: "succeeded",
          target: {
            plugin: "slack",
            operation: "chat.postMessage",
          },
          trigger: {
            kind: "manual",
          },
          createdAt: "2026-04-20T00:00:00Z",
          completedAt: "2026-04-20T00:02:00Z",
        },
      ],
    });

    await page.goto("/workflows");
    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toBeVisible();

    runController.setRuns([
      {
        id: "run_refreshed",
        provider: "basic",
        status: "failed",
        target: {
          plugin: "github",
          operation: "issues.create",
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

  test("links a run back to its originating schedule or trigger", async ({
    authenticatedPage: page,
  }) => {
    await mockWorkflowState(page, {
      runs: [
        {
          id: "run_123",
          provider: "basic",
          status: "succeeded",
          target: {
            plugin: "slack",
            operation: "chat.postMessage",
            connection: "workspace",
            input: { channel: "C123", text: "hello" },
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
            plugin: "github",
            operation: "issues.create",
          },
          trigger: {
            kind: "event",
            triggerId: "evt_456",
          },
          createdAt: "2026-04-19T00:00:00Z",
        },
      ],
      schedules: [
        {
          id: "sched_123",
          provider: "basic",
          cron: "0 9 * * 1-5",
          timezone: "America/New_York",
          paused: false,
          target: {
            plugin: "slack",
            operation: "chat.postMessage",
          },
          createdAt: "2026-04-19T00:00:00Z",
          updatedAt: "2026-04-20T00:00:00Z",
          nextRunAt: "2026-04-22T13:00:00Z",
        },
      ],
      triggers: [
        {
          id: "evt_456",
          provider: "basic",
          paused: false,
          match: {
            type: "task.updated",
            source: "roadmap",
          },
          target: {
            plugin: "github",
            operation: "issues.create",
          },
          createdAt: "2026-04-19T00:00:00Z",
          updatedAt: "2026-04-20T00:00:00Z",
        },
      ],
    });

    await page.goto("/workflows");

    const runDetailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });
    await expect(runDetailPanel.getByText("schedule:sched_123")).toBeVisible();
    await page.getByRole("button", { name: "Open schedule" }).click();

    const scheduleDetailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Schedule Details" }),
    });
    await expect(scheduleDetailPanel.getByText("sched_123")).toBeVisible();
    await expect(page.getByLabel("Cron")).toHaveValue("0 9 * * 1-5");

    await page.getByRole("button", { name: /Runs/i }).click();
    await page.getByRole("button", { name: /github\.issues\.create/i }).click();
    await page.getByRole("button", { name: "Open event trigger" }).click();

    const triggerDetailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Event Trigger Details" }),
    });
    await expect(triggerDetailPanel.getByText("evt_456")).toBeVisible();
    await expect(page.getByLabel("Event type")).toHaveValue("task.updated");
  });

  test("creates, pauses, resumes, and deletes a schedule", async ({
    authenticatedPage: page,
  }) => {
    await mockWorkflowState(page, {});

    await page.goto("/workflows");
    await page.getByRole("button", { name: /Schedules/i }).click();
    await page.getByRole("button", { name: "New schedule" }).click();

    await page.getByLabel("Cron").fill("0 9 * * 1-5");
    await page.getByLabel("Target plugin").fill("slack");
    await page.getByLabel("Target operation").fill("chat.postMessage");
    await page.getByLabel("Connection").fill("workspace");
    await page.getByLabel("Input JSON").fill('{"channel":"C123","text":"hello"}');
    await page.getByRole("button", { name: "Create schedule" }).click();

    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toBeVisible();
    await expect(
      page
        .locator("section")
        .filter({ has: page.getByRole("heading", { name: "Schedule Details" }) })
        .getByText("sched_1"),
    ).toBeVisible();

    await page.getByRole("button", { name: "Pause schedule" }).click();
    await expect(page.getByRole("button", { name: "Resume schedule" })).toBeVisible();

    await page.getByRole("button", { name: "Resume schedule" }).click();
    await expect(page.getByRole("button", { name: "Pause schedule" })).toBeVisible();

    await page.getByRole("button", { name: "Delete schedule" }).click();
    await expect(page.getByText("No workflow schedules yet.")).toBeVisible();
  });

  test("creates, updates, pauses, and deletes an event trigger", async ({
    authenticatedPage: page,
  }) => {
    await mockWorkflowState(page, {});

    await page.goto("/workflows");
    await page.getByRole("button", { name: /Event Triggers/i }).click();
    await page.getByRole("button", { name: "New event trigger" }).click();

    await page.getByLabel("Event type").fill("task.updated");
    await page.getByLabel("Source").fill("roadmap");
    await page.getByLabel("Target plugin").fill("github");
    await page.getByLabel("Target operation").fill("issues.create");
    await page.getByLabel("Input JSON").fill('{"title":"Follow up"}');
    await page.getByRole("button", { name: "Create event trigger" }).click();

    await expect(page.getByRole("button", { name: /task\.updated/i })).toBeVisible();
    await expect(page.getByLabel("Event type")).toHaveValue("task.updated");

    await page.getByLabel("Source").fill("roadmap-sync");
    await page.getByRole("button", { name: "Save event trigger" }).click();
    await expect(page.getByLabel("Source")).toHaveValue("roadmap-sync");

    await page.getByRole("button", { name: "Pause event trigger" }).click();
    await expect(page.getByRole("button", { name: "Resume event trigger" })).toBeVisible();

    await page.getByRole("button", { name: "Delete event trigger" }).click();
    await expect(page.getByText("No workflow event triggers yet.")).toBeVisible();
  });

  test("cancels an in-flight run from the detail panel", async ({ authenticatedPage: page }) => {
    await mockWorkflowState(page, {
      runs: [
        {
          id: "run_inflight",
          provider: "basic",
          status: "pending",
          target: {
            plugin: "slack",
            operation: "chat.postMessage",
          },
          trigger: {
            kind: "manual",
          },
          createdAt: "2026-04-20T00:00:00Z",
          startedAt: "2026-04-20T00:01:00Z",
        },
      ],
    });

    await page.goto("/workflows");
    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });

    await page.getByRole("button", { name: "Cancel run" }).click();
    await expect(detailPanel.getByText(/^canceled$/i)).toBeVisible();
    await expect(detailPanel.getByText("Run canceled.")).toBeVisible();
    await expect(page.getByRole("button", { name: "Cancel run" })).toHaveCount(0);
  });
});
