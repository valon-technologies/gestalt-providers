import { test, expect, mockAuthInfo, mockWorkflowRuns } from "./fixtures";

test.describe("Workflow runs", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
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
        },
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

  test("shows run details for the selected run", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
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
    ]);

    await page.goto("/workflows");
    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });
    await expect(page.getByRole("heading", { name: "Workflow Runs" })).toBeVisible();
    await expect(
      page.getByRole("button", { name: /slack\.chat\.postMessage/i }),
    ).toBeVisible();
    await expect(detailPanel.getByText(/^completed$/)).toBeVisible();
    await expect(detailPanel.getByText("schedule:sched_123")).toBeVisible();
    await expect(detailPanel.getByText("slack.chat.postMessage")).toBeVisible();

    await page.getByRole("button", { name: /github\.issues\.create/i }).click();
    await expect(detailPanel.getByText("run_456")).toBeVisible();
    await expect(detailPanel.getByText("event:evt_456")).toBeVisible();
    await expect(detailPanel.getByText("github.issues.create")).toBeVisible();
  });

  test("cancels an in-flight run from the detail panel", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
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
            plugin: "slack",
            operation: "chat.postMessage",
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
          plugin: "slack",
          operation: "chat.postMessage",
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
});
