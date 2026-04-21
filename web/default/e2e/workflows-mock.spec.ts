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
});
