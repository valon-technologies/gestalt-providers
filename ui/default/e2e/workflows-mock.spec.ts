import { test, expect, mockAuthInfo, mockWorkflowRuns } from "./fixtures";
import type { WorkflowAppTarget, WorkflowRun, WorkflowTarget } from "../src/lib/api";

function workflowAppTarget(
  name: string,
  operation: string,
  options: Omit<WorkflowAppTarget, "name" | "operation"> = {},
): WorkflowTarget {
  return {
    steps: [
      {
        id: "run",
        app: {
          name,
          operation,
          ...options,
        },
      },
    ],
  };
}

function workflowMultiStepTarget(): WorkflowTarget {
  return {
    steps: [
      {
        id: "diagnose",
        app: {
          name: "datadog",
          operation: "monitors.get",
          input: { monitor_id: "${{ input.monitor_id }}" },
        },
      },
      {
        id: "summarize",
        agent: {
          provider: "simple",
          model: "fast",
          prompt: {
            template: "Summarize ${{ steps.diagnose.outputs.body }}",
          },
        },
      },
      {
        id: "notify",
        app: {
          name: "slack",
          operation: "chat.postMessage",
          input: { text: "${{ steps.summarize.outputs.text }}" },
        },
      },
    ],
  };
}

test.describe("Workflows", () => {
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
        target: workflowAppTarget("slack", "chat.postMessage"),
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
        target: workflowAppTarget("github", "issues.create"),
        trigger: {
          kind: "schedule",
          activationId: "nightly",
        },
        createdAt: "2026-04-21T00:00:00Z",
      },
    ]);

    await page.getByRole("button", { name: "Refresh" }).click();
    await expect(page.getByRole("button", { name: /github\.issues\.create/i })).toBeVisible();
    await expect(page.getByRole("button", { name: /slack\.chat\.postMessage/i })).toHaveCount(0);
  });

  test("shows durable run details for the selected run", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_123",
        provider: "temporal",
        status: "succeeded",
        target: workflowMultiStepTarget(),
        trigger: {
          kind: "event",
          activationId: "datadog_alert",
          event: {
            type: "datadog.monitor.alert",
            source: "datadog",
            subject: "monitor:123",
          },
        },
        createdBy: { subjectId: "service_account:workflow-runner" },
        definitionId: "incident_triage",
        definitionGeneration: 4,
        input: { monitor_id: "123", channel: "C123" },
        currentStepId: "notify",
        steps: [
          {
            stepId: "diagnose",
            status: "succeeded",
            input: { monitor_id: "123" },
            output: { body: { name: "API latency" } },
            attempts: [
              {
                id: "attempt_diagnose_1",
                status: "succeeded",
                idempotencyKey: "run_123:diagnose:abc",
                output: { body: { name: "API latency" } },
              },
            ],
          },
          {
            stepId: "summarize",
            status: "succeeded",
            output: { text: "API latency is elevated." },
          },
          {
            stepId: "notify",
            status: "succeeded",
            output: { ok: true },
          },
        ],
        output: { ok: true },
        createdAt: "2026-04-20T00:00:00Z",
        startedAt: "2026-04-20T00:01:00Z",
        completedAt: "2026-04-20T00:02:00Z",
        statusMessage: "completed",
      },
      {
        id: "run_456",
        provider: "indexeddb",
        status: "failed",
        target: workflowAppTarget("github", "issues.create"),
        trigger: {
          kind: "manual",
        },
        createdAt: "2026-04-19T00:00:00Z",
      },
    ]);

    await page.goto("/workflows");
    const detailPanel = page.locator("section").filter({
      has: page.getByRole("heading", { name: "Run Details" }),
    });

    await expect(page.getByRole("heading", { name: "Workflows" })).toBeVisible();
    await expect(page.getByRole("button", { name: /datadog\.monitors\.get/i })).toBeVisible();
    await expect(detailPanel.getByText("incident_triage")).toBeVisible();
    await expect(detailPanel.getByText("service_account:workflow-runner")).toBeVisible();
    await expect(detailPanel.getByText("event:datadog_alert")).toBeVisible();
    await expect(detailPanel.getByText(/^diagnose$/).first()).toBeVisible();
    await expect(detailPanel.getByText(/^summarize$/).first()).toBeVisible();
    await expect(detailPanel.getByText(/^notify$/).first()).toBeVisible();
    await expect(detailPanel.getByText("run_123:diagnose:abc")).toBeVisible();
    await expect(detailPanel.getByText(/API latency is elevated/)).toBeVisible();

    await page.getByRole("button", { name: /github\.issues\.create/i }).click();
    await expect(detailPanel.getByText("run_456")).toBeVisible();
    await expect(detailPanel.getByText("manual")).toBeVisible();
  });

  test("cancels a pending run from the detail panel", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_pending",
        provider: "basic",
        status: "pending",
        target: workflowAppTarget("slack", "chat.postMessage"),
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
    await expect(detailPanel.getByText("Canceled from Gestalt UI")).toBeVisible();
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
          target: workflowAppTarget("slack", "chat.postMessage"),
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
        target: workflowAppTarget("slack", "chat.postMessage"),
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
