import {
  test,
  expect,
  mockAppAdminRegistry,
  mockAuthInfo,
  mockIntegrations,
  mockWorkflowRuns,
} from "./fixtures";
import type { AppAdminRegistryResponse, WorkflowAppTarget, WorkflowTarget } from "../src/lib/api";

const SLACK_APP = "slack";

const SLACK_REGISTRY: AppAdminRegistryResponse = {
  app: SLACK_APP,
  registry: "example-registry",
  knownVersions: [],
  publishedVersions: [],
  selectionDisabled: false,
};

async function openSlackWorkflows(page: import("@playwright/test").Page) {
  await mockIntegrations(page, [
    {
      name: SLACK_APP,
      displayName: "Slack",
      managementPath: `/apps/${SLACK_APP}/admin`,
    },
  ]);
  await mockAppAdminRegistry(page, SLACK_APP, SLACK_REGISTRY);
  await page.goto(`/apps/${SLACK_APP}/admin/workflows`);
}

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
          name: SLACK_APP,
          operation: "chat.postMessage",
          input: { text: "${{ steps.summarize.outputs.text }}" },
        },
      },
    ],
  };
}

test.describe("App admin workflows", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
  });

  test("shows an empty state when no runs exist", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, []);
    await openSlackWorkflows(page);
    await expect(page.getByTestId("app-workflows-empty")).toBeVisible();
  });

  test("refreshes the list without clearing the current page state", async ({
    authenticatedPage: page,
  }) => {
    const workflowRuns = await mockWorkflowRuns(page, [
      {
        id: "run_initial",
        provider: "basic",
        status: "succeeded",
        target: workflowAppTarget(SLACK_APP, "chat.postMessage"),
        trigger: {
          kind: "manual",
        },
        createdAt: "2026-04-20T00:00:00Z",
        completedAt: "2026-04-20T00:02:00Z",
      },
    ]);

    await openSlackWorkflows(page);
    await expect(
      page.getByRole("button", { name: /slack\.chat\.postMessage/i }),
    ).toBeVisible();

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
      {
        id: "run_slack_2",
        provider: "basic",
        status: "succeeded",
        target: workflowAppTarget(SLACK_APP, "chat.postMessage"),
        trigger: { kind: "manual" },
        createdAt: "2026-04-22T00:00:00Z",
      },
    ]);

    await page.getByRole("button", { name: "Refresh" }).click();
    await expect(
      page.getByRole("button", { name: /slack\.chat\.postMessage/i }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: /github\.issues\.create/i })).toHaveCount(0);
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

    await openSlackWorkflows(page);
    const detailPanel = page.locator('[data-testid="app-workflow-run-list"]').locator("..");

    await expect(page.getByTestId("app-admin-nav-workflows")).toHaveClass(/font-medium/);
    await expect(
      page.getByRole("button", { name: /datadog\.monitors\.get \(\+2\)/i }),
    ).toBeVisible();
    await expect(
      page.getByLabel("Recent runs").getByText("incident_triage"),
    ).toBeVisible();
    await expect(
      page.getByLabel("Automation identities").getByText("service_account:workflow-runner"),
    ).toBeVisible();
    await expect(
      page.getByLabel("Recent runs").getByText("event:datadog_alert"),
    ).toBeVisible();
    await expect(page.getByText(/^diagnose$/).first()).toBeVisible();
    await expect(page.getByText(/^summarize$/).first()).toBeVisible();
    await expect(page.getByText(/^notify$/).first()).toBeVisible();
    await expect(page.getByText(/API latency is elevated/)).toBeVisible();
    await expect(page.getByRole("button", { name: /github\.issues\.create/i })).toHaveCount(0);
    await expect(detailPanel.getByText("run_456")).toHaveCount(0);
  });

  test("cancels a pending run from the detail panel", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_pending",
        provider: "basic",
        status: "pending",
        target: workflowAppTarget(SLACK_APP, "chat.postMessage"),
        trigger: {
          kind: "manual",
        },
        createdAt: "2026-04-20T00:00:00Z",
        startedAt: "2026-04-20T00:01:00Z",
      },
    ]);

    await openSlackWorkflows(page);

    await page.getByRole("button", { name: "Cancel run" }).click();
    await expect(page.getByRole("button", { name: "Cancel run" })).toHaveCount(0);
    await expect(page.getByText("Canceled from Gestalt UI")).toBeVisible();
  });

  test("shows cancel errors without clearing the selected run", async ({
    authenticatedPage: page,
  }) => {
    await mockWorkflowRuns(
      page,
      [
        {
          id: "run_pending",
          provider: "basic",
          status: "pending",
          target: workflowAppTarget(SLACK_APP, "chat.postMessage"),
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

    await openSlackWorkflows(page);

    await page.getByRole("button", { name: "Cancel run" }).click();
    await expect(
      page.getByText("workflow run cannot be canceled once it has started"),
    ).toBeVisible();
    await expect(page.getByText("run_pending")).toBeVisible();
    await expect(page.getByRole("button", { name: "Cancel run" })).toBeVisible();
  });

  test("does not offer cancel for a running run", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_running",
        provider: "basic",
        status: "running",
        target: workflowAppTarget(SLACK_APP, "chat.postMessage"),
        trigger: {
          kind: "manual",
        },
        createdAt: "2026-04-20T00:00:00Z",
        startedAt: "2026-04-20T00:01:00Z",
      },
    ]);

    await openSlackWorkflows(page);
    await expect(page.getByRole("button", { name: "Cancel run" })).toHaveCount(0);
  });

  test("scopes workflow runs to the current app", async ({ authenticatedPage: page }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_slack",
        provider: "basic",
        status: "succeeded",
        definitionId: "app_slack_notify",
        target: workflowAppTarget(SLACK_APP, "chat.postMessage"),
        trigger: {
          kind: "schedule",
          activationId: "morning",
        },
        createdBy: { subjectId: "service_account:slack-bot" },
        createdAt: "2026-04-20T00:00:00Z",
        completedAt: "2026-04-20T00:02:00Z",
      },
      {
        id: "run_gmail",
        provider: "basic",
        status: "failed",
        definitionId: "app_gmail_sync",
        target: workflowAppTarget("gmail", "users.messages.list"),
        createdAt: "2026-04-21T00:00:00Z",
      },
    ]);

    await openSlackWorkflows(page);
    await expect(page.getByTestId("app-workflow-ownership-note")).toBeVisible();
    await expect(
      page.getByRole("heading", { level: 3, name: "Definitions & schedules" }),
    ).toBeVisible();
    await expect(
      page.getByLabel("Definitions and schedules").getByText("app_slack_notify"),
    ).toBeVisible();
    await expect(
      page.getByLabel("Automation identities").getByText("service_account:slack-bot"),
    ).toBeVisible();
    await expect(page.getByTestId("app-workflow-run-list")).toBeVisible();
    await expect(
      page.getByRole("button", { name: /slack\.chat\.postMessage/i }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: /gmail\.users\.messages\.list/i }),
    ).toHaveCount(0);
  });
});
