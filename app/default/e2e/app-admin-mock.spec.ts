import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrations,
  mockIntegrationOperations,
  mockManagedIdentities,
  mockWorkflowRuns,
} from "./fixtures";
import type { Page } from "@playwright/test";
import type { WorkflowAppTarget, WorkflowTarget } from "../src/lib/workflow";

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

async function mockAppMembers(
  page: Page,
  members: Array<{
    email?: string;
    role?: string;
    source?: string;
    mutable?: boolean;
    effective?: boolean;
  }>,
) {
  await page.route("**/admin/api/v1/authorization/apps/*/members", (route) => {
    if (route.request().method() === "GET") {
      route.fulfill({ json: members });
    } else {
      route.fallback();
    }
  });
}

test.describe("App admin", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(authenticatedPage, []);
    await mockAppMembers(authenticatedPage, []);
    await mockIntegrations(authenticatedPage, [
      {
        name: "slack",
        displayName: "Slack",
        description: "Team messaging.",
        status: "ready",
        credentialState: "connected",
        connections: [
          {
            name: "default",
            status: "ready",
            credentialState: "connected",
            authTypes: ["oauth"],
          },
        ],
      },
      {
        name: "gmail",
        displayName: "Gmail",
        description: "Email.",
        status: "needs_user_connection",
        credentialState: "missing",
      },
    ]);
  });

  test("catalog card opens app admin", async ({ authenticatedPage: page }) => {
    await page.goto("/apps");
    await page.getByTestId("integration-card-slack").click();
    await expect(page).toHaveURL(/\/apps\/slack/);
    await expect(
      page.getByRole("heading", { level: 1, name: "Slack" }),
    ).toBeVisible();
  });

  test("shows overview connection controls", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/apps/slack");
    await expect(
      page.getByRole("heading", { level: 1, name: "Slack" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Connection" }),
    ).toBeVisible();
    await expect(page.getByTestId("integration-card-slack")).toBeVisible();
  });

  test("shows members on the access tab", async ({
    authenticatedPage: page,
  }) => {
    await mockAppMembers(page, [
      {
        email: "alice@example.com",
        role: "admin",
        source: "static",
        mutable: false,
        effective: true,
      },
      {
        email: "bob@example.com",
        role: "viewer",
        source: "dynamic",
        mutable: true,
        effective: true,
      },
    ]);

    await page.goto("/apps/slack");
    await page.getByRole("radio", { name: "Access" }).click();
    await expect(page.getByTestId("app-members-list")).toBeVisible();
    await expect(page.getByText("alice@example.com")).toBeVisible();
    await expect(page.getByText("bob@example.com")).toBeVisible();
  });

  test("scopes workflow runs and shows research sections", async ({
    authenticatedPage: page,
  }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_slack",
        provider: "basic",
        status: "succeeded",
        definitionId: "app_slack_notify",
        target: workflowAppTarget("slack", "chat.postMessage"),
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

    await page.goto("/apps/slack");
    await page.getByRole("radio", { name: "Workflows" }).click();
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

  test("shows empty workflow state for apps without runs", async ({
    authenticatedPage: page,
  }) => {
    await mockWorkflowRuns(page, [
      {
        id: "run_gmail",
        provider: "basic",
        status: "succeeded",
        definitionId: "app_gmail_sync",
        target: workflowAppTarget("gmail", "users.messages.list"),
        createdAt: "2026-04-21T00:00:00Z",
      },
    ]);

    await page.goto("/apps/slack");
    await page.getByRole("radio", { name: "Workflows" }).click();
    await expect(page.getByTestId("app-workflows-empty")).toBeVisible();
  });

  test("lists operations for the app", async ({ authenticatedPage: page }) => {
    await mockIntegrationOperations(page, {
      slack: [
        {
          id: "chat.postMessage",
          title: "Post message",
          description: "Send a message to a channel.",
          tags: ["chat"],
        },
      ],
    });

    await page.goto("/apps/slack");
    await page.getByRole("radio", { name: "Operations" }).click();
    await expect(page.getByTestId("app-operations-list")).toBeVisible();
    await expect(page.getByText("chat.postMessage")).toBeVisible();
    await expect(page.getByText("Post message")).toBeVisible();
  });
});
