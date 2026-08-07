import {
  test,
  expect,
  mockAppAdminIdentities,
  mockAppAdminRegistry,
  mockAuthInfo,
  mockIntegrations,
} from "./fixtures";
import type { AppAdminIdentity } from "../src/lib/api";

const hasBackend =
  !!process.env.PLAYWRIGHT_BASE_URL || !!process.env.GESTALT_BASE_URL;

const identitiesFixture: AppAdminIdentity[] = [
  {
    subjectId: "service_account:slack-bot",
    displayName: "slack-bot",
    role: "viewer",
    source: "static",
    mutable: false,
    effective: true,
  },
  {
    subjectId: "service_account:ci-runner",
    displayName: "ci-runner",
    role: "editor",
    source: "dynamic",
    mutable: true,
    effective: true,
  },
];

test.describe("App admin agent identities", () => {
  test.skip(
    hasBackend,
    "Agent identity tests use mocked routes and do not apply when running against a real server",
  );

  test.beforeEach(async ({ authenticatedPage: page }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockIntegrations(page, [
      {
        name: "httpbin",
        displayName: "HTTPBin",
        description: "HTTP request and response service",
        connected: true,
      },
    ]);
    await mockAppAdminRegistry(page, "httpbin", {
      app: "httpbin",
      registry: "toolshed",
      desiredVersion: "1.0.0",
      knownVersions: [],
      publishedVersions: [],
      autoDeploy: { enabled: false },
      selectionDisabled: false,
    });
  });

  test("lists service accounts with grants on the app", async ({
    authenticatedPage: page,
  }) => {
    await mockAppAdminIdentities(page, "httpbin", identitiesFixture);

    await page.goto("/apps/httpbin/admin/agent-identities");
    await expect(
      page.getByRole("heading", { name: "Agent identities" }),
    ).toBeVisible();
    await expect(page.getByTestId("app-agent-identities-list")).toBeVisible();
    await expect(page.getByText("slack-bot")).toBeVisible();
    await expect(page.getByText("service_account:slack-bot")).toBeVisible();
    await expect(page.getByText("ci-runner")).toBeVisible();
    await expect(page.getByText("viewer").first()).toBeVisible();
    await expect(page.getByText("editor").first()).toBeVisible();
  });

  test("shows empty state when no agent identities have grants", async ({
    authenticatedPage: page,
  }) => {
    await mockAppAdminIdentities(page, "httpbin", []);

    await page.goto("/apps/httpbin/admin/agent-identities");
    await expect(
      page.getByText("No agent identities have a grant for this app yet."),
    ).toBeVisible();
  });

  test("legacy settings identities paths redirect to apps", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/settings/identities");
    await expect(page).toHaveURL(/\/apps$/);

    await page.goto("/identities");
    await expect(page).toHaveURL(/\/apps$/);

    await page.goto("/settings/identities/agent-1");
    await expect(page).toHaveURL(/\/apps$/);
  });
});
