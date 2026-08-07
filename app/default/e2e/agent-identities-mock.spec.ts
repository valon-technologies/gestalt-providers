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
    displayName: "CI Runner",
    role: "editor",
    source: "dynamic",
    mutable: true,
    effective: true,
  },
  {
    subjectId: "service_account:old-bot",
    displayName: "old-bot",
    role: "viewer",
    source: "dynamic",
    mutable: true,
    effective: false,
    shadowedBy: "static viewer grant",
  },
];

test.describe("App admin service accounts", () => {
  test.skip(
    hasBackend,
    "Service account tests use mocked routes and do not apply when running against a real server",
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

  test("lists service accounts with name and role only", async ({
    authenticatedPage: page,
  }) => {
    await mockAppAdminIdentities(page, "httpbin", identitiesFixture);

    await page.goto("/apps/httpbin/admin/agent-identities");
    await expect(
      page.getByRole("heading", { name: "Service accounts" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "How to create a service account" }),
    ).toHaveAttribute("href", "/docs/authorization#authz-service-accounts");
    await expect(page.getByTestId("app-agent-identities-list")).toBeVisible();

    await expect(page.getByText("slack-bot", { exact: true })).toBeVisible();
    await expect(page.getByText("service_account:slack-bot")).toHaveCount(0);
    await expect(page.getByText("static · locked")).toHaveCount(0);
    await expect(page.getByText("Effective", { exact: true })).toHaveCount(0);

    await expect(page.getByText("CI Runner", { exact: true })).toBeVisible();
    await expect(page.getByText("Account ID · ci-runner")).toBeVisible();
    await expect(page.getByText("Viewer").first()).toBeVisible();
    await expect(page.getByText("Editor").first()).toBeVisible();

    await expect(page.getByText("Overridden")).toBeVisible();
    await expect(
      page.getByText("Not used — static viewer grant takes priority"),
    ).toBeVisible();
  });

  test("shows empty state when no service accounts have access", async ({
    authenticatedPage: page,
  }) => {
    await mockAppAdminIdentities(page, "httpbin", []);

    await page.goto("/apps/httpbin/admin/agent-identities");
    await expect(
      page.getByText("No service accounts have access to this app yet."),
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
