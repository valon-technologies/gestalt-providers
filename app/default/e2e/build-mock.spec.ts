import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrations,
  mockManagedIdentities,
  mockTokens,
} from "./fixtures";

test.describe("Build page", () => {
  test.beforeEach(async ({ authenticatedPage }) => {
    await mockAuthInfo(authenticatedPage, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(authenticatedPage, []);
    await mockIntegrations(authenticatedPage, []);
    await mockTokens(authenticatedPage, []);
  });

  test("renders connect checklist and primary Calendar aha", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/build");

    await expect(
      page.getByRole("heading", { name: "Build", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Connect an app" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Grant access" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Make your first call" }),
    ).toBeVisible();

    await expect(
      page.getByRole("button", { name: /Primary Google Calendar See what.s next/ }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Alternate Slack Post a hello" }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Alternate Notion Find a page" }),
    ).toBeVisible();

    await expect(page.getByText(/Google Calendar: See what.s next/)).toBeVisible();
    await expect(
      page.getByText(/gestalt apps invoke google_calendar events\.list/),
    ).toBeVisible();

    await expect(
      page.getByRole("link", { name: "Build", exact: true }),
    ).toBeVisible();
  });

  test("switches invoke recipe when selecting Slack", async ({
    authenticatedPage: page,
  }) => {
    await page.goto("/build");
    await page
      .getByRole("button", { name: "Alternate Slack Post a hello" })
      .click();
    await expect(page.getByText("Slack: Post a hello")).toBeVisible();
    await expect(
      page.getByText(/gestalt apps invoke slack chat\.postMessage/),
    ).toBeVisible();
  });

  test("marks steps complete when apps and tokens exist", async ({
    authenticatedPage: page,
  }) => {
    await mockIntegrations(page, [
      {
        name: "google_calendar",
        displayName: "Google Calendar",
        description: "Calendar",
        credentialState: "connected",
      },
    ]);
    await mockTokens(page, [
      {
        id: "tok_123",
        name: "Default token",
        scopes: ["api"],
        createdAt: "2026-04-13T00:00:00Z",
      },
    ]);

    await page.goto("/build");
    await expect(page.getByText(/You.re ready to build/)).toBeVisible();
    await expect(page.getByText("Done").first()).toBeVisible();
  });
});
