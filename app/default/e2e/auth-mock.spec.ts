import {
  test,
  expect,
  mockAuthInfo,
  mockAuthSession,
  mockAuthSessionUnauthorized,
  mockIntegrations,
  mockManagedIdentities,
  mockTokens,
} from "./fixtures";

const hasBackend =
  !!process.env.PLAYWRIGHT_BASE_URL || !!process.env.GESTALT_BASE_URL;

test.describe("Authentication", () => {
  test.skip(
    hasBackend,
    "Auth flow tests use mocked routes and do not apply when running against a real server",
  );

  test("unauthenticated session check redirects to server login with return path", async ({
    page,
  }) => {
    await page.addInitScript(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockAuthSessionUnauthorized(page);

    await page.goto("/identities?id=agent-1#profile");
    await expect(page).toHaveURL((url) => {
      return (
        url.pathname === "/api/v1/auth/login" &&
        url.searchParams.get("next") === "/identities?id=agent-1#profile"
      );
    });
  });

  test("no-auth server redirects to apps without showing logout", async ({
    page,
  }) => {
    await mockAuthInfo(page, {
      provider: "none",
      displayName: "none",
      loginSupported: false,
    });
    await mockAuthSession(page, {
      subjectId: "user:anonymous@gestalt",
      email: "anonymous@gestalt",
    });
    await mockIntegrations(page, []);
    await mockTokens(page, []);

    await page.goto("/");
    await expect(page).toHaveURL(/\/apps/);
    await expect(page.getByText("anonymous@gestalt")).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Authorization", exact: true }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: /Logout/i })).toHaveCount(0);

    await page.goto("/identities");
    await expect(
      page.getByRole("heading", { name: "Agent Identities" }),
    ).toBeVisible();

    await page.goto("/identities?id=agent-1");
    await expect(
      page.getByRole("heading", { name: "Agent Identities" }),
    ).toBeVisible();
  });

  test("authenticated user sees apps home", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await page.route("**/api/v1/auth/info", (route) => {
      route.abort();
    });
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, [
      { name: "test-svc", displayName: "Test Service" },
    ]);
    await mockTokens(page, []);

    await page.goto("/");
    await expect(page).toHaveURL(/\/apps/);
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Apps", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Authorization", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: /Logout/i }),
    ).toBeVisible();
  });

  test("logout clears session and redirects to server login", async ({ page }) => {
    let loggedOut = false;
    await page.addInitScript(() => {
      if (sessionStorage.getItem("logout-test-seeded") === "1") {
        return;
      }
      sessionStorage.setItem("logout-test-seeded", "1");
      localStorage.setItem(
        "gestalt.auth.session",
        JSON.stringify({
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
        }),
      );
    });
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await page.route("**/api/v1/auth/logout", (route) => {
      loggedOut = true;
      route.fulfill({ json: { status: "ok" } });
    });
    await page.route("**/api/v1/auth/session", (route) => {
      if (loggedOut) {
        route.fulfill({ status: 401, json: { error: "missing authorization" } });
        return;
      }
      route.fulfill({
        json: {
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
        },
      });
    });

    await page.goto("/apps");
    await page.getByRole("button", { name: /Logout/i }).click();
    await expect(page).toHaveURL((url) => {
      return (
        url.pathname === "/api/v1/auth/login" &&
        url.searchParams.get("next") === "/apps"
      );
    });
    await expect(
      await page.evaluate(() => localStorage.getItem("gestalt.auth.session")),
    ).toBeNull();
    await expect(
      await page.evaluate(() => localStorage.getItem("user_email")),
    ).toBeNull();
  });

  test("401 response clears session and redirects to server login", async ({
    page,
  }) => {
    await page.goto("/apps");
    await page.evaluate(() => {
      localStorage.setItem(
        "gestalt.auth.session",
        JSON.stringify({
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
        }),
      );
    });
    await page.route("**/api/v1/auth/session", (route, request) => {
      if (request.method() === "GET") {
        route.fulfill({ status: 401, json: { error: "invalid token" } });
        return;
      }
      route.fallback();
    });
    await page.route("**/api/v1/apps", (route) => {
      route.fulfill({ status: 401, json: { error: "invalid token" } });
    });
    await page.route("**/api/v1/tokens", (route) => {
      route.fulfill({ status: 401, json: { error: "invalid token" } });
    });

    await page.goto("/apps?view=catalog#list");
    await expect(page).toHaveURL((url) => {
      return (
        url.pathname === "/api/v1/auth/login" &&
        url.searchParams.get("next") === "/apps?view=catalog#list"
      );
    });
    await expect(
      await page.evaluate(() => localStorage.getItem("gestalt.auth.session")),
    ).toBeNull();
  });
});
