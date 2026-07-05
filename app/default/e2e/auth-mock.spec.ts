import {
  test,
  expect,
  mockAuthInfo,
  mockAuthSession,
  mockAuthSessionUnauthorized,
  mockIntegrations,
  mockManagedIdentities,
  mockTokens,
  mockWorkflowRuns,
} from "./fixtures";

const hasBackend =
  !!process.env.PLAYWRIGHT_BASE_URL || !!process.env.GESTALT_BASE_URL;
const AUTH_TEST_USER_SEEDED_KEY = "gestalt.test.userSeeded";

async function seedAuthenticatedUserOnce(
  page: import("@playwright/test").Page,
) {
  await page.addInitScript(
    ({ seededKey }) => {
      if (sessionStorage.getItem(seededKey) === "1") {
        return;
      }
      localStorage.clear();
      localStorage.setItem(
        "gestalt.auth.session",
        JSON.stringify({
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
        }),
      );
      sessionStorage.setItem(seededKey, "1");
    },
    { seededKey: AUTH_TEST_USER_SEEDED_KEY },
  );
  await mockAuthSession(page);
}

test.describe("Authentication", () => {
  test.skip(
    hasBackend,
    "Auth flow tests use mocked routes and do not apply when running against a real server",
  );
  test("unauthenticated user is redirected to /login with the current route", async ({
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
        url.pathname === "/login" &&
        url.searchParams.get("next") === "/identities?id=agent-1#profile"
      );
    });
  });

  test("login page renders with provider button", async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockAuthSessionUnauthorized(page);
    await page.goto("/login");
    await expect(page.getByRole("heading", { name: "Gestalt" })).toBeVisible();
    await expect(
      page.getByRole("button", { name: /Sign in with Test SSO/i }),
    ).toBeVisible();
  });

  test("no-auth server redirects to dashboard without showing logout", async ({
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
    await mockWorkflowRuns(page, []);

    await page.goto("/login");
    await expect(page).toHaveURL("/");
    await expect(page.getByText("anonymous@gestalt")).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Authorization", exact: true }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: /Logout/i })).toHaveCount(0);

    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await page.goto(
      `/login?next=${encodeURIComponent("/identities?id=agent-1#profile")}`,
    );
    await expect(page).toHaveURL((url) => {
      return (
        url.pathname === "/identities" &&
        url.searchParams.get("id") === "agent-1" &&
        url.hash === "#profile"
      );
    });

    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await mockAuthSession(page, {
      subjectId: "user:anonymous@gestalt",
      email: "anonymous@gestalt",
    });

    await page.goto("/");
    await expect(page).toHaveURL("/");
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

  test("authenticated user sees dashboard", async ({ authenticatedPage }) => {
    const page = authenticatedPage;
    await page.route("**/api/v1/auth/info", (route) => {
      route.abort();
    });
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, [
      { name: "test-svc", displayName: "Test Service" },
    ]);
    await mockTokens(page, []);
    await mockWorkflowRuns(page, []);

    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
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

  test("authenticated user on /login is redirected to the requested app route", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowRuns(page, []);

    await page.goto(
      `/login?next=${encodeURIComponent("/authorization?view=tokens#api")}`,
    );
    await expect(page).toHaveURL((url) => {
      return (
        url.pathname === "/authorization" &&
        url.searchParams.get("view") === "tokens" &&
        url.hash === "#api"
      );
    });
  });

  test("login sanitizes invalid next paths to dashboard", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowRuns(page, []);

    for (const next of [
      "https://evil.example.test/app",
      "//evil.example.test/app",
      "/\\evil.example.test/app",
      "/login?next=/identities",
      "/api/v1/auth/login/callback?code=test-code",
    ]) {
      await page.goto(`/login?next=${encodeURIComponent(next)}`);
      await expect(page).toHaveURL("/");
    }
  });

  test("login start stores sanitized return path", async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockAuthSessionUnauthorized(page);

    let loginBody: { state?: string; next?: string } | null = null;
    await page.route("**/api/v1/auth/login", async (route, request) => {
      loginBody = request.postDataJSON() as { state?: string; next?: string };
      await route.fulfill({ json: { url: "#idp" } });
    });

    const returnPath = "/identities?id=agent-1#profile";
    await page.goto(`/login?next=${encodeURIComponent(returnPath)}`);
    await page.getByRole("button", { name: /Sign in with Test SSO/i }).click();

    await expect.poll(() => loginBody?.state).toBeTruthy();
    await expect.poll(() => loginBody?.next).toBe(returnPath);
  });

  test("logout clears session and redirects to login", async ({ page }) => {
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
    await mockWorkflowRuns(page, []);
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

    await page.goto("/");
    await page.getByRole("button", { name: /Logout/i }).click();
    await expect(page).toHaveURL(/\/login/);
    await expect(
      await page.evaluate(() => localStorage.getItem("gestalt.auth.session")),
    ).toBeNull();
    await expect(
      await page.evaluate(() => localStorage.getItem("user_email")),
    ).toBeNull();
  });

  test("401 response clears session and redirects to login", async ({
    page,
  }) => {
    await seedAuthenticatedUserOnce(page);
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
    await page.route("**/api/v1/workflow/runs", (route) => {
      route.fulfill({ status: 401, json: { error: "invalid token" } });
    });

    await page.goto("/workflows?range=week#runs");
    await expect(page.getByRole("button", { name: /Sign in/i })).toBeVisible();
    const redirectURL = new URL(page.url());
    expect(redirectURL.pathname).toBe("/login");
    expect(redirectURL.searchParams.get("next")).toBe(
      "/workflows?range=week#runs",
    );
    await expect(
      await page.evaluate(() => localStorage.getItem("gestalt.auth.session")),
    ).toBeNull();
  });

});
