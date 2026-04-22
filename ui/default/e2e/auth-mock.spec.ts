import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrations,
  mockManagedIdentities,
  mockTokens,
  mockWorkflowEventTriggers,
  mockWorkflowRuns,
  mockWorkflowSchedules,
} from "./fixtures";

const hasBackend =
  !!process.env.PLAYWRIGHT_BASE_URL || !!process.env.GESTALT_BASE_URL;

function encodeWrappedState(hostState: string): string {
  return Buffer.from(
    JSON.stringify({ host_state: hostState }),
    "utf8",
  ).toString("base64url");
}

async function seedOAuthState(
  page: import("@playwright/test").Page,
  oauthState?: string,
) {
  await page.addInitScript((state) => {
    localStorage.clear();
    sessionStorage.clear();
    if (state) {
      sessionStorage.setItem("oauth_state", state);
    }
  }, oauthState);
}

test.describe("Authentication", () => {
  test.skip(
    hasBackend,
    "Auth flow tests use mocked routes and do not apply when running against a real server",
  );
  test("unauthenticated user is redirected to /login", async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await page.goto("/");
    await expect(page).toHaveURL(/\/login/);
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
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowSchedules(page, []);
    await mockWorkflowEventTriggers(page, []);
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
      localStorage.setItem("user_email", "anonymous@gestalt");
      sessionStorage.clear();
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
      page.getByText("Managed identities require platform auth and are unavailable when auth is disabled."),
    ).toBeVisible();

    await page.goto("/identities?id=agent-1");
    await expect(
      page.getByText("Managed identities require platform auth and are unavailable when auth is disabled."),
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
    await mockWorkflowSchedules(page, []);
    await mockWorkflowEventTriggers(page, []);
    await mockWorkflowRuns(page, []);

    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Plugins", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Authorization", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: /Logout/i }),
    ).toBeVisible();
  });

  test("authenticated user on /login is redirected to dashboard", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowSchedules(page, []);
    await mockWorkflowEventTriggers(page, []);
    await mockWorkflowRuns(page, []);

    await page.goto("/login");
    await expect(page).toHaveURL("/");
  });

  test("logout clears session and redirects to login", async ({ page }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowSchedules(page, []);
    await mockWorkflowEventTriggers(page, []);
    await mockWorkflowRuns(page, []);
    await page.route("**/api/v1/auth/logout", (route) => {
      route.fulfill({ json: { status: "ok" } });
    });

    await page.goto("/login");
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
      localStorage.setItem("user_email", "test@gestalt.dev");
    });
    await page.goto("/");
    await page.getByRole("button", { name: /Logout/i }).click();
    await expect(page).toHaveURL(/\/login/);
    await expect(await page.evaluate(() => localStorage.getItem("user_email"))).toBeNull();
  });

  test("auth callback redirects mismatched OAuth state back to login", async ({
    page,
  }) => {
    await seedOAuthState(page, "correct-state");
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });

    let callbackCalled = false;
    await page.route("**/api/v1/auth/login/callback?**", (route) => {
      callbackCalled = true;
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          email: "unexpected@gestalt.dev",
          displayName: "Unexpected",
        }),
      });
    });

    await page.goto("/auth/callback?code=test-code&state=wrong-state");
    await expect(page).toHaveURL(/\/login/);
    await expect(
      page.getByRole("button", { name: /Sign in with Test SSO/i }),
    ).toBeVisible();
    expect(callbackCalled).toBe(false);
  });

  test("auth callback redirects missing OAuth state back to login", async ({
    page,
  }) => {
    await seedOAuthState(page);
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });

    let callbackCalled = false;
    await page.route("**/api/v1/auth/login/callback?**", (route) => {
      callbackCalled = true;
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          email: "unexpected@gestalt.dev",
          displayName: "Unexpected",
        }),
      });
    });

    await page.goto("/auth/callback?code=attacker-code&state=attacker-state");
    await expect(page).toHaveURL(/\/login/);
    await expect(
      page.getByRole("button", { name: /Sign in with Test SSO/i }),
    ).toBeVisible();
    expect(callbackCalled).toBe(false);
  });

  test("auth callback accepts wrapped host_state and completes login", async ({
    page,
  }) => {
    const wrappedState = encodeWrappedState("correct-state");

    await seedOAuthState(page, "correct-state");
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowSchedules(page, []);
    await mockWorkflowEventTriggers(page, []);
    await mockWorkflowRuns(page, []);

    let callbackState: string | null = null;
    await page.route("**/api/v1/auth/login/callback?**", (route, request) => {
      callbackState = new URL(request.url()).searchParams.get("state");
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          email: "test@gestalt.dev",
          displayName: "Test User",
        }),
      });
    });

    await page.goto(`/auth/callback?code=test-code&state=${wrappedState}`);

    await expect.poll(() => callbackState).toBe(wrappedState);
    await expect(page).toHaveURL("/");
    await expect(page.getByText("test@gestalt.dev")).toBeVisible();
  });

  test("auth callback redirects wrapped CLI state to the local listener", async ({
    page,
  }) => {
    const port = 43123;
    const cliState = "cli-original-state";
    const wrappedState = encodeWrappedState(`cli:${port}:${cliState}`);

    await seedOAuthState(page);

    let localCallbackURL: string | null = null;
    let serverCallbackCalled = false;

    await page.route(`http://127.0.0.1:${port}/**`, (route, request) => {
      localCallbackURL = request.url();
      route.fulfill({
        status: 200,
        contentType: "text/html",
        body: "<!doctype html><title>CLI callback</title><p>ok</p>",
      });
    });
    await page.route("**/api/v1/auth/login/callback?**", (route) => {
      serverCallbackCalled = true;
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          email: "unexpected@gestalt.dev",
          displayName: "Unexpected",
        }),
      });
    });

    await page.goto(`/auth/callback?code=test-code&state=${wrappedState}`);

    await expect(page).toHaveURL(new RegExp(`^http://127\\.0\\.0\\.1:${port}/\\?`));
    await expect.poll(() => localCallbackURL).not.toBeNull();
    expect(new URL(localCallbackURL!).searchParams.get("code")).toBe("test-code");
    expect(new URL(localCallbackURL!).searchParams.get("state")).toBe(cliState);
    expect(serverCallbackCalled).toBe(false);
  });

  test("401 response clears session and redirects to login", async ({
    authenticatedPage,
  }) => {
    const page = authenticatedPage;
    await page.route("**/api/v1/integrations", (route) => {
      route.fulfill({ status: 401, json: { error: "invalid token" } });
    });
    await page.route("**/api/v1/tokens", (route) => {
      route.fulfill({ status: 401, json: { error: "invalid token" } });
    });
    await page.route("**/api/v1/workflow/schedules", (route) => {
      route.fulfill({ status: 401, json: { error: "invalid token" } });
    });
    await page.route("**/api/v1/workflow/event-triggers", (route) => {
      route.fulfill({ status: 401, json: { error: "invalid token" } });
    });
    await page.route("**/api/v1/workflow/runs", (route) => {
      route.fulfill({ status: 401, json: { error: "invalid token" } });
    });

    await page.goto("/");
    await expect(page).toHaveURL(/\/login/);
  });

});
