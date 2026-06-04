import {
  test,
  expect,
  mockAuthInfo,
  mockIntegrations,
  mockManagedIdentities,
  mockTokens,
  mockWorkflowRuns,
} from "./fixtures";

const hasBackend =
  !!process.env.PLAYWRIGHT_BASE_URL || !!process.env.GESTALT_BASE_URL;
const AUTH_RETURN_PATH_STORAGE_KEY = "gestalt.auth.returnPath";
const AUTH_TEST_OAUTH_SEEDED_KEY = "gestalt.test.oauthSeeded";
const AUTH_TEST_USER_SEEDED_KEY = "gestalt.test.userSeeded";

function encodeWrappedState(hostState: string): string {
  return Buffer.from(
    JSON.stringify({ host_state: hostState }),
    "utf8",
  ).toString("base64url");
}

async function seedOAuthState(
  page: import("@playwright/test").Page,
  oauthState?: string,
  returnPath?: string,
) {
  await page.addInitScript(
    ({ state, path, key, seededKey }) => {
      if (sessionStorage.getItem(seededKey) === "1") {
        return;
      }
      localStorage.clear();
      sessionStorage.clear();
      if (state) {
        sessionStorage.setItem("oauth_state", state);
      }
      if (path) {
        sessionStorage.setItem(key, path);
      }
      sessionStorage.setItem(seededKey, "1");
    },
    {
      state: oauthState,
      path: returnPath,
      key: AUTH_RETURN_PATH_STORAGE_KEY,
      seededKey: AUTH_TEST_OAUTH_SEEDED_KEY,
    },
  );
}

async function seedAuthenticatedUserOnce(
  page: import("@playwright/test").Page,
) {
  await page.addInitScript(
    ({ seededKey }) => {
      if (sessionStorage.getItem(seededKey) === "1") {
        return;
      }
      localStorage.clear();
      localStorage.setItem("user_email", "test@gestalt.dev");
      sessionStorage.setItem(seededKey, "1");
    },
    { seededKey: AUTH_TEST_USER_SEEDED_KEY },
  );
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
      "/auth/callback?code=test-code",
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

    let loginBody: { state?: string } | null = null;
    await page.route("**/api/v1/auth/login", async (route, request) => {
      loginBody = request.postDataJSON() as { state?: string };
      await route.fulfill({ json: { url: "#idp" } });
    });

    const returnPath = "/identities?id=agent-1#profile";
    await page.goto(`/login?next=${encodeURIComponent(returnPath)}`);
    await page.getByRole("button", { name: /Sign in with Test SSO/i }).click();

    await expect.poll(() => loginBody?.state).toBeTruthy();
    await expect
      .poll(() =>
        page.evaluate(
          (key) => sessionStorage.getItem(key),
          AUTH_RETURN_PATH_STORAGE_KEY,
        ),
      )
      .toBe(returnPath);
  });

  test("logout clears session and redirects to login", async ({ page }) => {
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, []);
    await mockTokens(page, []);
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
    await expect(page).toHaveURL((url) => {
      return url.pathname === "/login" && url.searchParams.get("next") === "/";
    });
    await expect(
      page.getByRole("button", { name: /Sign in with Test SSO/i }),
    ).toBeVisible();
    expect(callbackCalled).toBe(false);
  });

  test("auth callback delegates missing OAuth state to the server callback", async ({
    page,
  }) => {
    await seedOAuthState(page);

    let callbackURL: string | null = null;
    await page.route("**/api/v1/auth/login/callback?**", (route, request) => {
      callbackURL = request.url();
      route.fulfill({
        status: 200,
        contentType: "text/plain",
        body: "delegated",
      });
    });

    await page.goto("/auth/callback?code=attacker-code&state=attacker-state");
    await expect.poll(() => callbackURL).not.toBeNull();
    expect(new URL(callbackURL!).pathname).toBe("/api/v1/auth/login/callback");
    expect(new URL(callbackURL!).searchParams.get("code")).toBe(
      "attacker-code",
    );
    expect(new URL(callbackURL!).searchParams.get("state")).toBe(
      "attacker-state",
    );
    await expect(page).toHaveURL((url) => {
      return (
        url.pathname === "/api/v1/auth/login/callback" &&
        url.searchParams.get("code") === "attacker-code" &&
        url.searchParams.get("state") === "attacker-state"
      );
    });
  });

  test("auth callback accepts wrapped host_state and completes login", async ({
    page,
  }) => {
    const wrappedState = encodeWrappedState("correct-state");
    const returnPath = "/identities?id=agent-1#profile";

    await seedOAuthState(page, "correct-state", returnPath);
    await mockAuthInfo(page, {
      provider: "test-sso",
      displayName: "Test SSO",
    });
    await mockManagedIdentities(page, []);
    await mockIntegrations(page, []);
    await mockTokens(page, []);
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
    await expect(page).toHaveURL((url) => {
      return (
        url.pathname === "/identities" &&
        url.searchParams.get("id") === "agent-1" &&
        url.hash === "#profile"
      );
    });
    await expect(page.getByText("test@gestalt.dev")).toBeVisible();
  });

  test("auth callback sanitizes stored return path before redirecting", async ({
    page,
  }) => {
    await seedOAuthState(page, "correct-state", "https://evil.example.test/app");
    await mockIntegrations(page, []);
    await mockTokens(page, []);
    await mockWorkflowRuns(page, []);

    let callbackCalled = false;
    await page.route("**/api/v1/auth/login/callback?**", (route) => {
      callbackCalled = true;
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          email: "test@gestalt.dev",
          displayName: "Test User",
        }),
      });
    });

    await page.goto("/auth/callback?code=test-code&state=correct-state");

    await expect.poll(() => callbackCalled).toBe(true);
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
    page,
  }) => {
    await seedAuthenticatedUserOnce(page);
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
  });

});
