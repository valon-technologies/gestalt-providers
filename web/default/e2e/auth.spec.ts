import { test, expect } from "@playwright/test";

test.describe("Authentication", () => {
  test("unauthenticated user is redirected to login", async ({ page }) => {
    await page.goto("/integrations");
    await expect(page).toHaveURL(/login/);
  });

  test("login page auto-authenticates when auth provider is disabled", async ({
    page,
  }) => {
    await page.goto("/login");
    await page.waitForURL((url) => url.pathname !== "/login", {
      timeout: 10000,
    });
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
    ).toBeVisible();
  });

  test("authenticated user can access pages", async ({ page }) => {
    await page.goto("/login");
    await page.waitForURL((url) => url.pathname !== "/login", {
      timeout: 10000,
    });
    await page.goto("/tokens");
    await expect(
      page.getByRole("heading", { name: "API Tokens" }),
    ).toBeVisible();
  });
});
