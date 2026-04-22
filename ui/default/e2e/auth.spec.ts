import { test, expect } from "@playwright/test";

test.describe("Authentication", () => {
  test("unauthenticated user is redirected through login", async ({ page }) => {
    await page.goto("/integrations");
    await page.waitForURL((url) => url.pathname !== "/integrations", {
      timeout: 10000,
    });
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
    ).toBeVisible();
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
    await page.goto("/authorization");
    await expect(
      page.getByRole("heading", { name: "Authorization" }),
    ).toBeVisible();
  });
});
