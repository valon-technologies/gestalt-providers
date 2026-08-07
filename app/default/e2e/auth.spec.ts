import { test, expect } from "@playwright/test";

const hasBackend =
  !!process.env.PLAYWRIGHT_BASE_URL || !!process.env.GESTALT_BASE_URL;

test.describe("Authentication", () => {
  test.skip(
    !hasBackend,
    "Live gestaltd auth flows require PLAYWRIGHT_BASE_URL or GESTALT_BASE_URL",
  );

  test("unauthenticated user is redirected through login", async ({ page }) => {
    await page.goto("/apps");
    await expect(page).toHaveURL((url) => url.pathname === "/apps");
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
  });

  test("authenticated user can access pages", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveURL(/\/apps/);
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
    await page.goto("/authorization");
    await expect(page).toHaveURL(/\/settings\/tokens\/new/);
    await expect(
      page.getByRole("heading", { name: "Create token" }),
    ).toBeVisible();
  });
});
