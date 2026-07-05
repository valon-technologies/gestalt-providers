import { test, expect } from "@playwright/test";

test.describe("Authentication", () => {
  test("unauthenticated user is redirected through login", async ({ page }) => {
    await page.goto("/apps");
    await expect(page).toHaveURL((url) => url.pathname === "/apps");
    await expect(
      page.getByRole("heading", { name: "Apps" }),
    ).toBeVisible();
  });

  test("authenticated user can access pages", async ({ page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
    ).toBeVisible();
    await page.goto("/authorization");
    await expect(
      page.getByRole("heading", { name: "Authorization" }),
    ).toBeVisible();
  });
});
