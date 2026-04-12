import { test, expect } from "@playwright/test";
import { authenticate } from "./helpers";

test.describe("Navigation", () => {
  test.beforeEach(async ({ page }) => {
    await authenticate(page);
  });

  test("dashboard page renders", async ({ page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Dashboard" }),
    ).toBeVisible();
  });

  test("integrations page renders", async ({ page }) => {
    await page.goto("/integrations");
    await expect(
      page.getByRole("heading", { name: "Integrations" }),
    ).toBeVisible();
  });

  test("tokens page renders", async ({ page }) => {
    await page.goto("/tokens");
    await expect(
      page.getByRole("heading", { name: "API Tokens" }),
    ).toBeVisible();
  });

  test("docs page renders", async ({ page }) => {
    await page.goto("/docs");
    await expect(page.getByRole("heading", { name: "Gestalt User Guide" })).toBeVisible();
  });

  test("nav links work", async ({ page }) => {
    await page.goto("/integrations");
    await page.getByRole("link", { name: "Tokens" }).click();
    await expect(page).toHaveURL(/tokens/);
    await expect(
      page.getByRole("heading", { name: "API Tokens" }),
    ).toBeVisible();
  });
});
