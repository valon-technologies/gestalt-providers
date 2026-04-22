import { test, expect } from "@playwright/test";
import { authenticate } from "./helpers";

test.describe("API Tokens", () => {
  test.beforeEach(async ({ page }) => {
    await authenticate(page);
  });

  test("shows empty state when no tokens exist", async ({ page }) => {
    await page.goto("/authorization");
    await expect(
      page.getByRole("heading", { name: "Authorization" }),
    ).toBeVisible();
    await expect(page.getByText("No API tokens yet")).toBeVisible();
  });

  test("create and revoke a token", async ({ page }) => {
    await page.goto("/authorization");

    await page.getByPlaceholder("e.g. ci-pipeline").fill("test-ci-token");
    await page.getByRole("button", { name: "Create Token" }).click();

    await expect(page.getByText("test-ci-token")).toBeVisible();

    await page.getByRole("button", { name: "Revoke" }).click();

    await expect(page.getByText("No API tokens yet")).toBeVisible();
  });
});
